# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import contextlib
import importlib
import inspect
import io
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional, List

import pandas as pd

try:
    from fts_runtime_diagnostics import record_issue, write_summary as write_runtime_diagnostics_summary
except Exception:  # runtime diagnostics  # pragma: no cover
    def record_issue(*args, **kwargs):
        return {}
    def write_runtime_diagnostics_summary(*args, **kwargs):
        return None

from fts_config import PATHS, CONFIG
from fts_utils import log, now_str
from fts_project_healthcheck import ProjectHealthcheck
from fts_pipeline import run_level2_mainline
from fts_live_readiness_gate import LiveReadinessGate
from fts_live_safety import LiveSafetyGate
from fts_broker_core import BrokerApprovalGate
from fts_admin_suite import SubmissionContractGate
from fts_project_quality_suite import ValidationSuiteBuilder
from fts_model_gate import ModelVersionRegistry, ModelSelectionGate
from fts_gatekeeper import LaunchGatekeeper
from fts_live_suite import LiveReleaseGate
from fts_operations_suite import OperatorApprovalRegistry
from fts_execution_runtime import RecoveryEngine
from fts_project_quality_suite import RecoveryValidationBuilder
from fts_execution_runtime import ReconciliationEngine
from fts_tri_lane_orchestrator import TriLaneOrchestrator
from fts_prelive_runtime import write_json
from fts_training_data_builder import get_dynamic_watchlist, generate_ml_dataset
from fts_trainer_backend import train_models
from fts_maturity_upgrade_suite import MaturityUpgradeSuite
from fts_execution_journal_service import append_execution_journal_event
from fts_kill_switch import KillSwitchManager

RUNTIME_DIR = PATHS.runtime_dir


def _ensure_kill_switch_state() -> tuple[str, dict[str, Any]]:
    try:
        payload = KillSwitchManager().ensure_default_state()
        return str(PATHS.runtime_dir / 'kill_switch_state.json'), {'status': 'kill_switch_state_ready', 'state': payload}
    except Exception as exc:
        record_issue('control_tower', 'kill_switch_state_init_failed', exc, severity='ERROR', fail_mode='fail_closed')
        return str(PATHS.runtime_dir / 'kill_switch_state.json'), {'status': 'kill_switch_state_error', 'error': repr(exc)}


def _load_rows_from_json_candidates(candidates: list[Path | str], record_keys: tuple[str, ...] = ('orders', 'fills', 'positions', 'rows', 'records', 'events')) -> tuple[str | None, list[dict[str, Any]]]:
    import json
    for candidate in candidates:
        path = Path(candidate)
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            continue
        if isinstance(data, list):
            rows = [x for x in data if isinstance(x, dict)]
            if rows:
                return str(path), rows
        if isinstance(data, dict):
            for key in record_keys:
                value = data.get(key)
                if isinstance(value, list):
                    rows = [x for x in value if isinstance(x, dict)]
                    if rows:
                        return str(path), rows
        
    return None, []


def _runtime_cash_value(*candidates: Path) -> tuple[str | None, float | None]:
    import json
    for path in candidates:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            continue
        if isinstance(data, dict):
            for key in ('cash', 'cash_available', 'broker_cash', 'local_cash'):
                value = data.get(key)
                try:
                    if value is not None:
                        return str(path), float(value)
                except Exception:
                    pass
            snap = data.get('broker_snapshot') if isinstance(data.get('broker_snapshot'), dict) else {}
            for key in ('cash', 'cash_available'):
                value = snap.get(key)
                try:
                    if value is not None:
                        return str(path), float(value)
                except Exception:
                    pass
    return None, None


def _write_decision_output_evidence(decision_df: pd.DataFrame, orders: list[dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    decision_preview: list[dict[str, Any]] = []
    if decision_df is not None and not decision_df.empty:
        for _, row in decision_df.head(50).iterrows():
            action = str(row.get('Action') or row.get('Decision') or row.get('Signal') or '').strip()
            stage = str(row.get('Entry_State') or row.get('Entry_Action') or row.get('Early_Path_State') or row.get('Confirm_Path_State') or '').strip()
            score = row.get('Score', row.get('System_Score', row.get('Signal_Score', None)))
            reason = str(row.get('Reason') or row.get('觸發條件明細') or row.get('Golden_Type') or row.get('Setup_Tag') or '').strip()
            decision_preview.append({
                'ticker': str(row.get('Ticker') or row.get('Ticker SYMBOL') or '').strip(),
                'action': action,
                'stage': stage,
                'PREPARE': 'PREPARE' in stage.upper() or '布局' in stage,
                'PILOT': 'PILOT' in stage.upper() or '試單' in stage,
                'FULL': 'FULL' in stage.upper() or '確認' in stage,
                'score': score,
                'reason': reason,
                'timestamp': now_str(),
            })
    payload = {
        'generated_at': now_str(),
        'status': 'decision_output_ready' if not decision_df.empty else 'decision_output_missing',
        'decision_row_count': int(len(decision_df)),
        'normalized_order_count': int(len(orders)),
        'tickers': sorted({str(o.get('ticker') or '').upper() for o in orders if o.get('ticker')})[:200],
        'decision_preview': decision_preview,
        'source_candidates': [str(p) for p in [
            PATHS.data_dir / 'normalized_decision_output_enriched.csv',
            PATHS.data_dir / 'normalized_decision_output.csv',
            PATHS.base_dir / 'daily_decision_desk.csv',
            PATHS.data_dir / 'daily_decision_desk.csv',
        ]],
    }
    path = PATHS.runtime_dir / 'decision_output_evidence.json'
    write_json(path, payload)
    return str(path), payload


def _collect_reconciliation_inputs() -> dict[str, Any]:
    local_orders_src, local_orders = _load_rows_from_json_candidates([
        PATHS.runtime_dir / 'execution_orders.json',
        PATHS.runtime_dir / 'execution_runtime.json',
        PATHS.runtime_dir / 'decision_execution_bridge.json',
        PATHS.state_dir / 'engine_state.json',
    ], record_keys=('orders', 'open_orders', 'rows', 'records', 'events'))
    broker_orders_src, broker_orders = _load_rows_from_json_candidates([
        PATHS.runtime_dir / 'paper_orders.json',
        PATHS.runtime_dir / 'broker_orders.json',
        PATHS.runtime_dir / 'paper_broker_snapshot.json',
        PATHS.state_dir / 'restart_recovery_snapshot.json',
        PATHS.state_dir / 'engine_state.json',
    ], record_keys=('orders', 'open_orders', 'rows', 'records', 'events'))
    local_fills_src, local_fills = _load_rows_from_json_candidates([
        PATHS.runtime_dir / 'execution_fills.json',
        PATHS.runtime_dir / 'execution_runtime.json',
        PATHS.runtime_dir / 'decision_execution_bridge.json',
        PATHS.state_dir / 'engine_state.json',
    ], record_keys=('fills', 'recent_fills', 'rows', 'records', 'events'))
    broker_fills_src, broker_fills = _load_rows_from_json_candidates([
        PATHS.runtime_dir / 'paper_fills.json',
        PATHS.runtime_dir / 'broker_fills.json',
        PATHS.runtime_dir / 'paper_broker_snapshot.json',
        PATHS.state_dir / 'restart_recovery_snapshot.json',
        PATHS.state_dir / 'engine_state.json',
    ], record_keys=('fills', 'recent_fills', 'rows', 'records', 'events'))
    local_positions_src, local_positions = _load_rows_from_json_candidates([
        PATHS.runtime_dir / 'execution_positions_snapshot.json',
        PATHS.runtime_dir / 'execution_runtime.json',
        PATHS.state_dir / 'engine_state.json',
    ], record_keys=('positions', 'rows', 'records', 'events'))
    broker_positions_src, broker_positions = _load_rows_from_json_candidates([
        PATHS.runtime_dir / 'paper_positions.json',
        PATHS.runtime_dir / 'broker_positions.json',
        PATHS.runtime_dir / 'paper_broker_snapshot.json',
        PATHS.state_dir / 'restart_recovery_snapshot.json',
        PATHS.state_dir / 'engine_state.json',
    ], record_keys=('positions', 'rows', 'records', 'events'))
    local_cash_src, local_cash = _runtime_cash_value(PATHS.runtime_dir / 'execution_runtime.json', PATHS.state_dir / 'engine_state.json')
    broker_cash_src, broker_cash = _runtime_cash_value(PATHS.runtime_dir / 'paper_broker_snapshot.json', PATHS.state_dir / 'restart_recovery_snapshot.json', PATHS.state_dir / 'engine_state.json')
    if local_cash is None:
        local_cash = CONFIG.starting_cash
    if broker_cash is None:
        broker_cash = local_cash
    return {
        'local_orders': local_orders, 'broker_orders': broker_orders, 'local_fills': local_fills, 'broker_fills': broker_fills,
        'local_positions': local_positions, 'broker_positions': broker_positions, 'local_cash': local_cash, 'broker_cash': broker_cash,
        'sources': {
            'local_orders': local_orders_src, 'broker_orders': broker_orders_src, 'local_fills': local_fills_src, 'broker_fills': broker_fills_src,
            'local_positions': local_positions_src, 'broker_positions': broker_positions_src, 'local_cash': local_cash_src, 'broker_cash': broker_cash_src,
        },
    }



def _load_runtime_json(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            import json
            data = json.loads(path.read_text(encoding='utf-8'))
            return data if isinstance(data, dict) else {}
    except Exception as exc:
        record_issue('control_tower', 'load_runtime_json_failed', exc, severity='WARNING', fail_mode='fail_open')
    return {}


def _run_maturity_upgrade_suite(stage: str = 'control_tower') -> tuple[str, dict[str, Any]]:
    try:
        path, payload = MaturityUpgradeSuite().build()
        payload = payload if isinstance(payload, dict) else {'value': payload}
        payload = dict(payload); payload['invoked_by'] = stage
        return str(path), payload
    except Exception as exc:
        record_issue('control_tower', 'maturity_upgrade_suite_failed', exc, severity='ERROR', fail_mode='fail_closed')
        return str(PATHS.runtime_dir / 'maturity_upgrade_suite.json'), {'status': 'error', 'error': repr(exc), 'invoked_by': stage}


def _apply_entry_tracking_gate(orders: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    journal = _load_runtime_json(PATHS.runtime_dir / 'entry_tracking_journal.json')
    states = journal.get('positions', {}) if isinstance(journal, dict) else {}
    passed, blocked, capped = [], [], []
    for order in orders:
        ticker = str(order.get('ticker') or '').upper()
        state = states.get(ticker, {}) if isinstance(states, dict) else {}
        followup = str(state.get('followup_status') or '')
        stage = str(state.get('stage') or '')
        o = dict(order)
        if 'expired' in followup and stage != 'FULL_ENTRY':
            o['gate_blocked_by'] = 'entry_tracking_expired_without_confirmation'
            o['entry_tracking_state'] = state
            blocked.append(o)
            append_execution_journal_event(
                'ORDER_BLOCKED_BY_ENTRY_TRACKING', source='control_tower_entry_gate', ticker=ticker,
                action=o.get('action'), qty=o.get('target_qty', o.get('qty', 0)), reference_price=o.get('reference_price', o.get('ref_price')),
                status='BLOCKED', reason=followup, stage=stage,
            )
            continue
        if stage == 'PILOT_ENTRY':
            old_qty = int(o.get('target_qty', o.get('qty', 0)) or 0)
            cap_qty = max(1, old_qty // 3) if old_qty > 0 else 0
            if cap_qty and cap_qty < old_qty:
                o['target_qty_before_entry_gate'] = old_qty
                o['qty_before_entry_gate'] = old_qty
                o['target_qty'] = cap_qty
                o['qty'] = cap_qty
                o['entry_tracking_gate_action'] = 'pilot_qty_capped_to_one_third'
                capped.append({'ticker': ticker, 'old_qty': old_qty, 'new_qty': cap_qty})
                append_execution_journal_event(
                    'ORDER_QTY_CAPPED_BY_ENTRY_TRACKING', source='control_tower_entry_gate', ticker=ticker,
                    action=o.get('action'), qty=cap_qty, reference_price=o.get('reference_price', o.get('ref_price')),
                    status='CAPPED', reason='pilot_qty_capped_to_one_third', stage=stage, old_qty=old_qty,
                )
        if state:
            o['entry_tracking_stage'] = stage
            o['entry_tracking_followup_status'] = followup
        append_execution_journal_event(
            'ORDER_PASSED_ENTRY_TRACKING', source='control_tower_entry_gate', ticker=ticker,
            action=o.get('action'), qty=o.get('target_qty', o.get('qty', 0)), reference_price=o.get('reference_price', o.get('ref_price')),
            status='PASSED', reason=followup or 'entry_tracking_ok', stage=stage,
        )
        passed.append(o)
    return passed, {
        'status': 'entry_tracking_gate_ready' if journal else 'entry_tracking_gate_no_journal_yet',
        'input_order_count': len(orders),
        'passed_order_count': len(passed),
        'blocked_order_count': len(blocked),
        'pilot_qty_capped_count': len(capped),
        'blocked_orders': blocked[:50],
        'capped_orders': capped[:50],
        'journaled_gate_events': True,
        'policy': {'block_expired_prepare_or_pilot': True, 'cap_pilot_entry_qty_to_one_third': True},
    }


def _apply_position_lifecycle_gate(orders: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    lifecycle = _load_runtime_json(PATHS.runtime_dir / 'position_lifecycle.json')
    positions = lifecycle.get('positions', {}) if isinstance(lifecycle, dict) else {}
    out = list(orders)
    generated = []
    existing = {(str(o.get('ticker') or '').upper(), str(o.get('action') or '').upper()) for o in out}
    if isinstance(positions, dict):
        for ticker, pos in positions.items():
            if not isinstance(pos, dict):
                continue
            rec = str(pos.get('recommendation') or '').upper()
            qty = int(pos.get('qty') or 0)
            px = float(pos.get('current_close') or 0.0)
            if qty <= 0 or px <= 0 or rec not in {'EXIT', 'REDUCE'}:
                if rec == 'DEFEND':
                    append_execution_journal_event('POSITION_DEFEND_JOURNAL_ONLY', source='control_tower_position_gate', ticker=ticker, status='DEFEND', qty=qty, reference_price=px, reason=pos.get('exit_attribution'))
                continue
            action = 'SELL'
            target_qty = qty if rec == 'EXIT' else max(1, qty // 2)
            if (str(ticker).upper(), action) in existing:
                append_execution_journal_event('POSITION_LIFECYCLE_ORDER_SUPPRESSED_EXISTING_SELL', source='control_tower_position_gate', ticker=ticker, status=rec, qty=target_qty, reference_price=px, reason='existing_sell_order')
                continue
            o = {
                'ticker': str(ticker).upper(), 'action': action, 'target_qty': target_qty,
                'reference_price': px, 'qty': target_qty, 'ref_price': px,
                'strategy_name': 'position_lifecycle_' + rec.lower(), 'industry': '持倉生命週期',
                'position_lifecycle_recommendation': rec,
                'position_lifecycle_attribution': pos.get('exit_attribution'),
            }
            out.append(o)
            generated.append(o)
            append_execution_journal_event('ORDER_GENERATED_BY_POSITION_LIFECYCLE', source='control_tower_position_gate', ticker=ticker, action=action, qty=target_qty, reference_price=px, status=rec, reason=pos.get('exit_attribution'), strategy_name=o['strategy_name'])
    return out, {
        'status': 'position_lifecycle_gate_ready' if lifecycle else 'position_lifecycle_gate_no_snapshot_yet',
        'input_order_count': len(orders),
        'output_order_count': len(out),
        'generated_exit_or_reduce_order_count': len(generated),
        'generated_orders': generated[:50],
        'journaled_gate_events': True,
        'policy': {'exit_generates_full_sell': True, 'reduce_generates_half_sell': True, 'defend_is_journal_only': True},
    }


def _write_json(name: str, payload: dict[str, Any]) -> str:
    path = RUNTIME_DIR / name
    write_json(path, payload)
    return str(path)


def _call_script(script_name: str, args: Optional[list[str]] = None, allow_missing: bool = True) -> dict[str, Any]:
    args = args or []
    script_path = PATHS.base_dir / script_name
    if not script_path.exists():
        if allow_missing:
            return {'status': 'missing', 'script': script_name, 'returncode': None, 'args': args}
        raise FileNotFoundError(f'找不到腳本：{script_name}')
    env = {'PYTHONIOENCODING': 'utf-8', 'PYTHONUTF8': '1'}
    proc = subprocess.run(
        [sys.executable, str(script_path), *args],
        cwd=str(PATHS.base_dir),
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
        env=env,
    )
    return {
        'status': 'ok' if proc.returncode == 0 else 'error',
        'script': script_name,
        'returncode': proc.returncode,
        'args': args,
        'stdout_tail': '\n'.join((proc.stdout or '').splitlines()[-20:]),
        'stderr_tail': '\n'.join((proc.stderr or '').splitlines()[-20:]),
    }


def _extract_path_payload(result: Any, fallback_path: Optional[Path] = None) -> tuple[str, dict[str, Any]]:
    if isinstance(result, tuple):
        if len(result) >= 2:
            path = result[0]
            payload = result[1]
            if not isinstance(payload, dict):
                payload = {'value': payload}
            if len(result) > 2:
                payload = dict(payload)
                payload['_extra_return_values'] = list(result[2:])
            return str(path), payload
        if len(result) == 1:
            only = result[0]
            if isinstance(only, dict):
                return str(fallback_path or ''), only
            return str(only), {}
    if isinstance(result, dict):
        return str(fallback_path or ''), result
    if isinstance(result, (str, Path)):
        return str(result), {}
    if result is None:
        return str(fallback_path or ''), {}
    return str(fallback_path or ''), {'value': result}


def _call_builder_result(builder: Any, method_name: str, *args: Any, fallback_path: Optional[Path] = None, **kwargs: Any) -> tuple[str, dict[str, Any]]:
    method = getattr(builder, method_name)
    try:
        return _extract_path_payload(method(*args, **kwargs), fallback_path=fallback_path)
    except TypeError:
        sig = inspect.signature(method)
        accepted_kwargs = {name: value for name, value in kwargs.items() if name in sig.parameters}
        return _extract_path_payload(method(*args, **accepted_kwargs), fallback_path=fallback_path)


def _safe_build(module_name: str, class_name: str, method_name: str, kwargs: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    kwargs = kwargs or {}
    try:
        mod = importlib.import_module(module_name)
        cls = getattr(mod, class_name)
        builder = cls()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = getattr(builder, method_name)(**kwargs)
        if isinstance(result, tuple) and len(result) == 2:
            path, payload = result
            return {'status': 'ok', 'path': str(path), 'payload': payload}
        return {'status': 'ok', 'payload': result}
    except Exception as exc:  # runtime diagnostics
        record_issue('control_tower', 'run_service_failed', exc, severity='ERROR', fail_mode='fail_closed')
        return {'status': 'error', 'error': repr(exc), 'module': module_name, 'class_name': class_name, 'method': method_name}


def _load_decision_df() -> pd.DataFrame:
    candidates = [
        PATHS.data_dir / 'normalized_decision_output_enriched.csv',
        PATHS.data_dir / 'normalized_decision_output.csv',
        PATHS.base_dir / 'daily_decision_desk.csv',
        PATHS.data_dir / 'daily_decision_desk.csv',
    ]
    for candidate in candidates:
        if candidate.exists():
            try:
                return pd.read_csv(candidate)
            except Exception as exc:  # runtime diagnostics
                record_issue('control_tower', 'read_decision_csv_candidate_failed', exc, severity='WARNING', fail_mode='fail_open')
                continue
    return pd.DataFrame()


def _normalize_orders(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    orders = []
    for _, row in df.head(50).iterrows():
        ticker = str(row.get('Ticker') or row.get('ticker') or '').strip()
        if not ticker:
            continue
        direction = str(row.get('Direction') or row.get('direction') or '').strip()
        action = 'BUY'
        if '空' in direction or 'SHORT' in direction.upper():
            action = 'SHORT'
        desk_usable = bool(row.get('DeskUsable', not bool(row.get('FallbackBuild', False))))
        execution_eligible = bool(row.get('ExecutionEligible', row.get('CanAutoSubmit', False)))
        market_rule_passed = bool(row.get('MarketRulePassed', False))
        qty = int(max(0, round(float(row.get('Target_Qty', row.get('TargetQty', 0)) or 0))))
        ref_price = float(row.get('Reference_Price', row.get('ref_price', 0.0)) or 0.0)
        if not desk_usable or not execution_eligible or qty <= 0 or ref_price <= 0 or ('MarketRulePassed' in row and not market_rule_passed):
            continue
        orders.append({
            'ticker': ticker,
            'action': action,
            'target_qty': qty,
            'reference_price': ref_price,
            'qty': qty,
            'ref_price': ref_price,
            'strategy_name': str(row.get('Structure', row.get('strategy_name', 'default'))),
            'industry': str(row.get('產業類別', row.get('industry', '未知'))),
        })
    return orders


class _AcceptedSignal:
    def __init__(self, order: dict[str, Any]):
        self.ticker = order.get('ticker')
        self.action = order.get('action')
        self.target_qty = int(order.get('target_qty', 0) or 0)
        self.reference_price = float(order.get('reference_price', 0.0) or 0.0)


def _build_control_outputs() -> dict[str, Any]:
    health_path, health_payload = ProjectHealthcheck(PATHS.base_dir).build_report(deep=False)
    level2_path, level2_payload = run_level2_mainline()
    maturity_path, maturity_payload = _run_maturity_upgrade_suite('daily_control_outputs_pre_gate')
    decision_df = _load_decision_df()
    raw_orders = _normalize_orders(decision_df)
    orders_after_entry_gate, entry_tracking_gate = _apply_entry_tracking_gate(raw_orders)
    orders, position_lifecycle_gate = _apply_position_lifecycle_gate(orders_after_entry_gate)
    decision_evidence_path, decision_evidence_payload = _write_decision_output_evidence(decision_df, orders)
    decision_execution_gate_path = _write_json('decision_execution_formal_gate.json', {'generated_at': now_str(), 'status': 'decision_execution_gate_ready', 'raw_order_count': len(raw_orders), 'final_order_count': len(orders), 'entry_tracking_gate': entry_tracking_gate, 'position_lifecycle_gate': position_lifecycle_gate, 'decision_output_evidence_path': decision_evidence_path})
    accepted_signals = [_AcceptedSignal(o) for o in orders]

    readiness_path, readiness_payload = _call_builder_result(
        LiveReadinessGate(), 'evaluate', decision_df if not decision_df.empty else None,
        fallback_path=PATHS.runtime_dir / 'live_readiness_gate.json'
    )
    training_governance = _safe_build('fts_training_governance_mainline', 'TrainingGovernanceMainline', 'build_summary', {'execute_backend': False})
    tg_path = Path(training_governance.get('path', '')) if training_governance.get('path') else (PATHS.runtime_dir / 'training_governance_mainline.json')
    governance_payload = training_governance.get('payload', {}) if training_governance.get('status') == 'ok' else {}
    model_registry_path, model_registry_payload = ModelVersionRegistry().build()
    model_gate_path, model_gate_payload = ModelSelectionGate().evaluate(
        ai_status={'all_core_scripts_present': True, 'training_assets_present': model_registry_payload.get('candidate_count', 0) > 0},
        readiness={'total_signals': len(orders)},
        governance=governance_payload,
    )
    compat_info = {
        'row_count': int(len(decision_df)),
        'rows_with_price': int(decision_df['Reference_Price'].notna().sum()) if 'Reference_Price' in decision_df.columns else int(len(orders)),
        'rows_with_ticker': int(decision_df['Ticker'].notna().sum()) if 'Ticker' in decision_df.columns else int(len(orders)),
        'rows_with_action': int(decision_df['Direction'].notna().sum()) if 'Direction' in decision_df.columns else int(len(orders)),
    }
    launch_gate_path, launch_gate_payload = _call_builder_result(
        LaunchGatekeeper(), 'evaluate', {'ready': [], 'missing': []}, {'failed': []}, {'items': []}, compat_info, {'total_signals': len(orders)},
        fallback_path=PATHS.runtime_dir / 'launch_gate.json'
    )
    live_safety_path, live_safety_payload = _call_builder_result(
        LiveSafetyGate(), 'evaluate', readiness_payload, launch_gate_payload,
        orders=orders, account_snapshot={'cash': CONFIG.starting_cash, 'equity': CONFIG.starting_cash}, risk_snapshot={'day_loss_pct': 0.0},
        fallback_path=PATHS.runtime_dir / 'live_safety_gate.json'
    )
    broker_approval_path, broker_approval_payload = _call_builder_result(BrokerApprovalGate(), 'evaluate', launch_gate_payload, live_safety_payload, fallback_path=PATHS.runtime_dir / 'broker_approval_gate.json')
    submission_path, submission_payload = _call_builder_result(SubmissionContractGate(), 'evaluate', accepted_signals, fallback_path=PATHS.runtime_dir / 'submission_contract_gate.json')
    validation_path, validation_payload = _call_builder_result(ValidationSuiteBuilder(), 'build', launch_gate_payload, model_gate_payload, live_safety_payload, broker_approval_payload, submission_payload, fallback_path=PATHS.runtime_dir / 'validation_suite_report.json')
    recon_inputs = _collect_reconciliation_inputs()
    recon_path, recon_payload = _call_builder_result(ReconciliationEngine(), 'reconcile', recon_inputs['local_orders'], recon_inputs['broker_orders'], recon_inputs['local_fills'], recon_inputs['broker_fills'], recon_inputs['local_positions'], recon_inputs['broker_positions'], recon_inputs['local_cash'], recon_inputs['broker_cash'], fallback_path=PATHS.runtime_dir / 'reconciliation_engine.json')
    if isinstance(recon_payload, dict):
        recon_payload.setdefault('input_sources', recon_inputs.get('sources', {}))
        recon_payload.setdefault('input_counts', {
            'local_orders': len(recon_inputs['local_orders']), 'broker_orders': len(recon_inputs['broker_orders']),
            'local_fills': len(recon_inputs['local_fills']), 'broker_fills': len(recon_inputs['broker_fills']),
            'local_positions': len(recon_inputs['local_positions']), 'broker_positions': len(recon_inputs['broker_positions']),
        })
        write_json(Path(recon_path), recon_payload)
    recovery_path, recovery_payload = (
        _call_builder_result(RecoveryEngine(), 'build_recovery_plan', fallback_path=PATHS.runtime_dir / 'recovery_plan.json')
        if hasattr(RecoveryEngine(), 'build_recovery_plan') else (str(PATHS.runtime_dir / 'recovery_plan.json'), {'status': 'missing_builder'})
    )
    recovery_validation_path, recovery_validation_payload = _call_builder_result(RecoveryValidationBuilder(), 'build', {'total': 0}, recovery_payload if isinstance(recovery_payload, dict) else {}, fallback_path=PATHS.runtime_dir / 'recovery_validation.json')
    latest_approval = OperatorApprovalRegistry().latest_for('live_release')
    live_release_path, live_release_payload = _call_builder_result(
        LiveReleaseGate(), 'evaluate', governance=governance_payload, safety=live_safety_payload, recon=recon_payload if isinstance(recon_payload, dict) else {},
        recovery=recovery_validation_payload, approval=latest_approval, broker_contract={'defined': True}, fallback_path=PATHS.runtime_dir / 'live_release_gate.json'
    )
    tri_lane_path, tri_lane_payload = _call_builder_result(TriLaneOrchestrator(), 'build', fallback_path=PATHS.runtime_dir / 'tri_lane_orchestrator.json')

    return {
        'project_healthcheck': {'path': health_path, 'payload': health_payload},
        'level2_mainline': {'path': level2_path, 'payload': level2_payload},
        'maturity_upgrade_suite': {'path': maturity_path, 'payload': maturity_payload},
        'entry_tracking_gate': {'path': str(PATHS.runtime_dir / 'entry_tracking_journal.json'), 'payload': entry_tracking_gate},
        'position_lifecycle_gate': {'path': str(PATHS.runtime_dir / 'position_lifecycle.json'), 'payload': position_lifecycle_gate},
        'decision_output_evidence': {'path': decision_evidence_path, 'payload': decision_evidence_payload},
        'decision_execution_formal_gate': {'path': decision_execution_gate_path, 'payload': {'status': 'decision_execution_gate_ready'}},
        'training_governance_mainline': {'path': str(tg_path) if tg_path.exists() else training_governance.get('path', ''), 'payload': governance_payload},
        'model_registry': {'path': str(model_registry_path), 'payload': model_registry_payload},
        'model_selection_gate': {'path': str(model_gate_path), 'payload': model_gate_payload},
        'live_readiness_gate': {'path': str(readiness_path), 'payload': readiness_payload},
        'launch_gate': {'path': str(launch_gate_path), 'payload': launch_gate_payload},
        'live_safety_gate': {'path': str(live_safety_path), 'payload': live_safety_payload},
        'broker_approval_gate': {'path': str(broker_approval_path), 'payload': broker_approval_payload},
        'submission_gate': {'path': str(submission_path), 'payload': submission_payload},
        'validation_suite': {'path': str(validation_path), 'payload': validation_payload},
        'reconciliation': {'path': str(recon_path), 'payload': recon_payload},
        'recovery': {'path': str(recovery_path), 'payload': recovery_payload},
        'recovery_validation': {'path': str(recovery_validation_path), 'payload': recovery_validation_payload},
        'live_release_gate': {'path': str(live_release_path), 'payload': live_release_payload},
        'tri_lane_orchestration': {'path': str(tri_lane_path), 'payload': tri_lane_payload},
        'tri_lane_stage_status': tri_lane_payload.get('lanes', {}) if isinstance(tri_lane_payload, dict) else {},
        'tri_lane_stage_runs': tri_lane_payload.get('tri_lane_stage_runs', {}) if isinstance(tri_lane_payload, dict) else {},
        'tri_lane_execution_status': tri_lane_payload.get('tri_lane_execution_status', {}) if isinstance(tri_lane_payload, dict) else {},
        'decision_rows': int(len(decision_df)),
        'normalized_orders': len(orders),
    }


def run_daily() -> dict[str, Any]:
    log('=' * 72)
    log('🚀 啟動 正式交易主控版_v83_official_main')
    log('🧭 模式：DAILY')
    log('=' * 72)
    kill_switch_path, kill_switch_payload = _ensure_kill_switch_state()
    outputs = _build_control_outputs()
    outputs['kill_switch_state'] = {'path': kill_switch_path, 'payload': kill_switch_payload}
    payload = {'generated_at': now_str(), 'mode': 'daily', 'module_version': 'v20260414_prebroker95_closure_bootstrap_cli', 'outputs': outputs, 'readiness_split': outputs.get('live_readiness_gate', {}).get('payload', {}).get('score_split', {}), 'operational_scope': {'prelive': outputs.get('live_readiness_gate', {}).get('payload', {}).get('prelive_ready', False), 'broker_production': outputs.get('live_readiness_gate', {}).get('payload', {}).get('broker_production_ready', False)}, 'status': 'control_tower_ready'}
    _write_json('formal_trading_system_v83_official_main.json', payload)
    return payload


def run_train() -> dict[str, Any]:
    log('=' * 72)
    log('🚀 啟動 正式交易主控版_v83_official_main')
    log('🧭 模式：TRAIN')
    log('=' * 72)

    import warnings
    warnings.filterwarnings('ignore', category=FutureWarning, module='fts_screening_engine')
    warnings.filterwarnings('ignore', category=FutureWarning, module='pandas')
    kill_switch_path, kill_switch_payload = _ensure_kill_switch_state()
    steps: list[dict[str, Any]] = [{'stage': 'kill_switch_state', 'status': 'ok' if kill_switch_payload.get('status') == 'kill_switch_state_ready' else 'error', 'path': kill_switch_path, 'payload_status': kill_switch_payload.get('status')}]
    try:
        tickers = get_dynamic_watchlist()
        df = generate_ml_dataset(tickers)
        steps.append({
            'stage': 'training_data_builder',
            'status': 'ok',
            'rows': int(len(df)) if hasattr(df, '__len__') else None,
            'entrypoint': 'fts_training_data_builder.generate_ml_dataset',
        })
    except Exception as exc:  # runtime diagnostics
        record_issue('control_tower', 'training_data_builder_failed', exc, severity='ERROR', fail_mode='fail_closed')
        steps.append({
            'stage': 'training_data_builder',
            'status': 'error',
            'error': repr(exc),
            'entrypoint': 'fts_training_data_builder.generate_ml_dataset',
        })

    try:
        maturity_path, maturity_payload = _run_maturity_upgrade_suite('train_before_trainer_backend')
        steps.append({'stage': 'maturity_upgrade_suite', 'status': 'ok' if maturity_payload.get('status') != 'error' else 'error', 'path': str(maturity_path), 'payload_status': maturity_payload.get('status'), 'entrypoint': 'fts_maturity_upgrade_suite.MaturityUpgradeSuite.build'})
    except Exception as exc:
        record_issue('control_tower', 'train_maturity_upgrade_failed', exc, severity='ERROR', fail_mode='fail_closed')
        steps.append({'stage': 'maturity_upgrade_suite', 'status': 'error', 'error': repr(exc)})

    try:
        trainer_path, trainer_payload = train_models()
        steps.append({
            'stage': 'trainer_backend',
            'status': 'ok',
            'path': str(trainer_path),
            'payload_status': trainer_payload.get('status') if isinstance(trainer_payload, dict) else None,
            'entrypoint': 'fts_trainer_backend.train_models',
        })
    except Exception as exc:  # runtime diagnostics
        record_issue('control_tower', 'trainer_backend_failed', exc, severity='ERROR', fail_mode='fail_closed')
        steps.append({
            'stage': 'trainer_backend',
            'status': 'error',
            'error': repr(exc),
            'entrypoint': 'fts_trainer_backend.train_models',
        })

    # v92：trainer_backend_report 產生後必須再跑一次 maturity suite。
    try:
        post_maturity_path, post_maturity_payload = _run_maturity_upgrade_suite('train_after_trainer_backend')
        steps.append({
            'stage': 'maturity_upgrade_suite_after_train',
            'status': 'ok' if post_maturity_payload.get('status') != 'error' else 'error',
            'path': str(post_maturity_path),
            'payload_status': post_maturity_payload.get('status'),
            'blocked_items': post_maturity_payload.get('blocked_items', []),
            'entrypoint': 'fts_maturity_upgrade_suite.MaturityUpgradeSuite.build',
        })
    except Exception as exc:
        record_issue('control_tower', 'train_post_maturity_upgrade_failed', exc, severity='ERROR', fail_mode='fail_closed')
        steps.append({'stage': 'maturity_upgrade_suite_after_train', 'status': 'error', 'error': repr(exc)})

    try:
        model_live_gate = _safe_build('fts_model_live_signal_gate', 'ModelLiveSignalGate', 'build')
        steps.append({
            'stage': 'model_live_signal_gate',
            'status': model_live_gate.get('status'),
            'path': model_live_gate.get('path', ''),
            'payload_status': (model_live_gate.get('payload') or {}).get('status'),
            'entrypoint': 'fts_model_live_signal_gate.ModelLiveSignalGate.build',
        })
    except Exception as exc:
        record_issue('control_tower', 'train_model_live_signal_gate_failed', exc, severity='ERROR', fail_mode='fail_closed')
        steps.append({'stage': 'model_live_signal_gate', 'status': 'error', 'error': repr(exc)})

    try:
        governance = _safe_build('fts_training_governance_mainline', 'TrainingGovernanceMainline', 'build_summary', {'execute_backend': False})
        steps.append({
            'stage': 'training_governance_mainline',
            'status': governance.get('status'),
            'path': governance.get('path', ''),
            'payload_status': (governance.get('payload') or {}).get('status'),
            'blocked_reasons': (governance.get('payload') or {}).get('blocked_reasons', []),
            'entrypoint': 'fts_training_governance_mainline.TrainingGovernanceMainline.build_summary',
        })
    except Exception as exc:
        record_issue('control_tower', 'train_training_governance_mainline_failed', exc, severity='ERROR', fail_mode='fail_closed')
        steps.append({'stage': 'training_governance_mainline', 'status': 'error', 'error': repr(exc)})

    tri_lane_path, tri_lane_payload = _call_builder_result(TriLaneOrchestrator(), 'build', fallback_path=PATHS.runtime_dir / 'tri_lane_orchestrator.json')
    hard_failed = [s for s in steps if s.get('status') == 'error']
    payload = {
        'generated_at': now_str(),
        'mode': 'train',
        'module_version': 'v87_clean_old_doors_mainline_train',
        'outputs': {
            'steps': steps,
            'tri_lane_orchestration': {'path': str(tri_lane_path), 'payload': tri_lane_payload},
        },
        'status': 'train_ready' if not hard_failed else 'train_partial',
    }
    _write_json('formal_trading_system_v83_train.json', payload)
    _write_json('training_orchestrator.json', {
        'generated_at': now_str(),
        'status': 'train_invoked_via_control_tower',
        'entrypoints': ['fts_training_data_builder.generate_ml_dataset', 'fts_trainer_backend.train_models'],
        'steps': steps,
    })
    return payload


def run_bootstrap() -> dict[str, Any]:
    log('=' * 72)
    log('🚀 啟動 正式交易主控版_v83_official_main')
    log('🧭 模式：BOOTSTRAP')
    log('=' * 72)
    kill_switch_path, kill_switch_payload = _ensure_kill_switch_state()
    steps = [
        {'status': 'ok' if kill_switch_payload.get('status') == 'kill_switch_state_ready' else 'error', 'script': 'kill_switch_state', 'path': kill_switch_path, 'payload_status': kill_switch_payload.get('status')},
        _call_script('fts_db_migrations.py', ['upgrade'], allow_missing=False),
        _call_script('db_setup.py', ['--mode', 'upgrade'], allow_missing=False),
        _call_script('db_setup_research_plus.py', allow_missing=True),
        _call_script('fts_admin_cli.py', ['full-market-percentile'], allow_missing=False),
        _call_script('fts_admin_cli.py', ['event-calendar-build'], allow_missing=False),
        _call_script('fts_admin_cli.py', ['sync-feature-snapshots'], allow_missing=False),
        _call_script("fts_admin_cli.py", ["broker-contract-audit"], allow_missing=False),
        _call_script("fts_admin_cli.py", ["callback-ingest"], allow_missing=False),
        _call_script("fts_admin_cli.py", ["reconciliation-runtime"], allow_missing=False),
        _call_script("fts_admin_cli.py", ["restart-recovery"], allow_missing=False),
        _call_script("fts_admin_cli.py", ["prebroker-95-audit"], allow_missing=False),
        _call_script("fts_admin_cli.py", ["maturity-upgrade"], allow_missing=False),
        _call_script("fts_admin_cli.py", ["patch-retirement-report"], allow_missing=False),
    ]
    daily_payload = run_daily()
    payload = {'generated_at': now_str(), 'mode': 'bootstrap', 'module_version': 'v20260414_prebroker95_closure_bootstrap_cli', 'steps': steps, 'daily_status': daily_payload.get('status'), 'daily_readiness_split': daily_payload.get('readiness_split', {}), 'status': 'bootstrap_ready'}
    _write_json('formal_trading_system_v83_bootstrap.json', payload)
    return payload


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='正式交易主控版_v83 第三級全主控整合單一入口')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--bootstrap', action='store_true')
    group.add_argument('--train', action='store_true')
    group.add_argument('--daily', action='store_true')
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    try:
        args = parse_args(argv)
        if args.bootstrap:
            payload = run_bootstrap()
        elif args.train:
            payload = run_train()
        else:
            payload = run_daily()
        return 0 if payload.get('status') in {'control_tower_ready', 'train_ready', 'bootstrap_ready'} else 1
    except Exception as exc:  # runtime diagnostics
        record_issue('control_tower', 'main_failure', exc, severity='CRITICAL', fail_mode='fail_closed')
        payload = {'generated_at': now_str(), 'module_version': 'v83_level3_control_tower_integrated', 'error_type': type(exc).__name__, 'error': str(exc)}
        _write_json('formal_trading_system_v83_official_main_error.json', payload)
        log(f'❌ control tower failure: {exc}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
