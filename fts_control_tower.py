# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import contextlib
import importlib
import inspect
import io
import json
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
from fts_real_api_readiness import RealAPIReadinessBuilder
from fts_true_broker_readiness_gate import TrueBrokerReadinessGate
from fts_true_broker_live_closure import TrueBrokerLiveClosureService
from fts_operations_suite import OperatorApprovalRegistry
from fts_execution_runtime import RecoveryEngine
from fts_restart_recovery_service import RestartRecoveryService
from fts_project_quality_suite import RecoveryValidationBuilder
from fts_execution_runtime import ReconciliationEngine
from fts_tri_lane_orchestrator import TriLaneOrchestrator
from fts_prelive_runtime import write_json
from fts_training_data_builder import get_dynamic_watchlist, generate_ml_dataset
from fts_decision_desk_builder import DecisionDeskBuilder
from fts_trainer_backend import train_models
from fts_maturity_upgrade_suite import MaturityUpgradeSuite
from fts_execution_journal_service import append_execution_journal_event
from fts_kill_switch import KillSwitchManager

RUNTIME_DIR = PATHS.runtime_dir

def _write_json(filename: str | Path, payload: dict[str, Any]) -> str:
    path = Path(filename)
    if not path.is_absolute():
        path = PATHS.runtime_dir / path
    write_json(path, payload)
    return str(path)


def _call_script(script: str, args: list[str] | None = None, allow_missing: bool = True, raise_on_nonzero: bool = False) -> dict[str, Any]:
    args = list(args or [])
    script_path = PATHS.base_dir / script

    if not script_path.exists():
        payload = {
            'script': script,
            'args': args,
            'status': 'missing',
            'path': str(script_path),
            'allow_missing': bool(allow_missing),
        }
        if allow_missing:
            return payload
        raise FileNotFoundError(f'Bootstrap required script not found: {script_path}')

    cmd = [sys.executable, '-u', str(script_path), *args]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(PATHS.base_dir),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
        )
        stdout_text = (proc.stdout or '').strip()
        stderr_text = (proc.stderr or '').strip()

        if stdout_text:
            log(stdout_text)
        if stderr_text:
            log(stderr_text)

        payload = {
            'script': script,
            'args': args,
            'status': 'ok' if proc.returncode == 0 else 'error',
            'returncode': int(proc.returncode),
            'path': str(script_path),
            'stdout_tail': stdout_text[-2000:] if stdout_text else '',
            'stderr_tail': stderr_text[-2000:] if stderr_text else '',
        }
        if proc.returncode != 0 and raise_on_nonzero:
            raise RuntimeError(f'{script} failed with returncode={proc.returncode}')
        return payload
    except Exception as exc:
        payload = {
            'script': script,
            'args': args,
            'status': 'error',
            'path': str(script_path),
            'error_type': type(exc).__name__,
            'error': str(exc),
        }
        if allow_missing:
            return payload
        raise



def _admin_cli_help_text() -> str:
    cli_path = PATHS.base_dir / 'fts_admin_cli.py'
    if not cli_path.exists():
        return ''
    proc = subprocess.run(
        [sys.executable, '-u', str(cli_path), '--help'],
        cwd=str(PATHS.base_dir),
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
    )
    return (proc.stdout or '') + '\n' + (proc.stderr or '')


def _admin_cli_supported_commands() -> set[str]:
    try:
        from fts_admin_cli import _COMMANDS, _ALIASES  # type: ignore
        commands = set(_COMMANDS.keys())
        commands.update(_ALIASES.keys())
        commands.update(_ALIASES.values())
        return {str(c) for c in commands}
    except Exception:
        help_text = _admin_cli_help_text()
        out: set[str] = set()
        for token in help_text.replace('\n', ' ').split():
            cleaned = token.strip(" ,{}[]()")
            if cleaned and ('-' in cleaned or cleaned.isidentifier()):
                out.add(cleaned)
        return out


def _admin_cli_supports(command: str) -> bool:
    return command in _admin_cli_supported_commands()

def _call_admin_command(
    command: str,
    args: list[str] | None = None,
    allow_missing: bool = True,
    raise_on_nonzero: bool = False,
) -> dict[str, Any]:
    args = list(args or [])

    if not _admin_cli_supports(command):
        payload = {
            'script': 'fts_admin_cli.py',
            'command': command,
            'args': args,
            'status': 'unsupported',
            'allow_missing': bool(allow_missing),
        }
        if allow_missing:
            return payload
        raise ValueError(f'Unsupported admin command: {command}')

    return _call_script(
        'fts_admin_cli.py',
        [command, *args],
        allow_missing=allow_missing,
        raise_on_nonzero=raise_on_nonzero,
    )
    
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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
    try:
        if value is None or pd.isna(value):
            return default
        if isinstance(value, str):
            return value.strip().lower() in {'1', 'true', 'yes', 'y', 'on'}
        return bool(value)
    except Exception:
        return default


def _entry_thresholds() -> tuple[float, float, float]:
    try:
        from config import PARAMS as _PARAMS  # type: ignore
    except Exception:
        _PARAMS = {}
    prepare_min = float(_PARAMS.get('PREENTRY_PILOT_THRESHOLD', 0.58))
    full_min = float(_PARAMS.get('CONFIRM_FULL_THRESHOLD', 0.66))
    readiness_min = float(_PARAMS.get('ENTRY_READINESS_PREPARE_MIN', 0.45))
    return prepare_min, full_min, readiness_min


def _infer_entry_stage(row: pd.Series | dict[str, Any]) -> str:
    stage = str(row.get('Entry_State') or row.get('Entry_Action') or row.get('Early_Path_State') or row.get('Confirm_Path_State') or '' ).strip()
    if stage:
        return stage
    prepare_min, full_min, readiness_min = _entry_thresholds()
    preentry_score = _safe_float(row.get('PreEntry_Score', 0.0), 0.0)
    confirm_score = _safe_float(row.get('Confirm_Entry_Score', 0.0), 0.0)
    entry_readiness = _safe_float(row.get('Entry_Readiness', 0.0), 0.0)
    if confirm_score >= full_min:
        return 'FULL_ENTRY'
    if preentry_score >= prepare_min:
        return 'PILOT_ENTRY'
    if entry_readiness >= readiness_min or preentry_score > 0 or confirm_score > 0:
        return 'PREPARE'
    return ''


def _build_order_blockers(row: pd.Series | dict[str, Any], ticker: str, action: str, stage: str) -> list[str]:
    blockers: list[str] = []
    desk_usable = _safe_bool(row.get('DeskUsable', not _safe_bool(row.get('FallbackBuild', False), False)), True)
    execution_eligible = _safe_bool(row.get('ExecutionEligible', row.get('CanAutoSubmit', False)), False)
    market_rule_known = 'MarketRulePassed' in row
    market_rule_passed = _safe_bool(row.get('MarketRulePassed', False), False)
    qty = int(max(0, round(_safe_float(row.get('Target_Qty', row.get('TargetQty', 0)), 0.0))))
    ref_price = _safe_float(row.get('Reference_Price', row.get('ref_price', 0.0)), 0.0)
    active_position_qty = int(max(0, round(_safe_float(row.get('Active_Position_Qty', row.get('Position_Qty', row.get('持倉張數', 0))), 0.0))))
    if not desk_usable:
        blockers.append('desk_unusable')
    if not execution_eligible:
        blockers.append('execution_ineligible')
    if qty <= 0:
        blockers.append('target_qty_missing_or_zero')
    if ref_price <= 0:
        blockers.append('reference_price_missing_or_zero')
    if market_rule_known and not market_rule_passed:
        blockers.append('market_rule_blocked')
    if action in {'BUY', 'LONG', 'SHORT'} and not stage:
        blockers.append('entry_stage_missing')
    if action in {'SELL', 'COVER'} and active_position_qty <= 0:
        blockers.append('exit_without_active_position')
    return list(dict.fromkeys(blockers))


def _classify_domain_from_row(row: pd.Series | dict[str, Any], blockers: list[str] | None = None) -> str:
    blockers = [str(x) for x in (blockers or []) if str(x)]
    row = row if isinstance(row, dict) else row.to_dict()
    strategy = [b for b in blockers if not b.startswith(('desk_', 'execution_', 'target_qty_', 'reference_price_', 'market_rule_', 'entry_stage_', 'exit_without_'))]
    engineering = [b for b in blockers if b not in strategy]
    if strategy and engineering:
        return 'mixed'
    if strategy:
        return 'strategy'
    if engineering:
        return 'engineering'
    if bool(row.get('NearMissFlag', False)):
        return 'strategy'
    return 'clean'

def _write_decision_output_evidence(decision_df: pd.DataFrame, raw_orders: list[dict[str, Any]], final_orders: list[dict[str, Any]], normalization_blocked: list[dict[str, Any]] | None = None) -> tuple[str, dict[str, Any]]:
    decision_preview: list[dict[str, Any]] = []
    near_miss_preview: list[dict[str, Any]] = []
    action_counts: dict[str, int] = {}
    stage_counts = {'PREPARE': 0, 'PILOT': 0, 'FULL': 0}
    normalization_blocked = normalization_blocked or []
    strategy_vs_engineering = {'strategy': 0, 'engineering': 0, 'mixed': 0, 'clean': 0}
    if decision_df is not None and not decision_df.empty:
        for _, row in decision_df.head(80).iterrows():
            action = str(row.get('Action') or row.get('Decision') or row.get('Signal') or '').strip().upper()
            if not action:
                stage = _infer_entry_stage(row)
                direction = str(row.get('Direction') or row.get('direction') or '').strip().upper()
                if stage in {'PILOT_ENTRY', 'FULL_ENTRY'}:
                    action = 'SHORT' if ('空' in direction or 'SHORT' in direction) else 'BUY'
                elif 'EXIT' in str(row.get('Exit_State') or '').upper() or 'REDUCE' in str(row.get('Exit_Action') or '').upper():
                    action = 'SELL'
                else:
                    action = 'HOLD'
            stage = _infer_entry_stage(row)
            score = row.get('Score', row.get('System_Score', row.get('Signal_Score', None)))
            preentry_score = _safe_float(row.get('PreEntry_Score', 0.0), 0.0)
            confirm_score = _safe_float(row.get('Confirm_Entry_Score', 0.0), 0.0)
            entry_readiness = _safe_float(row.get('Entry_Readiness', 0.0), 0.0)
            breakout_risk = _safe_float(row.get('Breakout_Risk_Next3', 0.0), 0.0)
            reversal_risk = _safe_float(row.get('Reversal_Risk_Next3', 0.0), 0.0)
            exit_hazard = _safe_float(row.get('Exit_Hazard_Score', 0.0), 0.0)
            reason = str(row.get('Reason') or row.get('觸發條件明細') or row.get('Golden_Type') or row.get('Setup_Tag') or '').strip()
            prepare = 'PREPARE' in stage.upper() or '布局' in stage
            pilot = 'PILOT' in stage.upper() or '試單' in stage
            full = 'FULL' in stage.upper() or '確認' in stage
            if prepare:
                stage_counts['PREPARE'] += 1
            if pilot:
                stage_counts['PILOT'] += 1
            if full:
                stage_counts['FULL'] += 1
            action_counts[action or 'UNKNOWN'] = action_counts.get(action or 'UNKNOWN', 0) + 1
            blockers = _build_order_blockers(row, str(row.get('Ticker') or row.get('Ticker SYMBOL') or '').strip(), action, stage)
            if not reason and blockers:
                reason = '|'.join(blockers[:4])
            decision_preview.append({
                'ticker': str(row.get('Ticker') or row.get('Ticker SYMBOL') or '').strip(),
                'action': action,
                'stage': stage,
                'PREPARE': prepare,
                'PILOT': pilot,
                'FULL': full,
                'score': score,
                'preentry_score': round(preentry_score, 4),
                'confirm_score': round(confirm_score, 4),
                'entry_readiness': round(entry_readiness, 4),
                'breakout_risk': round(breakout_risk, 4),
                'reversal_risk': round(reversal_risk, 4),
                'exit_hazard': round(exit_hazard, 4),
                'reason': reason,
                'blockers': blockers[:8],
                'timestamp': now_str(),
            })
            domain = _classify_domain_from_row(row, blockers)
            strategy_vs_engineering[domain] = strategy_vs_engineering.get(domain, 0) + 1
            if blockers and (prepare or pilot or full or preentry_score > 0 or confirm_score > 0 or entry_readiness > 0):
                near_miss_preview.append({
                    'ticker': str(row.get('Ticker') or row.get('Ticker SYMBOL') or '').strip(),
                    'action': action,
                    'inferred_stage': stage or 'NO_ENTRY',
                    'preentry_score': round(preentry_score, 4),
                    'confirm_score': round(confirm_score, 4),
                    'entry_readiness': round(entry_readiness, 4),
                    'blockers': blockers[:8],
                })
    payload = {
        'generated_at': now_str(),
        'status': 'decision_output_ready' if decision_df is not None and not decision_df.empty else 'decision_output_missing',
        'decision_row_count': int(len(decision_df)) if decision_df is not None else 0,
        'raw_order_count': int(len(raw_orders)),
        'normalized_order_count': int(len(final_orders)),
        'normalization_blocked_count': int(len(normalization_blocked)),
        'tickers': sorted({str(o.get('ticker') or '').upper() for o in final_orders if o.get('ticker')})[:200],
        'action_counts': action_counts,
        'stage_counts': stage_counts,
        'has_entry_stage_signal': bool(stage_counts['PREPARE'] or stage_counts['PILOT'] or stage_counts['FULL']),
        'decision_preview': decision_preview[:50],
        'near_miss_count': int(len(near_miss_preview)),
        'near_miss_preview': near_miss_preview[:30],
        'normalization_blocked_preview': normalization_blocked[:30],
        'strategy_vs_engineering_counts': strategy_vs_engineering,
        'source_candidates': [str(p) for p in [
            PATHS.data_dir / 'normalized_decision_output_enriched.csv',
            PATHS.data_dir / 'normalized_decision_output.csv',
            PATHS.base_dir / 'daily_decision_desk.csv',
            PATHS.data_dir / 'daily_decision_desk.csv',
        ]],
    }
    path = PATHS.runtime_dir / 'decision_output_evidence.json'
    write_json(path, payload)
    near_miss_path = PATHS.runtime_dir / 'entry_near_miss_report.json'
    write_json(near_miss_path, {
        'generated_at': now_str(),
        'status': 'entry_near_miss_report_ready',
        'near_miss_count': int(len(near_miss_preview)),
        'items': near_miss_preview[:100],
    })
    return str(path), payload



def _ensure_decision_desk_ready() -> tuple[str, dict[str, Any]]:
    if not _safe_bool(getattr(__import__("config"), "PARAMS", {}).get('CONTROL_TOWER_BUILD_DECISION_DESK', True), True):
        return '', {'status': 'decision_desk_builder_skipped'}
    try:
        return DecisionDeskBuilder().build_summary()
    except Exception as exc:
        record_issue('control_tower', 'decision_desk_builder_failed', exc, severity='ERROR', fail_mode='fail_open')
        return '', {'status': 'decision_desk_builder_error', 'error': repr(exc)}


def _load_decision_df() -> pd.DataFrame:
    _ensure_decision_desk_ready()
    candidates = [
        PATHS.data_dir / 'normalized_decision_output_enriched.csv',
        PATHS.data_dir / 'normalized_decision_output.csv',
        PATHS.base_dir / 'daily_decision_desk.csv',
        PATHS.data_dir / 'daily_decision_desk.csv',
    ]
    if _safe_bool(getattr(__import__("config"), "PARAMS", {}).get('CONTROL_TOWER_LOAD_PRERISK_IF_DECISION_EMPTY', True), True):
        candidates.extend([PATHS.base_dir / 'daily_decision_desk_prerisk.csv', PATHS.data_dir / 'daily_decision_desk_prerisk.csv'])
    for candidate in candidates:
        if candidate.exists():
            try:
                df = pd.read_csv(candidate)
                if df is not None and not df.empty:
                    return df
            except Exception as exc:  # runtime diagnostics
                record_issue('control_tower', 'read_decision_csv_candidate_failed', exc, severity='WARNING', fail_mode='fail_open')
                continue
    return pd.DataFrame()


def _normalize_orders(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if df is None or df.empty:
        return [], []
    orders: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for _, row in df.head(120).iterrows():
        ticker = str(row.get('Ticker') or row.get('ticker') or row.get('Ticker SYMBOL') or '').strip()
        if not ticker:
            continue
        explicit_action = str(row.get('Action') or row.get('Decision') or row.get('Signal') or '').strip().upper()
        direction = str(row.get('Direction') or row.get('direction') or '').strip()
        stage = _infer_entry_stage(row)
        exit_state = str(row.get('Exit_State') or '').strip().upper()
        exit_action = str(row.get('Exit_Action') or '').strip().upper()
        if explicit_action in {'SELL', 'EXIT'} or exit_state == 'EXIT':
            action = 'SELL'
        elif explicit_action == 'REDUCE' or exit_action == 'REDUCE':
            action = 'SELL'
        elif explicit_action == 'COVER':
            action = 'COVER'
        elif explicit_action == 'SHORT':
            action = 'SHORT'
        elif explicit_action in {'BUY', 'LONG'}:
            action = 'BUY'
        elif stage in {'PILOT_ENTRY', 'FULL_ENTRY'}:
            action = 'SHORT' if ('空' in direction or 'SHORT' in direction.upper()) else 'BUY'
        else:
            blocked.append({
                'ticker': ticker,
                'action': 'HOLD',
                'stage': stage or 'NO_ENTRY',
                'preentry_score': round(_safe_float(row.get('PreEntry_Score', 0.0), 0.0), 4),
                'confirm_score': round(_safe_float(row.get('Confirm_Entry_Score', 0.0), 0.0), 4),
                'entry_readiness': round(_safe_float(row.get('Entry_Readiness', 0.0), 0.0), 4),
                'target_qty': int(max(0, round(_safe_float(row.get('Target_Qty', row.get('TargetQty', 0)), 0.0)))),
                'reference_price': _safe_float(row.get('Reference_Price', row.get('Close', row.get('Current_Close', row.get('ref_price', 0.0)))), 0.0),
                'blockers': ['non_executable_or_action_missing'],
            })
            continue
        active_position_qty = int(max(0, round(_safe_float(row.get('Active_Position_Qty', row.get('Position_Qty', row.get('持倉張數', 0))), 0.0))))
        qty = int(max(0, round(_safe_float(row.get('Target_Qty', row.get('TargetQty', 0)), 0.0))))
        if action == 'SELL' and qty <= 0 and active_position_qty > 0:
            qty = active_position_qty
        if action == 'COVER' and qty <= 0 and active_position_qty > 0:
            qty = active_position_qty
        if action == 'SELL' and explicit_action == 'REDUCE' and active_position_qty > 0:
            qty = max(1, active_position_qty // 2)
        ref_price = _safe_float(row.get('Reference_Price', row.get('Close', row.get('Current_Close', row.get('ref_price', 0.0)))), 0.0)
        blockers = _build_order_blockers({**row.to_dict(), 'Target_Qty': qty, 'Reference_Price': ref_price, 'Active_Position_Qty': active_position_qty}, ticker, action, stage)
        if blockers:
            blocked.append({
                'ticker': ticker,
                'action': action,
                'stage': stage or 'NO_ENTRY',
                'preentry_score': round(_safe_float(row.get('PreEntry_Score', 0.0), 0.0), 4),
                'confirm_score': round(_safe_float(row.get('Confirm_Entry_Score', 0.0), 0.0), 4),
                'entry_readiness': round(_safe_float(row.get('Entry_Readiness', 0.0), 0.0), 4),
                'target_qty': qty,
                'reference_price': ref_price,
                'blockers': blockers,
            })
            continue
        orders.append({
            'ticker': ticker,
            'action': action,
            'target_qty': qty,
            'reference_price': ref_price,
            'qty': qty,
            'ref_price': ref_price,
            'stage': stage or ('EXIT' if action in {'SELL', 'COVER'} else 'UNSPECIFIED_ENTRY'),
            'strategy_name': str(row.get('Structure', row.get('strategy_name', 'default'))),
            'industry': str(row.get('產業類別', row.get('industry', '未知'))),
        })
    return orders, blocked



def _apply_entry_tracking_gate(raw_orders: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        from fts_entry_tracking_service import EntryTrackingService
        path, payload = EntryTrackingService().build()
    except Exception as exc:
        record_issue('control_tower', 'entry_tracking_gate_failed', exc, severity='ERROR', fail_mode='fail_open')
        return raw_orders, {'status': 'entry_tracking_gate_error', 'error': repr(exc), 'input_order_count': len(raw_orders), 'output_order_count': len(raw_orders)}
    actions = payload.get('action_plan', {}).get('actions', []) if isinstance(payload, dict) else []
    blocked_map = {str(a.get('ticker') or '').upper(): a for a in actions if 'BLOCK' in str(a.get('action') or '').upper()}
    out: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for order in raw_orders:
        ticker = str(order.get('ticker') or '').upper()
        stage = str(order.get('stage') or '').upper()
        if ticker in blocked_map and order.get('action') in {'BUY', 'SHORT'}:
            blocked.append({'ticker': ticker, 'action': order.get('action'), 'stage': stage, 'reason': blocked_map[ticker].get('reason', 'entry_tracking_blocked')})
            continue
        if stage == 'PILOT_ENTRY' and int(order.get('qty', 0) or 0) > 1:
            pilot_qty = max(1, int(order.get('qty', 0) or 0) // 3)
            order = {**order, 'qty': pilot_qty, 'target_qty': pilot_qty, 'entry_tracking_qty_capped': True}
        out.append(order)
    gate = {
        'status': 'entry_tracking_gate_ready',
        'path': str(path),
        'input_order_count': len(raw_orders),
        'output_order_count': len(out),
        'blocked_count': len(blocked),
        'blocked_preview': blocked[:30],
        'action_plan_count': len(actions),
    }
    return out, gate


def _apply_position_lifecycle_gate(raw_orders: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        from fts_position_lifecycle_service import PositionLifecycleService
        path, payload = PositionLifecycleService().build()
    except Exception as exc:
        record_issue('control_tower', 'position_lifecycle_gate_failed', exc, severity='ERROR', fail_mode='fail_open')
        return raw_orders, {'status': 'position_lifecycle_gate_error', 'error': repr(exc), 'input_order_count': len(raw_orders), 'output_order_count': len(raw_orders)}
    actions = payload.get('summary', {}) if isinstance(payload, dict) else {}
    lifecycle_actions = []
    try:
        lifecycle_actions = __import__('json').loads((PATHS.runtime_dir / 'position_lifecycle_action_plan.json').read_text(encoding='utf-8')).get('actions', [])
    except Exception:
        lifecycle_actions = []
    exit_map = {str(a.get('ticker') or '').upper(): a for a in lifecycle_actions if str(a.get('action') or '').upper() in {'SELL', 'TIGHTEN_STOP_AND_REPLACE_PROTECTIVE_ORDER'}}
    out: list[dict[str, Any]] = []
    lifecycle_injected: list[dict[str, Any]] = []
    blocked_entries: list[dict[str, Any]] = []
    seen_exit = set()
    for order in raw_orders:
        ticker = str(order.get('ticker') or '').upper()
        lifecycle = exit_map.get(ticker)
        if lifecycle and order.get('action') in {'BUY', 'SHORT'}:
            blocked_entries.append({'ticker': ticker, 'action': order.get('action'), 'reason': lifecycle.get('attribution') or lifecycle.get('action') or 'position_lifecycle_exit_active'})
            continue
        out.append(order)
    for ticker, lifecycle in exit_map.items():
        recommendation = str(lifecycle.get('recommendation') or '').upper()
        if recommendation in {'EXIT', 'REDUCE'} and ticker not in seen_exit:
            qty = int(max(0, round(_safe_float(lifecycle.get('qty', 0), 0.0))))
            ref_price = _safe_float(lifecycle.get('reference_price', 0.0), 0.0)
            if qty > 0 and ref_price > 0:
                lifecycle_injected.append({
                    'ticker': ticker,
                    'action': 'SELL',
                    'target_qty': qty,
                    'reference_price': ref_price,
                    'qty': qty,
                    'ref_price': ref_price,
                    'stage': 'EXIT',
                    'strategy_name': 'position_lifecycle_service',
                    'industry': '未知',
                    'exit_recommendation': recommendation,
                })
                seen_exit.add(ticker)
    out.extend(lifecycle_injected)
    gate = {
        'status': 'position_lifecycle_gate_ready',
        'path': str(path),
        'input_order_count': len(raw_orders),
        'output_order_count': len(out),
        'blocked_entry_count': len(blocked_entries),
        'blocked_entry_preview': blocked_entries[:30],
        'injected_exit_count': len(lifecycle_injected),
        'summary': actions,
    }
    return out, gate


class _AcceptedSignal:
    def __init__(self, order: dict[str, Any]):
        self.ticker = order.get('ticker')
        self.action = order.get('action')
        self.target_qty = int(order.get('target_qty', 0) or 0)
        self.reference_price = float(order.get('reference_price', 0.0) or 0.0)




def _call_builder_result(builder: Any, method_name: str, *args: Any, fallback_path: Path | None = None, **kwargs: Any) -> tuple[str, dict[str, Any]]:
    method = getattr(builder, method_name, None)
    if method is None:
        raise AttributeError(f"{type(builder).__name__} has no method {method_name}")
    result = method(*args, **kwargs)
    if isinstance(result, tuple) and len(result) == 2:
        path, payload = result
        return str(path), payload if isinstance(payload, dict) else {'status': 'ok', 'result': payload}
    payload = result if isinstance(result, dict) else {'status': 'ok', 'result': result}
    out_path = fallback_path or (PATHS.runtime_dir / f'{type(builder).__name__.lower()}_{method_name}.json')
    try:
        write_json(Path(out_path), payload)
    except Exception:
        pass
    return str(out_path), payload


def _safe_build(module_name: str, class_name: str, method_name: str, kwargs: dict[str, Any] | None = None) -> dict[str, Any]:
    kwargs = dict(kwargs or {})
    try:
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        obj = cls()
        path, payload = _call_builder_result(obj, method_name, **kwargs)
        return {'status': 'ok', 'path': str(path), 'payload': payload}
    except Exception as exc:
        record_issue('control_tower', f'safe_build_{module_name}_{class_name}_{method_name}', exc, severity='ERROR', fail_mode='fail_closed')
        out_path = PATHS.runtime_dir / f'{class_name}_{method_name}_error.json'
        payload = {'generated_at': now_str(), 'status': 'error', 'module': module_name, 'class_name': class_name, 'method_name': method_name, 'error_type': type(exc).__name__, 'error': str(exc)}
        try:
            write_json(out_path, payload)
        except Exception:
            pass
        return {'status': 'error', 'path': str(out_path), 'payload': payload}



def _run_param_governance(trigger_stage: str, scopes: list[str] | None = None, run_release_gate: bool = False) -> dict[str, Any]:
    """Run parameter candidate AI judge / mount summary from control tower.

    This stage is non-live by default. It never promotes live unless the release
    gate configuration explicitly allows it.
    """
    try:
        from fts_param_governance_orchestrator import run_param_governance
        payload = run_param_governance(scopes=scopes, run_release_gate=run_release_gate, force_release=False)
        payload.setdefault('trigger_stage', trigger_stage)
        out = PATHS.runtime_dir / 'param_governance_orchestrator.json'
        try:
            write_json(out, payload)
        except Exception:
            pass
        return {'status': 'ok', 'path': str(out), 'payload': payload}
    except Exception as exc:
        record_issue('control_tower', f'param_governance_{trigger_stage}', exc, severity='ERROR', fail_mode='fail_closed')
        payload = {'generated_at': now_str(), 'status': 'error', 'trigger_stage': trigger_stage, 'error': repr(exc)}
        out = PATHS.runtime_dir / 'param_governance_orchestrator.json'
        try:
            write_json(out, payload)
        except Exception:
            pass
        return {'status': 'error', 'path': str(out), 'payload': payload}

def _run_maturity_upgrade_suite(trigger_stage: str) -> tuple[str, dict[str, Any]]:
    try:
        path, payload = _call_builder_result(MaturityUpgradeSuite(), 'build', fallback_path=PATHS.runtime_dir / 'maturity_upgrade_suite.json')
        if isinstance(payload, dict):
            payload.setdefault('trigger_stage', trigger_stage)
            try:
                write_json(Path(path), payload)
            except Exception:
                pass
        return str(path), payload
    except Exception as exc:
        record_issue('control_tower', f'maturity_upgrade_suite_{trigger_stage}', exc, severity='ERROR', fail_mode='fail_closed')
        payload = {'generated_at': now_str(), 'status': 'error', 'trigger_stage': trigger_stage, 'error_type': type(exc).__name__, 'error': str(exc)}
        path = PATHS.runtime_dir / 'maturity_upgrade_suite.json'
        try:
            write_json(path, payload)
        except Exception:
            pass
        return str(path), payload



def _collect_reconciliation_inputs() -> dict[str, Any]:
    """Best-effort collector for reconciliation inputs from runtime artifacts.

    Missing files are treated as empty snapshots so the control tower can
    continue building readiness artifacts without crashing.
    """
    def _load_json_file(path: Path, default: Any) -> Any:
        try:
            if not path.exists():
                return default
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception as exc:
            try:
                record_issue('control_tower', f'reconciliation_input_load_failed_{path.name}', exc, severity='WARNING', fail_mode='fail_open')
            except Exception:
                pass
            return default

    ledger = _load_json_file(PATHS.runtime_dir / 'execution_ledger_summary.json', {})
    broker_snapshot = _load_json_file(PATHS.runtime_dir / 'broker_runtime_snapshot.json', {})
    callback_summary = _load_json_file(PATHS.runtime_dir / 'broker_callback_ingestion_summary.json', {})
    account_snapshot = _load_json_file(PATHS.runtime_dir / 'execution_account_snapshot.json', {})
    twap_state = _load_json_file(PATHS.runtime_dir / 'twap3_child_order_state.json', {})

    local_orders = list(ledger.get('orders', []) or [])
    twap_children = []
    if isinstance(twap_state, dict) and isinstance(twap_state.get('children'), dict):
        twap_children = [dict(x) for x in twap_state.get('children', {}).values() if isinstance(x, dict)]
        for child in twap_children:
            child.setdefault('client_order_id', child.get('child_order_id'))
            child.setdefault('order_id', child.get('child_order_id'))
            child.setdefault('ticker_symbol', child.get('ticker'))
            child.setdefault('execution_style', 'TWAP3')
        local_orders.extend(twap_children)
    broker_orders = list(
        broker_snapshot.get('open_orders')
        or broker_snapshot.get('orders')
        or callback_summary.get('latest_callbacks')
        or []
    )
    local_fills = list(ledger.get('fills', []) or [])
    for child in twap_children:
        try:
            filled_qty = float(child.get('filled_qty') or 0)
        except Exception:
            filled_qty = 0.0
        if filled_qty > 0 or str(child.get('status') or '').upper() in {'FILLED', 'PARTIALLY_FILLED'}:
            local_fills.append({**child, 'fill_id': 'TWAP3-FILL-' + str(child.get('child_order_id')), 'filled_qty': filled_qty, 'fill_qty': filled_qty})
    broker_fills = list(broker_snapshot.get('fills', []) or callback_summary.get('fills', []) or [])
    local_positions = list(ledger.get('positions', []) or [])
    broker_positions = list(broker_snapshot.get('positions', []) or broker_snapshot.get('holdings', []) or [])
    local_cash = ledger.get('cash')
    if local_cash in (None, ''):
        local_cash = account_snapshot.get('cash') or account_snapshot.get('available_cash')
    broker_cash = broker_snapshot.get('cash')
    if broker_cash in (None, ''):
        broker_cash = broker_snapshot.get('available_cash') or broker_snapshot.get('equity_cash')

    return {
        'local_orders': local_orders,
        'broker_orders': broker_orders,
        'local_fills': local_fills,
        'broker_fills': broker_fills,
        'local_positions': local_positions,
        'broker_positions': broker_positions,
        'local_cash': float(local_cash or 0),
        'broker_cash': float(broker_cash or 0),
        'sources': {
            'execution_ledger_summary': str(PATHS.runtime_dir / 'execution_ledger_summary.json'),
            'broker_runtime_snapshot': str(PATHS.runtime_dir / 'broker_runtime_snapshot.json'),
            'broker_callback_ingestion_summary': str(PATHS.runtime_dir / 'broker_callback_ingestion_summary.json'),
            'execution_account_snapshot': str(PATHS.runtime_dir / 'execution_account_snapshot.json'),
        },
    }

def _build_control_outputs() -> dict[str, Any]:
    health_path, health_payload = ProjectHealthcheck(PATHS.base_dir).build_report(deep=False)
    level2_path, level2_payload = run_level2_mainline()
    maturity_path, maturity_payload = _run_maturity_upgrade_suite('daily_control_outputs_pre_gate')
    decision_df = _load_decision_df()
    raw_orders, normalization_blocked = _normalize_orders(decision_df)
    orders_after_entry_gate, entry_tracking_gate = _apply_entry_tracking_gate(raw_orders)
    orders, position_lifecycle_gate = _apply_position_lifecycle_gate(orders_after_entry_gate)
    decision_evidence_path, decision_evidence_payload = _write_decision_output_evidence(decision_df, raw_orders, orders, normalization_blocked=normalization_blocked)
    decision_execution_gate_path = _write_json('decision_execution_formal_gate.json', {'generated_at': now_str(), 'status': 'decision_execution_gate_ready', 'raw_order_count': len(raw_orders), 'final_order_count': len(orders), 'normalization_blocked_count': len(normalization_blocked), 'normalization_blocked_preview': normalization_blocked[:30], 'entry_tracking_gate': entry_tracking_gate, 'position_lifecycle_gate': position_lifecycle_gate, 'decision_output_evidence_path': decision_evidence_path})
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
    twap3_runtime_closure = _safe_build('fts_twap3_runtime_closure', 'TWAP3RuntimeClosure', 'build')
    shadow_runtime_evidence = _safe_build('fts_shadow_runtime_evidence', 'ShadowRuntimeEvidenceBuilder', 'build')
    param_governance = _run_param_governance('daily_control_outputs', scopes=['strategy_signal::default', 'execution_policy::default'], run_release_gate=True)
    nonbroker_runtime_closure_gate = _safe_build('fts_nonbroker_runtime_closure_gate', 'NonBrokerRuntimeClosureGate', 'build')
    true_broker_closure_path, true_broker_closure_payload = _call_builder_result(TrueBrokerLiveClosureService(), 'build', fallback_path=PATHS.runtime_dir / 'true_broker_live_closure.json')
    real_api_path, real_api_payload = _call_builder_result(RealAPIReadinessBuilder(), 'build', fallback_path=PATHS.runtime_dir / 'real_api_readiness.json')
    true_broker_path, true_broker_payload = _call_builder_result(TrueBrokerReadinessGate(), 'build', fallback_path=PATHS.runtime_dir / 'true_broker_readiness_gate.json')
    if isinstance(recon_payload, dict):
        recon_payload.setdefault('input_sources', recon_inputs.get('sources', {}))
        recon_payload.setdefault('input_counts', {
            'local_orders': len(recon_inputs['local_orders']), 'broker_orders': len(recon_inputs['broker_orders']),
            'local_fills': len(recon_inputs['local_fills']), 'broker_fills': len(recon_inputs['broker_fills']),
            'local_positions': len(recon_inputs['local_positions']), 'broker_positions': len(recon_inputs['broker_positions']),
        })
        write_json(Path(recon_path), recon_payload)
    recovery_service = RestartRecoveryService()
    recovery_path, recovery_payload = recovery_service.build_plan(require_broker_snapshot=False)
    recovery_validation_path, recovery_validation_payload = _call_builder_result(RecoveryValidationBuilder(), 'build', {'total': 0}, recovery_payload if isinstance(recovery_payload, dict) else {}, fallback_path=PATHS.runtime_dir / 'recovery_validation.json')
    latest_approval = OperatorApprovalRegistry().latest_for('live_release')
    live_release_path, live_release_payload = _call_builder_result(
        LiveReleaseGate(), 'evaluate', governance=governance_payload, safety=live_safety_payload, recon=recon_payload if isinstance(recon_payload, dict) else {},
        recovery=recovery_validation_payload, approval=latest_approval, broker_contract={'defined': True}, true_broker=true_broker_payload if isinstance(true_broker_payload, dict) else {}, fallback_path=PATHS.runtime_dir / 'live_release_gate.json'
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
        'true_broker_live_closure': {'path': str(true_broker_closure_path), 'payload': true_broker_closure_payload},
        'real_api_readiness': {'path': str(real_api_path), 'payload': real_api_payload},
        'true_broker_readiness_gate': {'path': str(true_broker_path), 'payload': true_broker_payload},
        'reconciliation': {'path': str(recon_path), 'payload': recon_payload},
        'twap3_runtime_closure': twap3_runtime_closure,
        'shadow_runtime_evidence': shadow_runtime_evidence,
        'param_governance': param_governance,
        'nonbroker_runtime_closure_gate': nonbroker_runtime_closure_gate,
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
        pg = _run_param_governance('train_pre_data_builder', scopes=['trainer::default', 'label_policy::default'], run_release_gate=False)
        steps.append({'stage': 'param_governance_pre_train', 'status': pg.get('status'), 'path': pg.get('path', ''), 'payload_status': (pg.get('payload') or {}).get('status')})
    except Exception as exc:
        record_issue('control_tower', 'train_param_governance_failed', exc, severity='ERROR', fail_mode='fail_closed')
        steps.append({'stage': 'param_governance_pre_train', 'status': 'error', 'error': repr(exc)})
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
        _call_admin_command('full-market-percentile', allow_missing=False),
        _call_admin_command('event-calendar-build', allow_missing=False),
        _call_admin_command('sync-feature-snapshots', allow_missing=False),
        _call_admin_command('broker-contract-audit', allow_missing=False),
        _call_admin_command('callback-ingest', allow_missing=False),
        _call_admin_command('reconciliation-runtime', allow_missing=False),
        _call_admin_command('twap3-runtime', allow_missing=True),
        _call_admin_command('shadow-evidence', allow_missing=True),
        _call_admin_command('runtime-closure', allow_missing=True),
        _call_admin_command('restart-recovery', allow_missing=False),
        _call_admin_command('prebroker-95-audit', allow_missing=False),
        _call_admin_command('maturity-upgrade', allow_missing=True),
        _call_admin_command('patch-retirement-report', allow_missing=True),
    ]
    daily_payload = run_daily()
    hard_failed = [x for x in steps if str(x.get('status')) == 'error']
    unsupported = [x for x in steps if str(x.get('status')) == 'unsupported']
    bootstrap_status = 'bootstrap_ready' if not hard_failed else 'bootstrap_partial'
    payload = {
        'generated_at': now_str(),
        'mode': 'bootstrap',
        'module_version': 'v20260416_bootstrap_full_repair',
        'steps': steps,
        'daily_status': daily_payload.get('status'),
        'daily_readiness_split': daily_payload.get('readiness_split', {}),
        'hard_failed_count': len(hard_failed),
        'unsupported_step_count': len(unsupported),
        'hard_failed_steps': hard_failed[:20],
        'unsupported_steps': unsupported[:20],
        'status': bootstrap_status,
    }
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
        return 0 if payload.get('status') in {'control_tower_ready', 'train_ready', 'bootstrap_ready', 'bootstrap_partial'} else 1
    except Exception as exc:  # runtime diagnostics
        record_issue('control_tower', 'main_failure', exc, severity='CRITICAL', fail_mode='fail_closed')
        payload = {'generated_at': now_str(), 'module_version': 'v83_level3_control_tower_integrated', 'error_type': type(exc).__name__, 'error': str(exc)}
        _write_json('formal_trading_system_v83_official_main_error.json', payload)
        log(f'❌ control tower failure: {exc}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
