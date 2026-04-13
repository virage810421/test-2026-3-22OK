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

from fts_config import PATHS, CONFIG
from fts_utils import log, now_str
from fts_project_healthcheck import ProjectHealthcheck
from fts_pipeline import run_level2_mainline
from fts_live_readiness_gate import LiveReadinessGate
from fts_live_safety import LiveSafetyGate
from fts_broker_approval import BrokerApprovalGate
from fts_submission_gate import SubmissionContractGate
from fts_validation_suite import ValidationSuiteBuilder
from fts_model_gate import ModelVersionRegistry, ModelSelectionGate
from fts_gatekeeper import LaunchGatekeeper
from fts_live_release_gate import LiveReleaseGate
from fts_operator_approval import OperatorApprovalRegistry
from fts_recovery_engine import RecoveryEngine
from fts_recovery_validation import RecoveryValidationBuilder
from fts_reconciliation_engine import ReconciliationEngine
from fts_tri_lane_orchestrator import TriLaneOrchestrator
from fts_compat import DecisionCompatibilityLayer, apply_decision_integrity_flags
from fts_prelive_runtime import write_json

RUNTIME_DIR = PATHS.runtime_dir


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
    except Exception as exc:
        return {'status': 'error', 'error': repr(exc), 'module': module_name, 'class_name': class_name, 'method': method_name}


def _load_decision_df() -> pd.DataFrame:
    compat = DecisionCompatibilityLayer()
    candidates = [
        PATHS.data_dir / 'normalized_decision_output_enriched.csv',
        PATHS.data_dir / 'normalized_decision_output.csv',
        PATHS.base_dir / 'daily_decision_desk.csv',
        PATHS.data_dir / 'daily_decision_desk.csv',
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            if candidate.name.startswith('normalized_decision_output'):
                df = pd.read_csv(candidate)
                df, _ = apply_decision_integrity_flags(df)
            else:
                df, _ = compat.normalize(candidate)
            return df
        except Exception:
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
    level2_path, level2_payload = run_level2_mainline(execute_legacy=bool(getattr(CONFIG, 'execute_legacy_pipeline', False)))
    decision_df = _load_decision_df()
    orders = _normalize_orders(decision_df)
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
    recon_path, recon_payload = _call_builder_result(ReconciliationEngine(), 'reconcile', [], [], [], [], [], [], CONFIG.starting_cash, CONFIG.starting_cash, fallback_path=PATHS.runtime_dir / 'reconciliation_engine.json')
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
    outputs = _build_control_outputs()
    payload = {'generated_at': now_str(), 'mode': 'daily', 'module_version': 'v86_level3_control_tower_split_ready', 'outputs': outputs, 'readiness_split': outputs.get('live_readiness_gate', {}).get('payload', {}).get('score_split', {}), 'operational_scope': {'prelive': outputs.get('live_readiness_gate', {}).get('payload', {}).get('prelive_ready', False), 'broker_production': outputs.get('live_readiness_gate', {}).get('payload', {}).get('broker_production_ready', False)}, 'status': 'control_tower_ready'}
    _write_json('formal_trading_system_v83_official_main.json', payload)
    return payload


def run_train() -> dict[str, Any]:
    log('=' * 72)
    log('🚀 啟動 正式交易主控版_v83_official_main')
    log('🧭 模式：TRAIN')
    log('=' * 72)
    steps = [_call_script('ml_data_generator.py', allow_missing=True), _call_script('ml_trainer.py', allow_missing=True)]
    tri_lane_path, tri_lane_payload = _call_builder_result(TriLaneOrchestrator(), 'build', fallback_path=PATHS.runtime_dir / 'tri_lane_orchestrator.json')
    payload = {'generated_at': now_str(), 'mode': 'train', 'module_version': 'v83_level3_control_tower_integrated', 'outputs': {'steps': steps, 'tri_lane_orchestration': {'path': str(tri_lane_path), 'payload': tri_lane_payload}}, 'status': 'train_ready'}
    _write_json('formal_trading_system_v83_train.json', payload)
    _write_json('training_orchestrator.json', {'generated_at': now_str(), 'status': 'train_invoked_via_control_tower'})
    return payload


def run_bootstrap() -> dict[str, Any]:
    log('=' * 72)
    log('🚀 啟動 正式交易主控版_v83_official_main')
    log('🧭 模式：BOOTSTRAP')
    log('=' * 72)
    steps = [
        _call_script('db_setup.py', ['--mode', 'upgrade'], allow_missing=False),
        _call_script('db_setup_research_plus.py', allow_missing=True),
        _call_script('run_full_market_percentile_snapshot.py', allow_missing=True),
        _call_script('run_precise_event_calendar_build.py', allow_missing=True),
        _call_script('run_sync_feature_snapshots_to_sql.py', allow_missing=True),
    ]
    daily_payload = run_daily()
    payload = {'generated_at': now_str(), 'mode': 'bootstrap', 'module_version': 'v86_level3_control_tower_split_ready', 'steps': steps, 'daily_status': daily_payload.get('status'), 'daily_readiness_split': daily_payload.get('readiness_split', {}), 'status': 'bootstrap_ready'}
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
    except Exception as exc:
        payload = {'generated_at': now_str(), 'module_version': 'v83_level3_control_tower_integrated', 'error_type': type(exc).__name__, 'error': str(exc)}
        _write_json('formal_trading_system_v83_official_main_error.json', payload)
        log(f'❌ control tower failure: {exc}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
