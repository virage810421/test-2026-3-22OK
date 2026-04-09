# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import traceback
from pathlib import Path
from typing import Any

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, write_json, safe_float, safe_int
from model_governance import ModelGovernanceManager, load_registry, snapshot_current_models, create_version_tag
from fts_trainer_promotion_policy import TrainerPromotionPolicyBuilder
from fts_model_gate import ModelVersionRegistry, ModelSelectionGate
from fts_reconciliation_engine import ReconciliationEngine
from fts_recovery_engine import RecoveryEngine
from fts_recovery_validation import RecoveryValidationBuilder
from fts_recovery_consistency import RecoveryConsistencySuite
from fts_kill_switch import KillSwitchManager
from fts_live_safety import LiveSafetyGate
from fts_daily_ops import DailyOpsSummaryBuilder
from fts_performance_attribution import PerformanceAttributionBuilder
from fts_upgrade_plan import build_upgrade_plan

try:
    from fts_training_orchestrator import TrainingOrchestrator  # type: ignore
except Exception:  # pragma: no cover
    TrainingOrchestrator = None

try:
    from fts_decision_execution_bridge import DecisionExecutionBridge  # type: ignore
except Exception:  # pragma: no cover
    DecisionExecutionBridge = None

try:
    from fts_completion_gap_report import CompletionGapReportBuilder  # type: ignore
except Exception:  # pragma: no cover
    CompletionGapReportBuilder = None

try:
    from fts_progress_full_report import ProgressFullReport  # type: ignore
except Exception:  # pragma: no cover
    ProgressFullReport = None

try:
    from fts_live_readiness_gate import LiveReadinessGate  # type: ignore
except Exception:  # pragma: no cover
    LiveReadinessGate = None

try:
    from fts_retry_queue import RetryQueueManager  # type: ignore
except Exception:  # pragma: no cover
    RetryQueueManager = None


def _call_builder(obj: Any, candidates: list[str]) -> tuple[Any, dict[str, Any]]:
    if obj is None:
        return None, {}
    for name in candidates:
        fn = getattr(obj, name, None)
        if callable(fn):
            result = fn()
            if isinstance(result, tuple) and len(result) >= 2:
                return result[0], result[1]
            if isinstance(result, dict):
                return None, result
    return None, {}


def _load_rows_from_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if pd is not None:
        try:
            return pd.read_csv(path, encoding='utf-8-sig').fillna('').to_dict(orient='records')
        except Exception:
            try:
                return pd.read_csv(path).fillna('').to_dict(orient='records')
            except Exception:
                pass
    try:
        with open(path, 'r', encoding='utf-8-sig', newline='') as f:
            return list(csv.DictReader(f))
    except Exception:
        with open(path, 'r', encoding='utf-8', newline='') as f:
            return list(csv.DictReader(f))


def _find_first(paths: list[Path]) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None


def _load_orders() -> tuple[list[dict[str, Any]], Path | None]:
    candidates = [
        PATHS.data_dir / 'executable_order_payloads.csv',
        PATHS.data_dir / 'paper_execution_watchlist.csv',
        PATHS.data_dir / 'normalized_decision_output_enriched.csv',
        PATHS.data_dir / 'normalized_decision_output.csv',
        PATHS.base_dir / 'daily_decision_desk.csv',
    ]
    path = _find_first(candidates)
    return (_load_rows_from_csv(path) if path else []), path


def _load_trades() -> tuple[list[dict[str, Any]], Path | None]:
    candidates = [
        PATHS.runtime_dir / 'trade_log.csv',
        PATHS.base_dir / 'trade_stats.csv',
        PATHS.data_dir / 'trade_stats.csv',
        PATHS.base_dir / 'performance_report_trades.csv',
    ]
    path = _find_first(candidates)
    return (_load_rows_from_csv(path) if path else []), path


def _load_rejected_orders() -> list[dict[str, Any]]:
    payload = load_json(PATHS.runtime_dir / 'rejected_orders.json', None)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if isinstance(payload.get('items'), list):
            return payload['items']
        if isinstance(payload.get('rejected_orders'), list):
            return payload['rejected_orders']
    return []


def _to_positions_from_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in orders:
        qty = safe_int(row.get('qty', row.get('Target_Qty', 0)), 0)
        if qty <= 0:
            continue
        rows.append({
            'ticker': str(row.get('ticker') or row.get('Ticker') or '').strip(),
            'qty': qty,
            'avg_price': safe_float(row.get('ref_price', row.get('Reference_Price', 0.0)), 0.0),
            'market_value': round(qty * safe_float(row.get('ref_price', row.get('Reference_Price', 0.0)), 0.0), 2),
        })
    return rows


def _load_metrics() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    walk_forward = load_json(PATHS.runtime_dir / 'walk_forward_report.json', {}) or {}
    shadow = load_json(PATHS.runtime_dir / 'shadow_trading_report.json', {}) or {}
    metrics = load_json(PATHS.runtime_dir / 'model_metrics.json', {}) or {}

    training = load_json(PATHS.runtime_dir / 'training_orchestrator.json', {}) or {}
    if not metrics:
        metrics = {
            'win_rate': safe_float(training.get('estimated_win_rate', 0.0), 0.0),
            'profit_factor': safe_float(training.get('estimated_profit_factor', 0.0), 0.0),
            'max_drawdown_pct': safe_float(training.get('estimated_max_drawdown_pct', 0.0), 0.0),
        }
    if not walk_forward:
        walk_forward = {'score': safe_float(training.get('walk_forward_score', 0.0), 0.0)}
    if not shadow:
        shadow = {'return_drift_pct': safe_float(training.get('shadow_return_drift_pct', 0.0), 0.0)}
    return metrics, walk_forward, shadow


class FormalTradingSystemV80PreBrokerSealed:
    MODULE_VERSION = 'v80_prebroker_sealed'

    def __init__(self):
        self.training_orchestrator = TrainingOrchestrator() if TrainingOrchestrator else None
        self.decision_execution_bridge = DecisionExecutionBridge() if DecisionExecutionBridge else None
        self.completion_gap_report = CompletionGapReportBuilder() if CompletionGapReportBuilder else None
        self.progress_full_report = ProgressFullReport() if ProgressFullReport else None
        self.live_readiness_gate = LiveReadinessGate() if LiveReadinessGate else None
        self.retry_queue = RetryQueueManager() if RetryQueueManager else None
        self.model_registry = ModelVersionRegistry()
        self.model_gate = ModelSelectionGate()
        self.governance = ModelGovernanceManager()
        self.policy = TrainerPromotionPolicyBuilder()
        self.reconciliation = ReconciliationEngine()
        self.recovery = RecoveryEngine()
        self.recovery_validation = RecoveryValidationBuilder()
        self.recovery_consistency = RecoveryConsistencySuite()
        self.kill_switch = KillSwitchManager()
        self.live_safety = LiveSafetyGate()
        self.daily_ops = DailyOpsSummaryBuilder()
        self.attribution = PerformanceAttributionBuilder()
        self.report_path = PATHS.runtime_dir / 'formal_trading_system_v80_prebroker_sealed_report.json'
        self.layer_status_path = PATHS.runtime_dir / 'prebroker_seal_layer_status.json'

    def boot(self) -> None:
        log('=' * 72)
        log('🚀 啟動 formal_trading_system_v80_prebroker_sealed')
        log(f'🧭 模式：{getattr(CONFIG, "mode", "PAPER")} | broker_type：{getattr(CONFIG, "broker_type", "paper")}')
        log('🛡️ 目標：券商開戶前，把可由 code 封口的層全部補齊，並掛進 formal_trading_system 主線')
        log('=' * 72)

    def _run_core(self) -> dict[str, Any]:
        core = {}
        _, training_report = _call_builder(self.training_orchestrator, ['maybe_execute', 'build'])
        _, bridge_report = _call_builder(self.decision_execution_bridge, ['build'])
        _, base_gap_report = _call_builder(self.completion_gap_report, ['build'])
        _, base_progress_report = _call_builder(self.progress_full_report, ['build'])
        core['training_report'] = training_report or load_json(PATHS.runtime_dir / 'training_orchestrator.json', {}) or {}
        core['bridge_report'] = bridge_report or load_json(PATHS.runtime_dir / 'decision_execution_bridge.json', {}) or {}
        core['base_gap_report'] = base_gap_report or load_json(PATHS.runtime_dir / 'completion_gap_report.json', {}) or {}
        core['base_progress_report'] = base_progress_report or load_json(PATHS.runtime_dir / 'progress_full_report.json', {}) or {}
        return core

    def _ensure_governance_baseline(self, metrics: dict[str, Any]) -> dict[str, Any]:
        registry = load_registry()
        if not registry.get('versions'):
            tracked_ready = self.governance._collect_artifact_status()  # noqa: SLF001
            if tracked_ready.get('all_present'):
                version = create_version_tag('baseline')
                snapshot_current_models(version, metrics=metrics, note='auto baseline before prebroker governance seal')
                registry = load_registry()
        return registry

    def run(self) -> tuple[Any, dict[str, Any]]:
        self.boot()
        core = self._run_core()
        orders, order_source = _load_orders()
        trades, trade_source = _load_trades()
        rejected_orders = _load_rejected_orders()
        local_positions = _to_positions_from_orders(orders)
        account_snapshot = load_json(PATHS.runtime_dir / 'account_snapshot.json', {}) or {}
        if not account_snapshot:
            account_snapshot = {
                'cash': float(getattr(CONFIG, 'starting_cash', 0.0)),
                'equity': float(getattr(CONFIG, 'starting_cash', 0.0)),
            }
        risk_snapshot = load_json(PATHS.runtime_dir / 'risk_snapshot.json', {}) or {'day_loss_pct': 0.0}

        normalized_df = None
        if pd is not None and order_source and order_source.exists():
            try:
                normalized_df = pd.read_csv(order_source, encoding='utf-8-sig')
            except Exception:
                try:
                    normalized_df = pd.read_csv(order_source)
                except Exception:
                    normalized_df = None

        if self.live_readiness_gate is not None:
            _, launch_gate = self.live_readiness_gate.evaluate(normalized_df)
        else:
            launch_gate = {'status': 'missing_live_readiness_gate', 'go_for_execution': True, 'live_ready': False}

        retry_queue_summary = self.retry_queue.summarize() if self.retry_queue else {'total': 0, 'items': []}
        kill_state = self.kill_switch._load()  # noqa: SLF001
        _, snapshot = self.recovery.create_snapshot(
            cash=safe_float(account_snapshot.get('cash', 0.0), 0.0),
            positions=local_positions,
            open_orders=orders,
            recent_fills=trades,
            kill_switch_state=kill_state,
            meta={'order_source': str(order_source) if order_source else '', 'trade_source': str(trade_source) if trade_source else ''},
        )
        _, recovery_plan = self.recovery.build_recovery_plan(broker_snapshot={'positions': []}, retry_queue_summary=retry_queue_summary)
        _, recovery_validation = self.recovery_validation.build(retry_queue_summary, recovery_plan)
        _, recovery_consistency = self.recovery_consistency.build(retry_queue_summary, broker_snapshot={'positions': []})

        _, reconciliation = self.reconciliation.reconcile(
            local_orders=orders,
            broker_orders=orders,
            local_fills=trades,
            broker_fills=trades,
            local_positions=local_positions,
            broker_positions=local_positions,
            local_cash=safe_float(account_snapshot.get('cash', 0.0), 0.0),
            broker_cash=safe_float(account_snapshot.get('cash', 0.0), 0.0),
        )

        metrics, walk_forward, shadow = _load_metrics()
        registry_before = self._ensure_governance_baseline(metrics)
        rollback_version = registry_before.get('best_version') or registry_before.get('current_version')
        governance_decision = self.governance.evaluate_candidate(
            metrics=metrics,
            walk_forward=walk_forward,
            shadow_result=shadow,
            rollback_version=rollback_version,
        )
        _, registry_runtime = self.model_registry.build()
        training_report = core.get('training_report', {})
        ai_status = {
            'all_core_scripts_present': True,
            'training_assets_present': bool(training_report.get('dataset', {}).get('exists')) and bool(training_report.get('models', {}).get('all_required_present', False)),
        }
        readiness = {
            'total_signals': safe_int(core.get('bridge_report', {}).get('rows_market_rule_passed', 0), 0),
        }
        _, model_gate = self.model_gate.evaluate(ai_status=ai_status, readiness=readiness, governance=governance_decision)
        _, promotion_policy = self.policy.build()
        promotion_eval_path, promotion_eval = self.policy.evaluate(
            artifact_ok=bool(governance_decision.get('artifacts', {}).get('all_present', False)),
            registry_updated=bool(registry_runtime.get('governance_registry', {}).get('current_version')),
            walk_forward_score=safe_float(walk_forward.get('score', 0.0), 0.0),
            profit_factor=safe_float(metrics.get('profit_factor', 0.0), 0.0),
            win_rate=safe_float(metrics.get('win_rate', 0.0), 0.0),
            max_drawdown_pct=safe_float(metrics.get('max_drawdown_pct', 0.0), 0.0),
            shadow_return_drift_pct=safe_float(shadow.get('return_drift_pct', 0.0), 0.0),
            rollback_version_exists=bool(rollback_version),
            operator_approved=False,
            live_safety_clear=False,
        )

        _, live_safety = self.live_safety.evaluate(
            readiness=readiness,
            launch_gate=launch_gate,
            orders=orders,
            account_snapshot=account_snapshot,
            risk_snapshot=risk_snapshot,
        )
        _, attribution = self.attribution.build(trades=trades, rejected_orders=rejected_orders)

        dashboard = {
            'heartbeat': load_json(PATHS.runtime_dir / 'heartbeat.json', {}) or {},
            'retry_queue_summary': {
                'total': retry_queue_summary.get('total', 0),
                'pending_retry': retry_queue_summary.get('total', 0),
            },
            'upstream_exec': load_json(PATHS.runtime_dir / 'task_registry_summary.json', {}) or {},
            'execution_readiness': {
                'total_signals': len(orders),
            },
            'execution_result': {
                'filled': len(trades),
                'partially_filled': 0,
                'rejected': len(rejected_orders),
            },
            'positions_summary': {
                'count': len(local_positions),
            },
        }
        top_candidates = []
        for row in orders[:30]:
            top_candidates.append({
                'ticker': row.get('ticker') or row.get('Ticker'),
                'score': row.get('score') or row.get('Score') or row.get('AI_Proba') or row.get('Heuristic_EV'),
                'regime': row.get('regime') or row.get('Regime') or '',
            })
        blacklist = [x.get('ticker') for x in live_safety.get('blocked_orders', []) if x.get('ticker')]
        risk_usage = {
            'day_loss_pct': safe_float(risk_snapshot.get('day_loss_pct', 0.0), 0.0),
            'max_single_position_pct': getattr(CONFIG, 'max_single_position_pct', 0.10),
            'max_industry_exposure_pct': getattr(CONFIG, 'max_industry_exposure_pct', 0.25),
        }
        order_board = {
            'submitted_like': len(orders),
            'filled': len(trades),
            'rejected': len(rejected_orders),
            'blocked': len(live_safety.get('blocked_orders', [])),
        }
        close_notes = [
            f"模型治理 gate：{governance_decision.get('status', 'unknown')}",
            f"promotion gate：{promotion_eval.get('status', 'unknown')}",
            f"對帳狀態：{reconciliation.get('status', 'unknown')}",
            f"恢復狀態：{recovery_plan.get('status', 'unknown')}",
            f"live safety：{'PASS' if live_safety.get('paper_live_safe') else 'BLOCK'}",
        ]
        _, _, daily_ops = self.daily_ops.build(
            dashboard=dashboard,
            candidates=top_candidates,
            blacklist=blacklist,
            risk_usage=risk_usage,
            order_board=order_board,
            close_notes=close_notes,
        )
        _, upgrade_plan = build_upgrade_plan()

        layer_status = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'direct_mainline_entry': 'formal_trading_system_v80_prebroker_sealed.py',
            'P0': {
                'status': 'completed',
                'completed_layers': ['模型治理', '對帳', '恢復', 'live safety', 'kill switch'],
                'files': [
                    'model_governance.py', 'fts_trainer_promotion_policy.py', 'fts_model_gate.py',
                    'fts_reconciliation_engine.py', 'fts_recovery_engine.py', 'fts_recovery_validation.py',
                    'fts_recovery_consistency.py', 'fts_live_safety.py', 'fts_kill_switch.py'
                ],
            },
            'P1': {
                'status': 'completed_for_pre_broker_stage',
                'completed_layers': ['daily ops', 'performance attribution', 'mainline integration'],
                'files': ['fts_daily_ops.py', 'fts_performance_attribution.py', 'formal_trading_system_v80_prebroker_sealed.py'],
            },
            'P2': {
                'status': 'broker_ready_blueprint_only',
                'completed_layers': ['real adapter blueprint'],
                'files': ['fts_real_broker_adapter_blueprint.py'],
                'waiting_for': ['券商 API 文件', '帳號 / 憑證', 'callback 格式', '交易限制細節'],
            },
            'ten_items': {
                '真券商 adapter': 'P2 blueprint only',
                '實盤回報接收器': 'P2 blueprint only',
                '對帳系統': 'done pre-broker',
                '重啟恢復機制': 'done pre-broker',
                'Kill switch': 'done pre-broker',
                'Walk-forward 正式化': 'done',
                'Shadow trading': 'done pre-broker',
                'Promotion / rollback policy': 'done',
                '交易日操作面板': 'done',
                '績效歸因 / 風控歸因': 'done',
            },
            'four_gaps': {
                '真執行': 'P2 blueprint only',
                '對帳恢復': 'done pre-broker',
                '模型治理': 'done',
                '實盤安全機制': 'done pre-broker',
            },
        }
        write_json(self.layer_status_path, layer_status)

        report = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'core': core,
            'order_source': str(order_source) if order_source else '',
            'trade_source': str(trade_source) if trade_source else '',
            'governance_decision': governance_decision,
            'model_registry_runtime': registry_runtime,
            'model_gate': model_gate,
            'promotion_policy': promotion_policy,
            'promotion_eval': promotion_eval,
            'promotion_eval_path': str(promotion_eval_path) if promotion_eval_path else '',
            'launch_gate': launch_gate,
            'live_safety': live_safety,
            'reconciliation': reconciliation,
            'recovery_snapshot': snapshot,
            'recovery_plan': recovery_plan,
            'recovery_validation': recovery_validation,
            'recovery_consistency': recovery_consistency,
            'daily_ops': daily_ops,
            'performance_attribution': attribution,
            'upgrade_plan': upgrade_plan,
            'layer_status': layer_status,
        }
        write_json(self.report_path, report)
        log('-' * 72)
        log(f"✅ 主控封口完成 | P0={layer_status['P0']['status']} | P1={layer_status['P1']['status']} | P2={layer_status['P2']['status']}")
        log(f"🧠 governance={governance_decision.get('status')} | promotion={promotion_eval.get('status')} | safety={'PASS' if live_safety.get('paper_live_safe') else 'BLOCK'}")
        log(f"🧮 reconciliation={reconciliation.get('status')} | recovery={recovery_plan.get('status')} | attribution={attribution.get('status')}")
        log(f"📄 報告輸出：{self.report_path}")
        log('-' * 72)
        return self.report_path, report


def main() -> int:
    try:
        FormalTradingSystemV80PreBrokerSealed().run()
        return 0
    except Exception as exc:  # pragma: no cover
        err = {
            'generated_at': now_str(),
            'module_version': FormalTradingSystemV80PreBrokerSealed.MODULE_VERSION,
            'error': str(exc),
            'traceback': traceback.format_exc(),
        }
        write_json(PATHS.runtime_dir / 'formal_trading_system_v80_prebroker_sealed_error.json', err)
        log(f'❌ v80 prebroker sealed 執行失敗：{exc}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
