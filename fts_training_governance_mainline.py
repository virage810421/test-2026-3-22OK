# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_exception_policy import record_diagnostic
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log
from fts_training_orchestrator import TrainingOrchestrator
from fts_trainer_backend import train_models
from model_governance import ModelGovernanceManager, create_version_tag, get_best_version_entry
from fts_trainer_promotion_policy import TrainerPromotionPolicyBuilder


class TrainingGovernanceMainline:
    MODULE_VERSION = 'v20260417_training_governance_runtime_evidence_closed_loop'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'training_governance_mainline.json'
        self.backend_report_path = PATHS.runtime_dir / 'trainer_backend_report.json'
        self.regime_service_path = PATHS.runtime_dir / 'regime_service.json'
        self.alpha_miner_path = PATHS.runtime_dir / 'alpha_miner_directional.json'

    def _load_json(self, path: Path) -> dict[str, Any]:
        if path.exists():
            try:
                return json.loads(path.read_text(encoding='utf-8'))
            except Exception as exc:
                record_diagnostic('training_governance', 'load_json', exc, severity='warning', fail_closed=False, context={'path': str(path)})
                return {}
        return {}

    def _load_backend_report(self) -> dict[str, Any]:
        if self.backend_report_path.exists():
            try:
                return json.loads(self.backend_report_path.read_text(encoding='utf-8'))
            except Exception as exc:
                record_diagnostic('training_governance', 'load_backend_report', exc, severity='warning', fail_closed=False, context={'path': str(self.backend_report_path)})
                return {}
        return {}

    def _load_runtime_records(self, candidates: list[str]) -> tuple[str | None, list[dict[str, Any]]]:
        for name in candidates:
            path = PATHS.runtime_dir / name
            if not path.exists():
                continue
            try:
                data = json.loads(path.read_text(encoding='utf-8'))
            except Exception as exc:
                record_diagnostic('training_governance', 'read_runtime_json_candidate', exc, severity='warning', fail_closed=False, context={'path': str(path)})
                continue
            if isinstance(data, list):
                rows = [x for x in data if isinstance(x, dict)]
                if rows:
                    return str(path), rows
            if isinstance(data, dict):
                for key in ('fills', 'orders', 'callbacks', 'records', 'rows', 'events'):
                    value = data.get(key)
                    if isinstance(value, list):
                        rows = [x for x in value if isinstance(x, dict)]
                        if rows:
                            return str(path), rows
        return None, []

    @staticmethod
    def _status_value(row: dict[str, Any]) -> str:
        return str(row.get('status') or row.get('order_status') or row.get('event_type') or '').upper()

    def _load_shadow_runtime_evidence(self) -> dict[str, Any]:
        try:
            from fts_shadow_runtime_evidence import ShadowRuntimeEvidenceBuilder
            ShadowRuntimeEvidenceBuilder().build()
        except Exception:
            pass
        try:
            _path, payload = TrainerPromotionPolicyBuilder()._build_shadow_runtime_evidence()
            return payload if isinstance(payload, dict) else {}
        except Exception as exc:
            record_diagnostic('training_governance', 'load_shadow_runtime_evidence', exc, severity='warning', fail_closed=False)
            return {}

    def _load_live_metrics(self) -> dict[str, Any]:
        """
        讀真 runtime，不再使用硬編 win_rate/reject_rate/slippage。
        找不到資料時明確標示 not_available，避免治理報表假成熟。
        """
        fills_source, fills = self._load_runtime_records([
            'execution_fills.json',
            'fills.json',
            'paper_fills.json',
            'broker_fills.json',
            'execution_runtime.json',
            'decision_execution_bridge.json',
            'twap3_child_order_state.json',
            'shadow_runtime_evidence.json',
        ])
        orders_source, orders = self._load_runtime_records([
            'execution_orders.json',
            'orders.json',
            'paper_orders.json',
            'broker_orders.json',
            'execution_runtime.json',
            'decision_execution_bridge.json',
            'twap3_child_order_state.json',
            'shadow_runtime_evidence.json',
        ])
        callbacks_source, callbacks = self._load_runtime_records([
            'execution_broker_callbacks.json',
            'broker_callbacks.json',
            'callbacks.json',
            'execution_runtime.json',
            'twap3_child_order_state.json',
        ])

        closed_trades = [
            r for r in fills
            if any(k in r for k in ('pnl', 'realized_pnl', '淨損益金額', 'profit_pct', 'return_pct'))
        ]

        win_rate = None
        consecutive_losses = None
        if closed_trades:
            wins = 0
            loss_streak = 0
            for row in closed_trades:
                pnl = row.get('pnl', row.get('realized_pnl', row.get('淨損益金額', row.get('profit_pct', row.get('return_pct', 0)))))
                try:
                    pnl_f = float(pnl or 0)
                except Exception as exc:
                    record_diagnostic('training_governance', 'closed_trade_pnl_cast', exc, severity='warning', fail_closed=False, context={'value': str(pnl)[:80]})
                    pnl_f = 0.0
                if pnl_f > 0:
                    wins += 1
                    loss_streak = 0
                else:
                    loss_streak += 1
            win_rate = wins / max(1, len(closed_trades))
            consecutive_losses = loss_streak

        reject_rows = [r for r in (orders + callbacks) if self._status_value(r) in {'REJECTED', 'REJECT', 'ORDER_REJECTED'}]
        submitted_like = [
            r for r in (orders + callbacks)
            if self._status_value(r) in {'SUBMITTED', 'PARTIALLY_FILLED', 'FILLED', 'REJECTED', 'CANCELLED', 'NEW', 'PENDING_SUBMIT'}
        ]
        reject_rate = (len(reject_rows) / len(submitted_like)) if submitted_like else None

        slip_values = []
        for row in fills:
            val = row.get('slippage_bps', row.get('avg_slippage_bps', row.get('slippage')))
            if val is None:
                continue
            try:
                slip_values.append(abs(float(val)))
            except Exception:
                continue
        avg_slippage_bps = (sum(slip_values) / len(slip_values)) if slip_values else None

        ready = any(v is not None for v in (win_rate, reject_rate, avg_slippage_bps)) or bool(fills or orders or callbacks)
        metrics = {
            'live_metrics_ready': bool(ready),
            'live_metrics_source': 'runtime_files' if ready else 'not_available',
            'live_metrics_files': {
                'fills': fills_source,
                'orders': orders_source,
                'callbacks': callbacks_source,
            },
            'win_rate': win_rate,
            'consecutive_losses': consecutive_losses,
            'reject_rate': reject_rate,
            'avg_slippage_bps': avg_slippage_bps,
            'trade_count': len(closed_trades) if closed_trades else len(fills),
            'order_count': len(orders),
            'callback_count': len(callbacks),
        }
        return metrics

    def _evaluate_live_health_truthful(self, manager: ModelGovernanceManager, metrics: dict[str, Any]) -> dict[str, Any]:
        if not metrics.get('live_metrics_ready'):
            return {
                'status': 'live_metrics_unavailable',
                'approved': False,
                'reason': '沒有真實 execution runtime / fills / orders；已禁止使用硬編 live_metrics。',
                'metrics': metrics,
            }
        eval_metrics = {
            'win_rate': float(metrics['win_rate']) if metrics.get('win_rate') is not None else 0.0,
            'consecutive_losses': int(metrics['consecutive_losses']) if metrics.get('consecutive_losses') is not None else 999,
            'reject_rate': float(metrics['reject_rate']) if metrics.get('reject_rate') is not None else 1.0,
            'avg_slippage_bps': float(metrics['avg_slippage_bps']) if metrics.get('avg_slippage_bps') is not None else 999.0,
            'trade_count': int(metrics.get('trade_count') or 0),
        }
        result = manager.evaluate_live_health(eval_metrics)
        if isinstance(result, dict):
            result = dict(result)
            result['metrics'] = metrics
            result['evaluated_metrics'] = eval_metrics
            result['live_metrics_source'] = metrics.get('live_metrics_source')
        return result

    def build_summary(self, execute_backend: bool = False) -> tuple[Path, dict[str, Any]]:
        orchestrator = TrainingOrchestrator().maybe_execute()
        manager = ModelGovernanceManager()

        backend_result: dict[str, Any]
        if execute_backend:
            try:
                path, payload = train_models()
                backend_result = {'executed': True, 'status': payload.get('status'), 'path': str(path), 'report': payload}
            except Exception as exc:
                record_diagnostic('training_governance', 'train_models_backend', exc, severity='error', fail_closed=True)
                backend_result = {'executed': True, 'status': 'backend_failed', 'error': str(exc), 'report': {}}
        else:
            payload = self._load_backend_report()
            backend_result = {'executed': False, 'status': 'summary_only', 'path': str(self.backend_report_path) if payload else None, 'report': payload}

        report = backend_result.get('report', {}) or {}
        training_integrity = manager.evaluate_training_integrity(report or {
            'leakage_guards': {},
            'out_of_time': {},
            'overfit_gap': 1.0,
            'feature_to_sample_ratio': 1.0,
        })

        live_metrics = self._load_live_metrics()
        live_health = self._evaluate_live_health_truthful(manager, live_metrics)
        shadow_runtime = self._load_shadow_runtime_evidence()

        best_entry = get_best_version_entry() or {}
        candidate_eval = manager.evaluate_candidate(
            metrics={
                'win_rate': float(report.get('out_of_time', {}).get('hit_rate', 0.0) or 0.0),
                'profit_factor': float(report.get('out_of_time', {}).get('profit_factor', 0.0) or 0.0),
                'max_drawdown_pct': 0.08,
            },
            walk_forward={'score': float(report.get('walk_forward_summary', {}).get('score', 0.0) or 0.0)},
            shadow_result={
                # v20260417b：沒有 runtime shadow evidence 時，不再把 offline overfit_gap
                # 塞進 shadow_result；candidate 會因 runtime_observed=False 被擋下。
                'return_drift_pct': float(shadow_runtime.get('shadow_return_drift_pct') or 999.0) if shadow_runtime.get('runtime_observed', False) else 999.0,
                'runtime_observed': bool(shadow_runtime.get('runtime_observed', False)),
                'source': 'shadow_runtime_evidence' if shadow_runtime.get('runtime_observed', False) else 'runtime_shadow_missing_no_offline_fallback',
            },
            rollback_version=(best_entry.get('version') or create_version_tag('no_approved_version')),
        )

        directional_regime = self._load_json(self.regime_service_path)
        directional_alpha = self._load_json(self.alpha_miner_path)

        blocked_reasons: list[str] = []
        if not report:
            blocked_reasons.append('trainer_backend_report_missing')
        if str(training_integrity.get('status') or '') != 'training_integrity_ok':
            blocked_reasons.append(str(training_integrity.get('status') or 'training_integrity_blocked'))
        if not bool(candidate_eval.get('candidate_ready')):
            blocked_reasons.append('candidate_not_ready')
        if str(live_health.get('status') or '') == 'live_metrics_unavailable':
            blocked_reasons.append('live_metrics_unavailable')
        if int(orchestrator.get('training_readiness_pct') or 0) < 100:
            blocked_reasons.append('training_readiness_incomplete')
        if not bool(shadow_runtime.get('runtime_observed', False)):
            blocked_reasons.append('shadow_runtime_evidence_missing')
        overall_status = 'training_governance_mainline_ready' if not blocked_reasons else 'training_governance_mainline_blocked'

        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'system_name': CONFIG.system_name,
            'orchestrator_status': orchestrator.get('status'),
            'training_readiness_pct': orchestrator.get('training_readiness_pct'),
            'backend': backend_result,
            'training_integrity': training_integrity,
            'candidate_evaluation': candidate_eval,
            'live_health': live_health,
            'shadow_runtime_evidence': shadow_runtime,
            'directional_services': {
                'regime_service': directional_regime,
                'alpha_miner': directional_alpha,
            },
            'deep_risks': {
                'training_governance_overfit_risk': training_integrity.get('status') != 'training_integrity_ok',
                'sample_split_guard_ok': bool(report.get('leakage_guards', {}).get('out_of_time_holdout')),
                'feature_selection_train_only': bool(report.get('leakage_guards', {}).get('feature_selection_train_only')),
            },
            'blocked_reasons': blocked_reasons,
            'status': overall_status,
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧠 training governance 主線盤點完成：{self.runtime_path}')
        return self.runtime_path, payload


def main() -> int:
    TrainingGovernanceMainline().build_summary(execute_backend=False)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
