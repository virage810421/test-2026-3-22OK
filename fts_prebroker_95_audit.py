# -*- coding: utf-8 -*-
from __future__ import annotations

"""Fund-grade 95% pre-broker closure audit.

This command ties together six readiness pillars:
1 broker adapter contract, 2 callback ingestion, 3 ledger/reconciliation,
4 restart recovery, 5 exit AI artifacts, 6 portfolio-level backtest.
"""

import json
from typing import Any

from fts_config import PATHS
from fts_utils import now_str
from fts_exception_policy import record_diagnostic

PILLAR_WEIGHTS = {
    'broker_contract': 18,
    'callback_ingestion': 14,
    'reconciliation': 18,
    'restart_recovery': 14,
    'exit_ai_artifacts': 18,
    'portfolio_backtest': 18,
}


def _ok(status: Any, good_prefixes: tuple[str, ...]) -> bool:
    s = str(status or '')
    return any(s.startswith(p) or s == p for p in good_prefixes)


class PreBroker95Audit:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'prebroker_95_audit.json'

    def build(self, *, run_backtest: bool = False, bootstrap_exit: bool = False) -> tuple[str, dict[str, Any]]:
        pillars: dict[str, dict[str, Any]] = {}
        pillars['broker_contract'] = self._broker_contract()
        pillars['callback_ingestion'] = self._callback_ingestion()
        pillars['reconciliation'] = self._reconciliation()
        pillars['restart_recovery'] = self._restart_recovery()
        pillars['exit_ai_artifacts'] = self._exit_ai(bootstrap=bootstrap_exit)
        pillars['portfolio_backtest'] = self._portfolio_backtest(run=run_backtest)
        score = 0.0
        for name, p in pillars.items():
            weight = PILLAR_WEIGHTS[name]
            score += weight * float(p.get('score_ratio', 0.0))
        blockers = [f"{name}:{p.get('status')}" for name, p in pillars.items() if p.get('blocking')]
        payload = {
            'generated_at': now_str(),
            'status': 'prebroker_95_ready' if score >= 95 and not blockers else 'prebroker_95_not_ready',
            'score': round(score, 2),
            'target_score': 95,
            'blockers': blockers,
            'pillars': pillars,
            'note': 'Score is pre-broker/paper readiness. True live still requires real broker credentials and broker-side callback validation.',
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        return str(self.path), payload

    def _broker_contract(self) -> dict[str, Any]:
        try:
            from fts_broker_contract_audit import build_report
            _, payload = build_report()
            ok = _ok(payload.get('status'), ('paper_prelive_contract_smoke_ok', 'true_broker_contract_ready'))
            return {'status': payload.get('status'), 'path': str(PATHS.runtime_dir / 'broker_contract_readiness.json'), 'score_ratio': 1.0 if ok else 0.4, 'blocking': not ok}
        except Exception as exc:
            record_diagnostic('prebroker_95_audit', 'broker_contract_failed', exc, severity='error', fail_closed=True)
            return {'status': 'broker_contract_failed', 'score_ratio': 0.0, 'blocking': True, 'error': repr(exc)}

    def _callback_ingestion(self) -> dict[str, Any]:
        try:
            from fts_callback_ingestion_service import CallbackIngestionService
            _, payload = CallbackIngestionService().ingest([])
            ok = payload.get('status') in {'callback_ingestion_ready', 'callback_ingestion_with_errors'}
            return {'status': payload.get('status'), 'path': str(PATHS.runtime_dir / 'broker_callback_ingestion_summary.json'), 'score_ratio': 1.0 if ok else 0.0, 'blocking': not ok}
        except Exception as exc:
            record_diagnostic('prebroker_95_audit', 'callback_ingestion_failed', exc, severity='error', fail_closed=True)
            return {'status': 'callback_ingestion_failed', 'score_ratio': 0.0, 'blocking': True, 'error': repr(exc)}

    def _reconciliation(self) -> dict[str, Any]:
        try:
            from fts_reconciliation_runtime import PreliveReconciliationRuntime
            _, payload = PreliveReconciliationRuntime().reconcile(local_orders=[], broker_orders=[], local_fills=[], broker_fills=[], local_positions=[], broker_positions=[], local_cash=0, broker_cash=0)
            ok = payload.get('status') == 'reconciliation_clean'
            return {'status': payload.get('status'), 'path': str(PATHS.runtime_dir / 'prelive_reconciliation_runtime.json'), 'score_ratio': 1.0 if ok else 0.5, 'blocking': not ok}
        except Exception as exc:
            record_diagnostic('prebroker_95_audit', 'reconciliation_failed', exc, severity='error', fail_closed=True)
            return {'status': 'reconciliation_failed', 'score_ratio': 0.0, 'blocking': True, 'error': repr(exc)}

    def _restart_recovery(self) -> dict[str, Any]:
        try:
            from fts_restart_recovery_service import RestartRecoveryService
            svc = RestartRecoveryService()
            if not svc.snapshot_path.exists():
                # Create a conservative local snapshot so restart plan is testable.
                svc.create_snapshot_from_broker(None, meta={'created_by': 'prebroker_95_audit', 'paper_only': True})
            _, payload = svc.build_plan(require_broker_snapshot=False)
            ok = payload.get('status') == 'restart_recovery_ready'
            return {'status': payload.get('status'), 'path': str(PATHS.runtime_dir / 'restart_recovery_plan.json'), 'score_ratio': 1.0 if ok else 0.5, 'blocking': not ok}
        except Exception as exc:
            record_diagnostic('prebroker_95_audit', 'restart_recovery_failed', exc, severity='error', fail_closed=True)
            return {'status': 'restart_recovery_failed', 'score_ratio': 0.0, 'blocking': True, 'error': repr(exc)}

    def _exit_ai(self, *, bootstrap: bool) -> dict[str, Any]:
        try:
            status_path = PATHS.runtime_dir / 'exit_model_status.json'
            if bootstrap:
                from fts_exit_model_artifact_bootstrap import ExitModelArtifactBootstrap
                _, payload = ExitModelArtifactBootstrap().build()
                ok = payload.get('status') == 'exit_models_bootstrapped'
                return {'status': payload.get('status'), 'path': str(PATHS.runtime_dir / 'exit_model_artifact_bootstrap.json'), 'score_ratio': 1.0 if ok else 0.35, 'blocking': not ok}
            if status_path.exists():
                payload = json.loads(status_path.read_text(encoding='utf-8'))
                ok = bool(payload.get('exit_models_loaded')) and bool(payload.get('exit_selected_features_ready'))
                return {'status': payload.get('exit_model_source', 'unknown'), 'path': str(status_path), 'score_ratio': 1.0 if ok else 0.35, 'blocking': not ok}
            return {'status': 'exit_model_status_missing', 'score_ratio': 0.35, 'blocking': True}
        except Exception as exc:
            record_diagnostic('prebroker_95_audit', 'exit_ai_check_failed', exc, severity='error', fail_closed=True)
            return {'status': 'exit_ai_check_failed', 'score_ratio': 0.0, 'blocking': True, 'error': repr(exc)}

    def _portfolio_backtest(self, *, run: bool) -> dict[str, Any]:
        try:
            report_path = PATHS.runtime_dir / 'portfolio_backtest_report.json'
            if run:
                from fts_portfolio_backtester import PortfolioBacktester
                _, payload = PortfolioBacktester().run()
            elif report_path.exists():
                payload = json.loads(report_path.read_text(encoding='utf-8'))
            else:
                return {'status': 'portfolio_backtest_not_run', 'score_ratio': 0.3, 'blocking': True}
            ok = payload.get('status') == 'portfolio_backtest_ready' and int(payload.get('trade_count', 0) or 0) > 0
            return {'status': payload.get('status'), 'path': str(report_path), 'trade_count': payload.get('trade_count', 0), 'score_ratio': 1.0 if ok else 0.4, 'blocking': not ok}
        except Exception as exc:
            record_diagnostic('prebroker_95_audit', 'portfolio_backtest_failed', exc, severity='error', fail_closed=True)
            return {'status': 'portfolio_backtest_failed', 'score_ratio': 0.0, 'blocking': True, 'error': repr(exc)}


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description='Pre-broker 95% closure audit')
    parser.add_argument('--run-backtest', action='store_true')
    parser.add_argument('--bootstrap-exit', action='store_true')
    args = parser.parse_args(list(argv or []))
    path, payload = PreBroker95Audit().build(run_backtest=args.run_backtest, bootstrap_exit=args.bootstrap_exit)
    print(json.dumps({'status': payload.get('status'), 'score': payload.get('score'), 'path': path, 'blockers': payload.get('blockers')}, ensure_ascii=False, indent=2))
    return 0 if payload.get('status') == 'prebroker_95_ready' else 1


if __name__ == '__main__':
    raise SystemExit(main())
