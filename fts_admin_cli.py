# -*- coding: utf-8 -*-
from __future__ import annotations

"""
FTS admin CLI: consolidated replacement for small run_*.py entry scripts.

Supported examples:
  python fts_admin_cli.py healthcheck
  python fts_admin_cli.py healthcheck --deep
  python fts_admin_cli.py --deep
  python fts_admin_cli.py completion-audit
  python fts_admin_cli.py training-stress-audit
  python fts_admin_cli.py backfill-resilience-audit
  python fts_admin_cli.py full-market-percentile
  python fts_admin_cli.py event-calendar-build
  python fts_admin_cli.py sync-feature-snapshots
  python fts_admin_cli.py clean-old-doors --apply
  python fts_admin_cli.py second-merge-cleanup
  python fts_admin_cli.py drop-readiness
  python fts_admin_cli.py broker-contract-audit
  python fts_admin_cli.py callback-ingest
  python fts_admin_cli.py reconciliation-runtime
  python fts_admin_cli.py restart-recovery
  python fts_admin_cli.py shadow-evidence
  python fts_admin_cli.py twap3-runtime
  python fts_admin_cli.py runtime-closure
  python fts_admin_cli.py exit-artifact-bootstrap
  python fts_admin_cli.py portfolio-backtest --period 3y
  python fts_admin_cli.py prebroker-95-audit --run-backtest --bootstrap-exit
  python fts_admin_cli.py train-param-optimize --iterations 24
  python fts_admin_cli.py param-ai-judge --scope trainer::default
  python fts_admin_cli.py param-release-gate --scope strategy_signal::default
  python fts_admin_cli.py approved-param-mount-report
  python fts_admin_cli.py param-governance --all-scopes
"""

import argparse
import inspect
import sys
from typing import Callable, Sequence


def _call_main_with_argv(main_func: Callable, argv: Sequence[str] | None = None) -> int:
    argv_list = list(argv or [])
    try:
        sig = inspect.signature(main_func)
        if len(sig.parameters) >= 1:
            return int(main_func(argv_list) or 0)
    except (TypeError, ValueError):
        pass

    old_argv = sys.argv[:]
    try:
        sys.argv = [getattr(main_func, "__module__", "module")] + argv_list
        return int(main_func() or 0)
    finally:
        sys.argv = old_argv


def run_healthcheck(argv: Sequence[str] | None = None) -> int:
    from fts_project_healthcheck import main
    return _call_main_with_argv(main, argv)


def run_completion_audit(argv: Sequence[str] | None = None) -> int:
    try:
        from fts_project_quality_suite import ProjectCompletionAudit
        ProjectCompletionAudit().build()
        return 0
    except Exception:
        from fts_project_completion_audit import main
        return _call_main_with_argv(main, argv)


def run_training_stress_audit(argv: Sequence[str] | None = None) -> int:
    try:
        from fts_training_quality_suite import TrainingStressAudit
        TrainingStressAudit().build()
        return 0
    except Exception:
        from fts_training_stress_audit import main
        return _call_main_with_argv(main, argv)


def run_backfill_resilience_audit(argv: Sequence[str] | None = None) -> int:
    from fts_backfill_resilience_audit import BackfillResilienceAudit
    BackfillResilienceAudit().build()
    return 0


def run_full_market_percentile(argv: Sequence[str] | None = None) -> int:
    from fts_cross_sectional_percentile_service import CrossSectionalPercentileService
    CrossSectionalPercentileService().build_snapshot()
    return 0


def run_event_calendar_build(argv: Sequence[str] | None = None) -> int:
    from fts_event_calendar_service import EventCalendarService
    EventCalendarService().build_summary()
    return 0


def run_sync_feature_snapshots(argv: Sequence[str] | None = None) -> int:
    from fts_sql_feature_snapshot_sync import sync_all
    sync_all()
    return 0


def run_drop_readiness(argv: Sequence[str] | None = None) -> int:
    from fts_deprecated_drop_readiness import main
    return _call_main_with_argv(main, argv)


def run_second_merge_cleanup(argv: Sequence[str] | None = None) -> int:
    from cleanup_second_merge_retired_py_files import main
    return _call_main_with_argv(main, argv)


def run_clean_old_doors(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='fts_admin_cli.py clean-old-doors')
    parser.add_argument('--apply', action='store_true', help='Actually remove retired old-door files.')
    args = parser.parse_args(list(argv or []))
    from fts_legacy_facade_cleanup import main as cleanup_main
    return int(cleanup_main(apply=bool(args.apply)) or 0)


def run_broker_contract_audit(argv: Sequence[str] | None = None) -> int:
    from fts_broker_contract_audit import main
    return _call_main_with_argv(main, argv)


def run_callback_ingest(argv: Sequence[str] | None = None) -> int:
    from fts_callback_ingestion_service import main
    return _call_main_with_argv(main, argv)


def run_reconciliation_runtime(argv: Sequence[str] | None = None) -> int:
    from fts_reconciliation_runtime import main
    return _call_main_with_argv(main, argv)


def run_restart_recovery(argv: Sequence[str] | None = None) -> int:
    from fts_restart_recovery_service import main
    return _call_main_with_argv(main, argv)


def run_shadow_evidence(argv: Sequence[str] | None = None) -> int:
    from fts_shadow_runtime_evidence import main
    return _call_main_with_argv(main, argv)


def run_twap3_runtime(argv: Sequence[str] | None = None) -> int:
    from fts_twap3_runtime_closure import main
    return _call_main_with_argv(main, argv)

def run_runtime_closure(argv: Sequence[str] | None = None) -> int:
    from fts_nonbroker_runtime_closure_gate import main
    return _call_main_with_argv(main, argv)


def run_exit_artifact_bootstrap(argv: Sequence[str] | None = None) -> int:
    from fts_exit_model_artifact_bootstrap import main
    return _call_main_with_argv(main, argv)


def run_portfolio_backtest(argv: Sequence[str] | None = None) -> int:
    from fts_portfolio_backtester import main
    return _call_main_with_argv(main, argv)


def run_prebroker_95_audit(argv: Sequence[str] | None = None) -> int:
    from fts_prebroker_95_audit import main
    return _call_main_with_argv(main, argv)





def run_maturity_upgrade(argv: Sequence[str] | None = None) -> int:
    from fts_maturity_upgrade_suite import MaturityUpgradeSuite
    MaturityUpgradeSuite().build()
    return 0


def run_patch_retirement_report(argv: Sequence[str] | None = None) -> int:
    from datetime import datetime
    from pathlib import Path
    import json

    runtime_dir = Path('runtime')
    runtime_dir.mkdir(parents=True, exist_ok=True)
    patch_files = sorted(str(p.name) for p in Path('.').glob('_patch_manifest*.py'))
    payload = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'patch_retirement_report_ready',
        'patch_manifest_count': len(patch_files),
        'patch_manifests': patch_files[:200],
        'notes': 'Generated from local _patch_manifest*.py files. Review before destructive cleanup.',
    }
    path = runtime_dir / 'patch_retirement_report.json'
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'🧹 patch retirement report：{path}')
    return 0


def run_train_param_optimize(argv: Sequence[str] | None = None) -> int:
    from fts_train_param_optimizer import main
    return _call_main_with_argv(main, argv)


def run_param_ai_judge(argv: Sequence[str] | None = None) -> int:
    from fts_candidate_ai_judge import main
    return _call_main_with_argv(main, argv)


def run_param_release_gate(argv: Sequence[str] | None = None) -> int:
    from fts_param_release_gate import main
    return _call_main_with_argv(main, argv)


def run_approved_param_mount_report(argv: Sequence[str] | None = None) -> int:
    from fts_approved_param_mount import main
    return _call_main_with_argv(main, argv)


def run_param_governance(argv: Sequence[str] | None = None) -> int:
    from fts_param_governance_orchestrator import main
    return _call_main_with_argv(main, argv)

def run_param_evidence_collect(argv: Sequence[str] | None = None) -> int:
    from fts_param_evidence_collector import main
    return _call_main_with_argv(main, argv)

def run_label_policy_optimize(argv: Sequence[str] | None = None) -> int:
    from fts_label_policy_optimizer import main
    return _call_main_with_argv(main, argv)

def run_execution_policy_optimize(argv: Sequence[str] | None = None) -> int:
    from fts_execution_policy_optimizer import main
    return _call_main_with_argv(main, argv)

_COMMANDS: dict[str, Callable[[Sequence[str] | None], int]] = {
    'healthcheck': run_healthcheck,
    'completion-audit': run_completion_audit,
    'training-stress-audit': run_training_stress_audit,
    'backfill-resilience-audit': run_backfill_resilience_audit,
    'full-market-percentile': run_full_market_percentile,
    'event-calendar-build': run_event_calendar_build,
    'sync-feature-snapshots': run_sync_feature_snapshots,
    'clean-old-doors': run_clean_old_doors,
    'drop-readiness': run_drop_readiness,
    'second-merge-cleanup': run_second_merge_cleanup,
    'broker-contract-audit': run_broker_contract_audit,
    'callback-ingest': run_callback_ingest,
    'reconciliation-runtime': run_reconciliation_runtime,
    'restart-recovery': run_restart_recovery,
    'shadow-evidence': run_shadow_evidence,
    'twap3-runtime': run_twap3_runtime,
    'runtime-closure': run_runtime_closure,
    'exit-artifact-bootstrap': run_exit_artifact_bootstrap,
    'portfolio-backtest': run_portfolio_backtest,
    'prebroker-95-audit': run_prebroker_95_audit,
    'maturity-upgrade': run_maturity_upgrade,
    'patch-retirement-report': run_patch_retirement_report,
    'train-param-optimize': run_train_param_optimize,
    'param-ai-judge': run_param_ai_judge,
    'param-release-gate': run_param_release_gate,
    'approved-param-mount-report': run_approved_param_mount_report,
    'param-governance': run_param_governance,
    'param-evidence-collect': run_param_evidence_collect,
    'label-policy-optimize': run_label_policy_optimize,
    'execution-policy-optimize': run_execution_policy_optimize,
}


_ALIASES = {
    'project-healthcheck': 'healthcheck',
    'run-healthcheck': 'healthcheck',
    'clean': 'clean-old-doors',
    'deprecated-scan': 'drop-readiness',
    'drop-readiness-report': 'drop-readiness',
    'cleanup-second-merge': 'second-merge-cleanup',
    'broker-audit': 'broker-contract-audit',
    'callbacks': 'callback-ingest',
    'reconcile': 'reconciliation-runtime',
    'recovery': 'restart-recovery',
    'shadow': 'shadow-evidence',
    'twap3': 'twap3-runtime',
    'closure': 'runtime-closure',
    'nonbroker-closure': 'runtime-closure',
    'exit-bootstrap': 'exit-artifact-bootstrap',
    'portfolio-bt': 'portfolio-backtest',
    'prelive-95': 'prebroker-95-audit',
    'maturity': 'maturity-upgrade',
    'retirement-report': 'patch-retirement-report',
    'train-param-search': 'train-param-optimize',
    'candidate-judge': 'param-ai-judge',
    'release-gate': 'param-release-gate',
    'param-mount': 'approved-param-mount-report',
    'param-evidence': 'param-evidence-collect',
    'label-optimize': 'label-policy-optimize',
    'execution-optimize': 'execution-policy-optimize',
}



def main(argv: Sequence[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if not raw or raw[0].startswith('-'):
        return run_healthcheck(raw)
    command = _ALIASES.get(raw[0], raw[0])
    passthrough = raw[1:]
    if command not in _COMMANDS:
        parser = argparse.ArgumentParser(description='FTS admin CLI')
        parser.add_argument('command', choices=sorted(_COMMANDS))
        parser.parse_args(raw[:1])
        return 2
    return int(_COMMANDS[command](passthrough) or 0)


if __name__ == '__main__':
    raise SystemExit(main())
