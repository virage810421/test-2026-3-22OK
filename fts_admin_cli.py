# -*- coding: utf-8 -*-
from __future__ import annotations

"""
FTS admin CLI: consolidated replacement for small run_*.py entry scripts.

Supported examples:
  python fts_admin_cli.py healthcheck
  python fts_admin_cli.py healthcheck --deep
  python fts_admin_cli.py --deep                         # backward-compatible shortcut
  python fts_admin_cli.py completion-audit
  python fts_admin_cli.py training-stress-audit
  python fts_admin_cli.py backfill-resilience-audit
  python fts_admin_cli.py full-market-percentile
  python fts_admin_cli.py event-calendar-build
  python fts_admin_cli.py sync-feature-snapshots
  python fts_admin_cli.py clean-old-doors --apply
"""

import argparse
import inspect
import sys
from typing import Callable, Sequence


def _call_main_with_argv(main_func: Callable, argv: Sequence[str] | None = None) -> int:
    """Call a module main() safely without leaking fts_admin_cli.py arguments."""
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


def run_clean_old_doors(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog='fts_admin_cli.py clean-old-doors')
    parser.add_argument('--apply', action='store_true', help='Actually remove retired old-door files.')
    args = parser.parse_args(list(argv or []))
    from fts_legacy_facade_cleanup import main as cleanup_main
    return int(cleanup_main(apply=bool(args.apply)) or 0)


_COMMANDS: dict[str, Callable[[Sequence[str] | None], int]] = {
    'healthcheck': run_healthcheck,
    'completion-audit': run_completion_audit,
    'training-stress-audit': run_training_stress_audit,
    'backfill-resilience-audit': run_backfill_resilience_audit,
    'full-market-percentile': run_full_market_percentile,
    'event-calendar-build': run_event_calendar_build,
    'sync-feature-snapshots': run_sync_feature_snapshots,
    'clean-old-doors': run_clean_old_doors,
}


_ALIASES = {
    'project-healthcheck': 'healthcheck',
    'run-healthcheck': 'healthcheck',
    'clean': 'clean-old-doors',
}


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)

    # Backward-compatible shortcut: `python fts_admin_cli.py --deep` should run healthcheck --deep.
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
