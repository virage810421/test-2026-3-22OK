# -*- coding: utf-8 -*-
"""Runtime diagnostic for entry/exit strictness.

Reads decision / entry tracking / lifecycle runtime artifacts and reports whether
current parameters are too loose, too strict, or balanced.  This is used by
parameter governance as evidence; it never changes config, never promotes live,
and never submits orders.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

try:
    from config import PARAMS  # type: ignore
except Exception:  # pragma: no cover
    PARAMS = {}

try:
    from fts_approved_param_mount import get_effective_params_for_mode
except Exception:  # pragma: no cover
    def get_effective_params_for_mode(mode: str, base_params=None, stage=None):
        return dict(base_params or {})

from fts_entry_exit_param_policy import coerce_entry_exit_params, evaluate_strictness_health, write_policy_report

try:
    from fts_utils import now_str  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')

RUNTIME_DIR = Path('runtime')
DATA_DIR = Path('data')
REPORT_PATH = RUNTIME_DIR / 'entry_exit_strictness_diagnostic.json'


def _read_json(path: Path) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        pass
    return {}


def _read_decision_df() -> Any:
    if pd is None:
        return None
    for path in [
        DATA_DIR / 'normalized_decision_output_enriched.csv',
        DATA_DIR / 'normalized_decision_output.csv',
        Path('daily_decision_desk.csv'),
        DATA_DIR / 'daily_decision_desk.csv',
    ]:
        if path.exists():
            try:
                df = pd.read_csv(path)
                if df is not None and not df.empty:
                    return df
            except Exception:
                continue
    return None


def _safe_rate(n: float, d: float) -> float:
    try:
        return float(n) / float(d) if float(d) else 0.0
    except Exception:
        return 0.0


def _decision_metrics() -> dict[str, Any]:
    df = _read_decision_df()
    if df is None or getattr(df, 'empty', True):
        return {}
    total = len(df)
    def has_col(name: str) -> bool:
        return name in df.columns
    entry = df['Entry_State'].astype(str).str.upper() if has_col('Entry_State') else pd.Series([''] * total)
    exit_state = df['Exit_State'].astype(str).str.upper() if has_col('Exit_State') else pd.Series([''] * total)
    exec_eligible = df['ExecutionEligible'] if has_col('ExecutionEligible') else pd.Series([False] * total)
    try:
        exec_eligible = exec_eligible.astype(str).str.lower().isin({'1','true','yes','y','on'})
    except Exception:
        pass
    prepare_count = int((entry == 'PREPARE').sum())
    pilot_count = int((entry == 'PILOT_ENTRY').sum())
    full_count = int((entry == 'FULL_ENTRY').sum())
    executable_count = int(exec_eligible.sum()) if hasattr(exec_eligible, 'sum') else 0
    rejected_count = int(((entry.isin(['PILOT_ENTRY', 'FULL_ENTRY'])) & (~exec_eligible)).sum()) if hasattr(exec_eligible, '__invert__') else 0
    reduce_count = int((exit_state == 'REDUCE').sum())
    defend_count = int((exit_state == 'DEFEND').sum())
    exit_count = int((exit_state == 'EXIT').sum())
    return {
        'decision_rows': total,
        'trade_count': executable_count,
        'prepare_count': prepare_count,
        'pilot_count': pilot_count,
        'full_entry_count': full_count,
        'signal_reject_rate': _safe_rate(rejected_count, max(pilot_count + full_count, 1)),
        'pilot_to_full_rate': _safe_rate(full_count, max(pilot_count, 1)),
        'empty_signal_ratio': _safe_rate(total - (prepare_count + pilot_count + full_count), max(total, 1)),
        'reduce_count': reduce_count,
        'defend_count': defend_count,
        'exit_count': exit_count,
    }


def _runtime_metrics() -> dict[str, Any]:
    entry_tracking = _read_json(RUNTIME_DIR / 'entry_tracking_journal.json')
    lifecycle = _read_json(RUNTIME_DIR / 'position_lifecycle.json')
    metrics: dict[str, Any] = {}
    if isinstance(entry_tracking, dict):
        summary = entry_tracking.get('summary', {}) if isinstance(entry_tracking.get('summary'), dict) else {}
        metrics.update({k: v for k, v in summary.items() if k not in metrics})
    if isinstance(lifecycle, dict):
        positions = lifecycle.get('positions', {}) if isinstance(lifecycle.get('positions'), dict) else {}
        if positions:
            recs = [str(v.get('recommendation', '')).upper() for v in positions.values() if isinstance(v, dict)]
            metrics['lifecycle_position_count'] = len(recs)
            metrics['lifecycle_exit_count'] = sum(1 for x in recs if x == 'EXIT')
            metrics['lifecycle_reduce_count'] = sum(1 for x in recs if x == 'REDUCE')
            metrics['lifecycle_defend_count'] = sum(1 for x in recs if x == 'DEFEND')
    return metrics


def build_entry_exit_strictness_diagnostic(write: bool = True) -> dict[str, Any]:
    params = coerce_entry_exit_params(get_effective_params_for_mode('strategy_signal', dict(PARAMS)))
    metrics = {}
    metrics.update(_decision_metrics())
    metrics.update(_runtime_metrics())
    health = evaluate_strictness_health(metrics, params)
    payload = {
        'generated_at': now_str(),
        'status': 'entry_exit_strictness_diagnostic_ready',
        'metrics': metrics,
        'health': health,
        'effective_entry_exit_params': {k: params.get(k) for k in sorted(params) if k.startswith(('PREENTRY_', 'CONFIRM_', 'ENTRY_', 'PILOT_', 'FULL_', 'STATE_EXIT_', 'EXIT_', 'RANGE_', 'MISSING_'))},
        'writes_production_config': False,
        'promotes_live': False,
    }
    if write:
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        write_policy_report(metrics=metrics, params=params)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-write', action='store_true')
    args = parser.parse_args(argv)
    payload = build_entry_exit_strictness_diagnostic(write=not args.no_write)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
