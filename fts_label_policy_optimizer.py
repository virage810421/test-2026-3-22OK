# -*- coding: utf-8 -*-
"""Label policy optimizer v3.

Generates label_policy::default candidates.  It does not rebuild production
training data and does not change config.py.  Approved label policies must be
mounted explicitly before a dataset rebuild.
"""
from __future__ import annotations

import argparse
import json
import random
from copy import deepcopy
from pathlib import Path
from typing import Any

try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None

try:
    from config import PARAMS  # type: ignore
except Exception:  # pragma: no cover
    PARAMS = {}

try:
    from fts_utils import now_str  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')

from param_storage import save_candidate_params

REPORT_PATH = Path('runtime') / 'label_policy_optimizer_report.json'

LABEL_POLICY_SPACE: dict[str, list[Any]] = {
    'TP_BASE_PCT': [0.06, 0.08, 0.10, 0.12],
    'SL_MIN_PCT': [0.02, 0.025, 0.03, 0.04],
    'SL_MAX_PCT': [0.05, 0.06, 0.08, 0.10],
    'LABEL_TP_PCT': [0.03, 0.04, 0.05, 0.06],
    'HOLDING_DAYS': [5, 10, 15, 20],
    'EXIT_HAZARD_THRESHOLD': [0.45, 0.50, 0.55, 0.60],
}


def _dataset_path() -> Path:
    for raw in [PARAMS.get('ML_TRAINING_DATA_PATH'), 'data/ml_training_data.csv']:
        if not raw:
            continue
        p = Path(str(raw))
        if p.exists():
            return p
    return Path('data/ml_training_data.csv')


def _load_training_df():
    if pd is None:
        return None
    path = _dataset_path()
    try:
        if path.exists():
            return pd.read_csv(path)
    except Exception:
        return None
    return None


def _sample_policy() -> dict[str, Any]:
    return {k: random.choice(v) for k, v in LABEL_POLICY_SPACE.items()}


def _health_from_existing_dataset(df) -> dict[str, float]:
    if df is None or getattr(df, 'empty', True):
        return {'train_rows': 0.0, 'positive_ratio': 0.0, 'target_mean': 0.0, 'target_std': 0.0}
    rows = float(len(df))
    target_col = 'Target_Return' if 'Target_Return' in df.columns else None
    target_mean = 0.0
    target_std = 0.0
    positive_ratio = 0.0
    if target_col:
        s = df[target_col]
        try:
            target_mean = float(s.mean())
            target_std = float(s.std())
            positive_ratio = float((s > 0).mean())
        except Exception:
            pass
    elif 'Label' in df.columns:
        try:
            positive_ratio = float((df['Label'] > 0).mean())
        except Exception:
            pass
    return {'train_rows': rows, 'positive_ratio': positive_ratio, 'target_mean': target_mean, 'target_std': target_std}


def _score_policy(policy: dict[str, Any], data_health: dict[str, float]) -> dict[str, Any]:
    tp = float(policy.get('TP_BASE_PCT', 0.10))
    sl_min = float(policy.get('SL_MIN_PCT', 0.03))
    sl_max = float(policy.get('SL_MAX_PCT', 0.08))
    label_tp = float(policy.get('LABEL_TP_PCT', tp / 2.0))
    holding = int(policy.get('HOLDING_DAYS', 10))

    reasons: list[str] = []
    hard_pass = True
    if sl_min <= 0 or sl_max <= 0 or tp <= 0 or label_tp <= 0:
        hard_pass = False
        reasons.append('non_positive_tp_sl_or_label_threshold')
    if sl_min > sl_max:
        hard_pass = False
        reasons.append('sl_min_greater_than_sl_max')
    if label_tp > tp:
        hard_pass = False
        reasons.append('label_tp_above_trade_tp')
    if holding < 3 or holding > 30:
        hard_pass = False
        reasons.append('holding_days_out_of_safety_range')

    pos = float(data_health.get('positive_ratio', 0.0))
    rows = float(data_health.get('train_rows', 0.0))
    balance_score = max(0.0, 1.0 - abs(pos - 0.50) / 0.50) if pos > 0 else 0.35
    row_score = min(rows / 500.0, 1.0) if rows else 0.30
    policy_stability = max(0.0, 1.0 - abs(tp - sl_max) / max(tp + sl_max, 1e-9))
    leakage_safety = 1.0  # static policy only; actual future-data safety belongs to data builder.
    score = 100.0 * (
        0.25 * balance_score
        + 0.20 * min(abs(float(data_health.get('target_std', 0.0))) / 0.05, 1.0)
        + 0.20 * row_score
        + 0.15 * policy_stability
        + 0.10 * leakage_safety
        + 0.10 * (1.0 if hard_pass else 0.0)
    )
    if rows and rows < 60:
        hard_pass = False
        reasons.append('train_rows_too_small_for_label_policy_confidence')
    return {
        'ai_score': round(float(score), 3),
        'hard_gate_pass': bool(hard_pass),
        'reason': reasons or ['label_policy_candidate_health_ok'],
        'data_health': data_health,
    }


def run_label_policy_optimizer(iterations: int = 24, auto_judge: bool = True) -> dict[str, Any]:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = _load_training_df()
    data_health = _health_from_existing_dataset(df)
    rows: list[dict[str, Any]] = []
    for _ in range(max(1, int(iterations))):
        policy = _sample_policy()
        judgement = _score_policy(policy, data_health)
        rows.append({'params': policy, **judgement})
    best = sorted(rows, key=lambda r: (bool(r.get('hard_gate_pass')), float(r.get('ai_score', 0.0))), reverse=True)[0]
    candidate = save_candidate_params(
        scope_name='label_policy::default',
        best_params=best['params'],
        metrics={k: v for k, v in best.items() if k != 'params'},
        source_module='fts_label_policy_optimizer.py',
        note='label policy candidate; requires rebuild training data before formal train',
    )
    judge_payload = None
    if auto_judge:
        try:
            from fts_candidate_ai_judge import judge_candidate_by_id  # type: ignore
            judge_payload = judge_candidate_by_id(candidate['candidate_id'])
        except Exception as exc:
            judge_payload = {'status': 'auto_judge_failed', 'error': repr(exc)}
    report = {
        'generated_at': now_str(),
        'status': 'label_policy_candidate_generated',
        'scope': 'label_policy::default',
        'candidate_id': candidate.get('candidate_id'),
        'best': best,
        'leaderboard': rows[:50],
        'auto_judge': judge_payload,
        'writes_production_config': False,
        'requires_rebuild_training_data': True,
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--iterations', type=int, default=24)
    parser.add_argument('--no-auto-judge', action='store_true')
    args = parser.parse_args(argv)
    payload = run_label_policy_optimizer(args.iterations, auto_judge=not args.no_auto_judge)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
