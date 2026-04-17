# -*- coding: utf-8 -*-
"""Safe ML training-parameter optimizer.

This module searches only approved ML-training hyperparameter keys and writes
research candidates into param_storage.  It never modifies config.py, never loads
candidate params into daily/paper/live, and never changes governance floors.

Recommended flow:
1) python fts_train_param_optimizer.py --iterations 24
2) inspect runtime/train_param_optimizer_report.json
3) manually approve a candidate through param_storage if desired
4) set TRAIN_USE_APPROVED_PARAMS=True only when you intentionally want train to
   load approved scope="trainer::default".
"""
from __future__ import annotations

import argparse
import itertools
import json
import random
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config import PARAMS
from fts_trainer_backend import evaluate_training_params, _sanitize_training_params
from param_storage import save_candidate_params

RUNTIME_REPORT_PATH = Path('runtime') / 'train_param_optimizer_report.json'
RUNTIME_LEADERBOARD_PATH = Path('runtime') / 'train_param_optimizer_leaderboard.json'
DEFAULT_SCOPE = str(PARAMS.get('TRAIN_PARAM_OPTIMIZER_SCOPE', 'trainer::default'))

TRAIN_PARAM_SPACE: dict[str, list[Any]] = {
    'MODEL_N_ESTIMATORS': [100, 150, 200, 300, 400],
    'MODEL_MAX_DEPTH': [4, 5, 6, 7, 8, 10],
    'MODEL_MIN_SAMPLES_LEAF': [2, 3, 5, 8],
    'MODEL_MIN_SELECTED_FEATURES': [6, 8, 10, 12],
    'MODEL_MAX_SELECTED_FEATURES': [12, 16, 18, 24],
    'OOT_RATIO': [0.15, 0.20, 0.25],
    'WF_GAP': [3, 5, 7, 10],
    'WF_SPLITS': [4, 5, 6],
}

FROZEN_GOVERNANCE_KEYS = [
    'MODEL_MIN_OOT_PF',
    'MODEL_MIN_OOT_HIT_RATE',
    'MODEL_MIN_PROMOTION_SCORE',
    'LIVE_ONLY_USE_PROMOTED_MODEL',
    'LIVE_REQUIRE_PROMOTED_MODEL',
    'MODEL_BLOCK_LIVE_ON_UNPROMOTED',
]


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _base_training_params() -> dict[str, Any]:
    return _sanitize_training_params(dict(PARAMS))


def _candidate_signature(params: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    safe = _sanitize_training_params(params)
    return tuple(sorted((k, safe.get(k)) for k in TRAIN_PARAM_SPACE.keys() if k in safe))


def _make_candidate(space: dict[str, list[Any]], rng: random.Random) -> dict[str, Any]:
    candidate = {key: rng.choice(values) for key, values in space.items()}
    # Keep min/max selected feature counts logically consistent.
    if candidate['MODEL_MAX_SELECTED_FEATURES'] < candidate['MODEL_MIN_SELECTED_FEATURES']:
        candidate['MODEL_MAX_SELECTED_FEATURES'] = candidate['MODEL_MIN_SELECTED_FEATURES']
    return _sanitize_training_params(candidate)


def _generate_candidates(iterations: int, seed: int = 42) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    base = _base_training_params()
    candidates: list[dict[str, Any]] = [base]

    # Include several conservative anchor points before random search.
    conservative_anchors = [
        {
            'MODEL_N_ESTIMATORS': 150,
            'MODEL_MAX_DEPTH': 5,
            'MODEL_MIN_SAMPLES_LEAF': 5,
            'MODEL_MIN_SELECTED_FEATURES': 6,
            'MODEL_MAX_SELECTED_FEATURES': 12,
            'OOT_RATIO': 0.20,
            'WF_GAP': 5,
            'WF_SPLITS': 5,
        },
        {
            'MODEL_N_ESTIMATORS': 200,
            'MODEL_MAX_DEPTH': 6,
            'MODEL_MIN_SAMPLES_LEAF': 5,
            'MODEL_MIN_SELECTED_FEATURES': 8,
            'MODEL_MAX_SELECTED_FEATURES': 16,
            'OOT_RATIO': 0.20,
            'WF_GAP': 7,
            'WF_SPLITS': 5,
        },
        {
            'MODEL_N_ESTIMATORS': 300,
            'MODEL_MAX_DEPTH': 7,
            'MODEL_MIN_SAMPLES_LEAF': 3,
            'MODEL_MIN_SELECTED_FEATURES': 8,
            'MODEL_MAX_SELECTED_FEATURES': 18,
            'OOT_RATIO': 0.25,
            'WF_GAP': 5,
            'WF_SPLITS': 5,
        },
    ]
    candidates.extend(_sanitize_training_params(x) for x in conservative_anchors)

    # Random search keeps runtime bounded; do not exhaustively grid-search.
    max_total = max(1, int(iterations))
    seen = {_candidate_signature(c) for c in candidates}
    attempts = 0
    while len(candidates) < max_total and attempts < max_total * 30:
        attempts += 1
        c = _make_candidate(TRAIN_PARAM_SPACE, rng)
        sig = _candidate_signature(c)
        if sig in seen:
            continue
        seen.add(sig)
        candidates.append(c)
    return candidates[:max_total]


def _metric_float(payload: dict[str, Any], path: tuple[str, ...], default: float = 0.0) -> float:
    cur: Any = payload
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    try:
        return float(cur)
    except Exception:
        return default


def _beats_baseline(candidate: dict[str, Any], baseline: dict[str, Any], min_delta: float) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if candidate.get('status') != 'ok':
        reasons.append('candidate_not_ok')
    if candidate.get('hard_failures'):
        reasons.append('candidate_has_hard_failures')
    c_score = float(candidate.get('optimizer_score', -1e18) or -1e18)
    b_score = float(baseline.get('optimizer_score', -1e18) or -1e18)
    if c_score < b_score + min_delta:
        reasons.append('optimizer_score_not_above_baseline_delta')
    # Protect stability: do not accept prettier score by materially hurting OOT/WF.
    c_pf = _metric_float(candidate, ('out_of_time', 'profit_factor'))
    b_pf = _metric_float(baseline, ('out_of_time', 'profit_factor'))
    if c_pf + 0.02 < b_pf:
        reasons.append('oot_profit_factor_regressed')
    c_hit = _metric_float(candidate, ('out_of_time', 'hit_rate'))
    b_hit = _metric_float(baseline, ('out_of_time', 'hit_rate'))
    if c_hit + 0.01 < b_hit:
        reasons.append('oot_hit_rate_regressed')
    c_wf = _metric_float(candidate, ('walk_forward', 'ret_mean'))
    b_wf = _metric_float(baseline, ('walk_forward', 'ret_mean'))
    if c_wf + 0.001 < b_wf:
        reasons.append('walk_forward_ret_mean_regressed')
    return (len(reasons) == 0), reasons


def run_training_param_search(
    iterations: int | None = None,
    scope_name: str = DEFAULT_SCOPE,
    dataset_path: str | Path = 'data/ml_training_data.csv',
    seed: int = 42,
    min_delta: float = 0.05,
) -> dict[str, Any]:
    iterations = int(iterations or PARAMS.get('TRAIN_PARAM_OPTIMIZER_ITERATIONS', 24))
    RUNTIME_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    dataset_path = Path(dataset_path)
    if not dataset_path.exists() or dataset_path.stat().st_size == 0:
        report = {
            'generated_at': _now(),
            'status': 'blocked',
            'reason': 'training_dataset_missing_or_empty',
            'dataset_path': str(dataset_path),
            'writes_production_config': False,
            'candidate_saved': False,
        }
        RUNTIME_REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        return report

    df = pd.read_csv(dataset_path, low_memory=False)
    base_params = _base_training_params()
    baseline_eval = evaluate_training_params(df=df, params=base_params, dry_run=True, write_artifacts=False)

    candidates = _generate_candidates(iterations=iterations, seed=seed)
    leaderboard: list[dict[str, Any]] = []
    for idx, params in enumerate(candidates):
        result = evaluate_training_params(df=df, params=params, dry_run=True, write_artifacts=False)
        result['candidate_index'] = idx
        result['is_baseline_params'] = _candidate_signature(params) == _candidate_signature(base_params)
        leaderboard.append(result)

    leaderboard.sort(key=lambda row: float(row.get('optimizer_score', -1e18) or -1e18), reverse=True)
    best = leaderboard[0] if leaderboard else {'status': 'blocked', 'reason': 'no_candidates_evaluated'}
    recommended, reject_reasons = _beats_baseline(best, baseline_eval, min_delta=min_delta)

    saved_candidate: dict[str, Any] | None = None
    if recommended:
        saved_candidate = save_candidate_params(
            scope_name=scope_name,
            best_params=best.get('params', {}),
            metrics={
                'optimizer_score': best.get('optimizer_score'),
                'overall_score': best.get('overall_score'),
                'out_of_time': best.get('out_of_time', {}),
                'walk_forward': best.get('walk_forward', {}),
                'selected_feature_count': best.get('selected_feature_count'),
                'baseline_optimizer_score': baseline_eval.get('optimizer_score'),
                'baseline_overall_score': baseline_eval.get('overall_score'),
                'hard_failures': best.get('hard_failures', []),
            },
            source_module='fts_train_param_optimizer.py',
            note='research-only ML trainer hyperparameter candidate; not production config',
        )
        try:
            if bool(PARAMS.get('CANDIDATE_AI_JUDGE_ENABLED', True)):
                from fts_candidate_ai_judge import judge_latest
                auto_judge_report = judge_latest(scope=scope_name, auto_apply=True)
            else:
                auto_judge_report = {'status': 'disabled_by_config'}
        except Exception as exc:
            auto_judge_report = {'status': 'candidate_ai_judge_error', 'reason': repr(exc)}
    else:
        auto_judge_report = {'status': 'no_candidate_saved'}

    slim_leaderboard = []
    for row in leaderboard:
        slim_leaderboard.append({
            'candidate_index': row.get('candidate_index'),
            'status': row.get('status'),
            'optimizer_score': row.get('optimizer_score'),
            'overall_score': row.get('overall_score'),
            'selected_feature_count': row.get('selected_feature_count'),
            'out_of_time': row.get('out_of_time', {}),
            'walk_forward': row.get('walk_forward', {}),
            'hard_failures': row.get('hard_failures', []),
            'params': row.get('params', {}),
            'is_baseline_params': row.get('is_baseline_params', False),
        })
    RUNTIME_LEADERBOARD_PATH.write_text(json.dumps(slim_leaderboard, ensure_ascii=False, indent=2), encoding='utf-8')

    report = {
        'generated_at': _now(),
        'status': 'candidate_saved' if saved_candidate else 'completed_no_candidate_saved',
        'scope_name': scope_name,
        'dataset_path': str(dataset_path),
        'iterations_requested': iterations,
        'candidates_evaluated': len(leaderboard),
        'search_space': TRAIN_PARAM_SPACE,
        'frozen_governance_keys': FROZEN_GOVERNANCE_KEYS,
        'writes_production_config': False,
        'loads_candidate_into_train': False,
        'candidate_saved': saved_candidate is not None,
        'candidate_id': saved_candidate.get('candidate_id') if saved_candidate else None,
        'candidate_ai_judge': auto_judge_report,
        'baseline': {
            'status': baseline_eval.get('status'),
            'optimizer_score': baseline_eval.get('optimizer_score'),
            'overall_score': baseline_eval.get('overall_score'),
            'out_of_time': baseline_eval.get('out_of_time', {}),
            'walk_forward': baseline_eval.get('walk_forward', {}),
            'params': baseline_eval.get('params', {}),
            'hard_failures': baseline_eval.get('hard_failures', []),
        },
        'best': {
            'status': best.get('status'),
            'optimizer_score': best.get('optimizer_score'),
            'overall_score': best.get('overall_score'),
            'out_of_time': best.get('out_of_time', {}),
            'walk_forward': best.get('walk_forward', {}),
            'selected_feature_count': best.get('selected_feature_count'),
            'params': best.get('params', {}),
            'hard_failures': best.get('hard_failures', []),
        },
        'recommendation': 'approve_candidate_after_manual_review' if saved_candidate else 'keep_baseline_or_review_report',
        'reject_reasons': reject_reasons,
        'leaderboard_path': str(RUNTIME_LEADERBOARD_PATH),
        'top5': slim_leaderboard[:5],
    }
    RUNTIME_REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description='Safe ML trainer hyperparameter optimizer')
    parser.add_argument('--iterations', type=int, default=int(PARAMS.get('TRAIN_PARAM_OPTIMIZER_ITERATIONS', 24)))
    parser.add_argument('--scope', type=str, default=DEFAULT_SCOPE)
    parser.add_argument('--dataset', type=str, default='data/ml_training_data.csv')
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--min-delta', type=float, default=0.05)
    args = parser.parse_args()
    report = run_training_param_search(
        iterations=args.iterations,
        scope_name=args.scope,
        dataset_path=args.dataset,
        seed=args.seed,
        min_delta=args.min_delta,
    )
    print('🧪 train param optimizer:', report.get('status'))
    print('📄 report:', RUNTIME_REPORT_PATH)
    print('🏁 candidate_saved:', report.get('candidate_saved'), '| candidate_id:', report.get('candidate_id'))


if __name__ == '__main__':
    main()
