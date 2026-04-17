# -*- coding: utf-8 -*-
"""AI-style rule judge for label_policy::default candidates."""
from __future__ import annotations
from typing import Any

def _num(x: Any, default: float = 0.0) -> float:
    try: return float(x)
    except Exception: return default

def _score01(x: Any, floor: float = 0.0, cap: float = 1.0) -> float:
    v = _num(x, 0.0)
    return max(0.0, min((v - floor) / max(cap - floor, 1e-9), 1.0))

def judge_candidate(candidate: dict[str, Any], baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    metrics = candidate.get('metrics', {}) or {}
    positive_ratio = _num(metrics.get('positive_label_ratio', metrics.get('label_positive_ratio', 0.0)), 0.0)
    train_rows = _num(metrics.get('train_rows', metrics.get('rows', 0.0)), 0.0)
    target_valid = _num(metrics.get('target_return_valid_ratio', 1.0), 1.0)
    leakage_safety = _num(metrics.get('leakage_safety', 1.0), 1.0)
    target_dist_health = _num(metrics.get('target_return_distribution_health', 0.5), 0.5)
    reasons=[]
    hard=[]
    if train_rows < 80:
        hard.append('train_rows_too_low')
    if not (0.05 <= positive_ratio <= 0.80):
        hard.append('label_balance_out_of_range')
    if target_valid < 0.80:
        hard.append('target_return_valid_ratio_below_floor')
    if leakage_safety < 1.0:
        hard.append('leakage_safety_failed')
    balance_health = 1.0 - min(abs(positive_ratio - 0.35) / 0.35, 1.0)
    score = 100.0 * (
        0.25 * balance_health
        + 0.20 * target_dist_health
        + 0.20 * _score01(train_rows, 80, 1000)
        + 0.15 * _num(metrics.get('oot_after_retrain_potential', 0.5), 0.5)
        + 0.10 * leakage_safety
        + 0.10 * _num(metrics.get('policy_stability', 0.5), 0.5)
    )
    hard_gate_pass = len(set(hard)) == 0
    recommended_status = 'approved_for_rebuild_training_data' if hard_gate_pass and score >= 75.0 else 'rejected'
    reasons = ['label policy candidate passed rebuild hard gates'] if hard_gate_pass else sorted(set(hard))
    return {'enabled': True, 'ai_score': round(score, 4), 'hard_gate_pass': hard_gate_pass, 'recommended_status': recommended_status, 'reason': reasons, 'detail': {'positive_label_ratio': positive_ratio, 'train_rows': train_rows, 'target_valid_ratio': target_valid}}
