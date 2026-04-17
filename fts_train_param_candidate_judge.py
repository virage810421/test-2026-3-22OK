# -*- coding: utf-8 -*-
"""AI-style rule judge for trainer::default parameter candidates.

This module is deterministic: it scores candidate metrics and applies hard gates.
It is intentionally not an LLM call.  "AI judge" here means automated model/
statistics based judgement, with hard gates overriding the score.
"""
from __future__ import annotations

from typing import Any

FORBIDDEN_GOVERNANCE_KEYS = {
    'MODEL_MIN_OOT_PF', 'MODEL_MIN_OOT_HIT_RATE', 'MODEL_MIN_PROMOTION_SCORE',
    'LIVE_ONLY_USE_PROMOTED_MODEL', 'LIVE_REQUIRE_PROMOTED_MODEL',
    'MODEL_BLOCK_LIVE_ON_UNPROMOTED', 'KILL_SWITCH', 'MAX_DRAWDOWN_LIMIT',
}


def _num(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _score01(x: Any, floor: float = 0.0, cap: float = 1.0) -> float:
    v = _num(x, 0.0)
    return max(0.0, min((v - floor) / max(cap - floor, 1e-9), 1.0))


def judge_candidate(candidate: dict[str, Any], baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    metrics = candidate.get('metrics', {}) or {}
    params = candidate.get('params', {}) or {}
    oot = metrics.get('out_of_time', {}) or metrics.get('oot', {}) or {}
    wf = metrics.get('walk_forward', {}) or {}
    baseline_score = _num(metrics.get('baseline_optimizer_score', (baseline or {}).get('optimizer_score', 0.0)), 0.0)
    optimizer_score = _num(metrics.get('optimizer_score', metrics.get('overall_score', 0.0)), 0.0)
    overall_score = _num(metrics.get('overall_score', 0.0), 0.0)
    pf = _num(oot.get('profit_factor', 0.0), 0.0)
    hit = _num(oot.get('hit_rate', 0.0), 0.0)
    wf_ret = _num(wf.get('ret_mean', wf.get('avg_return', 0.0)), 0.0)
    wf_eff = _num(wf.get('effective_splits', 0), 0.0)
    hard_failures = list(metrics.get('hard_failures', []) or [])

    reasons: list[str] = []
    forbidden = sorted([k for k in params.keys() if k in FORBIDDEN_GOVERNANCE_KEYS])
    for key in forbidden:
        hard_failures.append(f'forbidden_governance_key:{key}')
    if optimizer_score < baseline_score:
        hard_failures.append('optimizer_score_below_baseline')
    if pf <= 0:
        hard_failures.append('oot_profit_factor_missing_or_zero')
    if wf_eff < 3:
        hard_failures.append('walk_forward_effective_splits_below_floor')

    oot_score = _score01(pf, 0.8, 1.5) * 0.55 + _score01(hit, 0.45, 0.62) * 0.45
    wf_score = _score01(wf_eff, 2, 6) * 0.5 + _score01(wf_ret, -0.01, 0.03) * 0.5
    overfit_control = 1.0 if 'overfit' not in ' '.join(hard_failures).lower() else 0.25
    baseline_improvement = _score01(optimizer_score - baseline_score, 0.0, 5.0)
    selected_feature_count = _num(metrics.get('selected_feature_count', 0), 0)
    train_live_parity = 1.0
    if selected_feature_count <= 0:
        train_live_parity = 0.0

    score = 100.0 * (
        0.25 * oot_score
        + 0.20 * wf_score
        + 0.15 * overfit_control
        + 0.15 * baseline_improvement
        + 0.10 * 0.8   # cost robustness placeholder; optimizer candidates do not change cost policy
        + 0.10 * 0.7   # regime coverage placeholder; full regime evidence comes later
        + 0.05 * train_live_parity
    )

    hard_gate_pass = len(set(hard_failures)) == 0
    recommended_status = 'approved_for_research' if hard_gate_pass and score >= 75.0 else 'rejected'
    if hard_gate_pass:
        reasons.append('trainer candidate passed OOT/WF/baseline hard gates')
    else:
        reasons.extend(sorted(set(hard_failures)))
    return {
        'enabled': True,
        'ai_score': round(score, 4),
        'hard_gate_pass': hard_gate_pass,
        'recommended_status': recommended_status,
        'reason': reasons,
        'detail': {
            'optimizer_score': optimizer_score,
            'baseline_optimizer_score': baseline_score,
            'overall_score': overall_score,
            'oot_profit_factor': pf,
            'oot_hit_rate': hit,
            'wf_effective_splits': wf_eff,
            'wf_ret_mean': wf_ret,
            'selected_feature_count': selected_feature_count,
        },
    }
