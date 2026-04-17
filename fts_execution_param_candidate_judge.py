# -*- coding: utf-8 -*-
"""AI-style rule judge for execution_policy::default candidates."""
from __future__ import annotations
from typing import Any
from fts_entry_exit_param_policy import candidate_hard_gate

FORBIDDEN_KEYS = {'KILL_SWITCH', 'LIVE_REQUIRE_PROMOTED_MODEL'}

def _num(x: Any, default: float = 0.0) -> float:
    try: return float(x)
    except Exception: return default

def _score01(x: Any, floor: float = 0.0, cap: float = 1.0) -> float:
    v = _num(x, 0.0)
    return max(0.0, min((v - floor) / max(cap - floor, 1e-9), 1.0))

def judge_candidate(candidate: dict[str, Any], baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    metrics = candidate.get('metrics', {}) or {}
    params = candidate.get('params', {}) or {}
    avg_slip = abs(_num(metrics.get('avg_slippage', 0.0), 0.0))
    reject_rate = _num(metrics.get('order_reject_rate', 0.0), 0.0)
    partial_ok = _num(metrics.get('partial_fill_handling_score', 0.5), 0.5)
    liquidity = _num(metrics.get('liquidity_gate_score', 0.5), 0.5)
    sizing = _num(metrics.get('position_sizing_health', 0.5), 0.5)
    hard = []
    for key in sorted([k for k in params.keys() if k in FORBIDDEN_KEYS]):
        hard.append(f'forbidden_execution_key:{key}')
    if avg_slip > 0.02:
        hard.append('avg_slippage_too_high')
    if reject_rate > 0.10:
        hard.append('order_reject_rate_too_high')
    if liquidity < 0.50:
        hard.append('liquidity_gate_score_too_low')
    score = 100.0 * (
        0.25 * (1.0 - _score01(avg_slip, 0.001, 0.02))
        + 0.20 * (1.0 - _score01(reject_rate, 0.0, 0.10))
        + 0.20 * partial_ok
        + 0.20 * liquidity
        + 0.15 * sizing
    )
    entry_exit_gate = candidate_hard_gate(candidate)
    hard.extend(entry_exit_gate.get('hard_failures', []))
    if entry_exit_gate.get('strictness_health', {}).get('status') == 'too_strict':
        score = min(score, 74.0)
    hard_gate_pass = len(set(hard)) == 0
    recommended_status = 'approved_for_paper' if hard_gate_pass and score >= 75.0 else 'rejected'
    reasons = ['execution policy candidate passed paper-entry hard gates'] if hard_gate_pass else sorted(set(hard))
    return {'enabled': True, 'ai_score': round(score, 4), 'hard_gate_pass': hard_gate_pass, 'recommended_status': recommended_status, 'reason': reasons, 'detail': {'avg_slippage': avg_slip, 'order_reject_rate': reject_rate, 'liquidity_gate_score': liquidity, 'entry_exit_gate': entry_exit_gate}}
