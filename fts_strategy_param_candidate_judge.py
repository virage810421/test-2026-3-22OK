# -*- coding: utf-8 -*-
"""AI-style rule judge for strategy_signal::default candidates."""
from __future__ import annotations
from typing import Any
from fts_entry_exit_param_policy import candidate_hard_gate

FORBIDDEN_KEYS = {'KILL_SWITCH', 'LIVE_REQUIRE_PROMOTED_MODEL', 'MODEL_MIN_PROMOTION_SCORE'}

def _num(x: Any, default: float = 0.0) -> float:
    try: return float(x)
    except Exception: return default

def _score01(x: Any, floor: float = 0.0, cap: float = 1.0) -> float:
    v = _num(x, 0.0)
    return max(0.0, min((v - floor) / max(cap - floor, 1e-9), 1.0))

def judge_candidate(candidate: dict[str, Any], baseline: dict[str, Any] | None = None) -> dict[str, Any]:
    metrics = candidate.get('metrics', {}) or {}
    params = candidate.get('params', {}) or {}
    pf = _num(metrics.get('profit_factor', metrics.get('PF', metrics.get('Test_PF', 0.0))), 0.0)
    win = _num(metrics.get('win_rate', metrics.get('hit_rate', metrics.get('WinRate', 0.0))), 0.0)
    mdd = abs(_num(metrics.get('max_drawdown', metrics.get('MDD', 0.0)), 0.0))
    trades = _num(metrics.get('trade_count', metrics.get('trades', metrics.get('Signal_Count', 0))), 0.0)
    ev = _num(metrics.get('cost_after_ev', metrics.get('Test_EV', metrics.get('EV', 0.0))), 0.0)
    false_breakout = _num(metrics.get('false_breakout_rate', 0.0), 0.0)
    reasons = []
    hard_failures = []
    for key in sorted([k for k in params.keys() if k in FORBIDDEN_KEYS]):
        hard_failures.append(f'forbidden_strategy_key:{key}')
    if trades < 8:
        hard_failures.append('trade_count_too_low')
    if ev <= 0:
        hard_failures.append('cost_after_ev_not_positive')
    if mdd > 0.25:
        hard_failures.append('max_drawdown_too_high')
    if false_breakout > 0.45:
        hard_failures.append('false_breakout_rate_too_high')
    score = 100.0 * (
        0.20 * _score01(pf, 0.9, 1.6)
        + 0.20 * (1.0 - _score01(mdd, 0.05, 0.25))
        + 0.15 * _score01(trades, 8, 80)
        + 0.15 * _score01(ev, 0.0, 2.0)
        + 0.15 * _score01(metrics.get('regime_coverage', 0.5), 0.3, 1.0)
        + 0.10 * (1.0 - _score01(false_breakout, 0.1, 0.45))
        + 0.05 * _score01(metrics.get('signal_frequency_health', 0.5), 0.0, 1.0)
    )
    entry_exit_gate = candidate_hard_gate(candidate)
    hard_failures.extend(entry_exit_gate.get('hard_failures', []))
    if entry_exit_gate.get('strictness_health', {}).get('status') == 'too_strict':
        score = min(score, 74.0)  # too-strict candidates need more evidence before paper approval
    hard_gate_pass = len(set(hard_failures)) == 0
    recommended_status = 'approved_for_paper' if hard_gate_pass and score >= 75.0 else 'rejected'
    reasons = ['strategy candidate passed paper-entry hard gates'] if hard_gate_pass else sorted(set(hard_failures))
    return {'enabled': True, 'ai_score': round(score, 4), 'hard_gate_pass': hard_gate_pass, 'recommended_status': recommended_status, 'reason': reasons, 'detail': {'profit_factor': pf, 'win_rate': win, 'max_drawdown': mdd, 'trade_count': trades, 'cost_after_ev': ev, 'entry_exit_gate': entry_exit_gate}}
