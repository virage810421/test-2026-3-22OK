# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_utils import safe_float


def _infer_direction(row: dict[str, Any]) -> str:
    text = ' '.join(str(row.get(k, '')) for k in ['Direction', 'Golden_Type', 'Structure', 'Regime']).upper()
    if 'SHORT' in text or '空' in text:
        return 'SHORT'
    if 'RANGE' in text or '區間' in text:
        return 'RANGE'
    return 'LONG'


def evaluate_signal_gate(row: dict[str, Any], trigger_score: float = 2.0) -> dict[str, Any]:
    kelly_pct = safe_float(row.get('Kelly_Pos', 0.0), 0.0)
    weighted_buy = safe_float(row.get('Weighted_Buy_Score', 0.0), 0.0)
    weighted_sell = safe_float(row.get('Weighted_Sell_Score', 0.0), 0.0)
    score_gap = safe_float(row.get('Score_Gap', 0.0), 0.0)
    ai_proba = safe_float(row.get('AI_Proba', 0.0), 0.0)
    realized_ev = safe_float(row.get('Realized_EV', 0.0), 0.0)
    health = str(row.get('Health', 'KEEP')).upper()
    direction = _infer_direction(row)

    blockers: list[str] = []
    warnings: list[str] = []
    diagnostics: list[str] = []

    if kelly_pct <= 0:
        blockers.append('kelly_zero')
    if health in {'KILL', 'BLOCKED'}:
        blockers.append('health_blocked')
    if health in {'REVIEW_REQUIRED', 'FALLBACK_BUILD'}:
        blockers.append('manual_review_required')
    if realized_ev <= 0:
        blockers.append('non_positive_ev')
    if ai_proba < 0.50:
        blockers.append('ai_probability_below_threshold')

    # 分數現在只做診斷與方向一致性，不再當主硬閘門。
    if direction == 'LONG':
        diagnostics.append(f'weighted_buy={weighted_buy:.3f}')
        diagnostics.append(f'weighted_sell={weighted_sell:.3f}')
        diagnostics.append(f'score_gap={score_gap:.3f}')
        if weighted_sell > weighted_buy and ai_proba < 0.55:
            blockers.append('directional_score_conflict_long')
        elif weighted_sell > weighted_buy:
            warnings.append('directional_score_conflict_long_but_model_override')
    elif direction == 'SHORT':
        diagnostics.append(f'weighted_sell={weighted_sell:.3f}')
        diagnostics.append(f'weighted_buy={weighted_buy:.3f}')
        diagnostics.append(f'score_gap={score_gap:.3f}')
        if weighted_buy > weighted_sell and ai_proba < 0.55:
            blockers.append('directional_score_conflict_short')
        elif weighted_buy > weighted_sell:
            warnings.append('directional_score_conflict_short_but_model_override')
    else:
        diagnostics.append(f'range_score_gap={score_gap:.3f}')
        if abs(score_gap) > max(trigger_score, 2.0) and ai_proba < 0.55:
            warnings.append('range_setup_score_tilted_to_trend')

    passed = len(blockers) == 0
    note_parts = []
    if blockers:
        note_parts.append('BLOCK:' + ','.join(blockers))
    if warnings:
        note_parts.append('WARN:' + ','.join(warnings))
    if diagnostics:
        note_parts.append('DIAG:' + ','.join(diagnostics))

    return {
        'passed': passed,
        'direction': direction,
        'blockers': blockers,
        'warnings': warnings,
        'diagnostics': diagnostics,
        'note': ' | '.join(note_parts) if note_parts else '通過訊號閘門',
        'core_driver': 'model_ev_kelly_first',
        'heuristic_role': 'diagnostic_only',
    }


def passes_signal_gate(row: dict[str, Any], trigger_score: float = 2.0) -> tuple[bool, str]:
    result = evaluate_signal_gate(row, trigger_score=trigger_score)
    return bool(result['passed']), str(result['note'])
