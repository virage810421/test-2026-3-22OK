# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_utils import safe_float


def passes_signal_gate(row: dict[str, Any], trigger_score: float = 2.0) -> tuple[bool, str]:
    kelly_pct = safe_float(row.get('Kelly_Pos', 0.0), 0.0)
    weighted_buy = safe_float(row.get('Weighted_Buy_Score', 0.0), 0.0)
    weighted_sell = safe_float(row.get('Weighted_Sell_Score', 0.0), 0.0)
    score_gap = safe_float(row.get('Score_Gap', 0.0), 0.0)
    ai_proba = safe_float(row.get('AI_Proba', 0.0), 0.0)
    realized_ev = safe_float(row.get('Realized_EV', 0.0), 0.0)
    health = str(row.get('Health', 'KEEP')).upper()

    if kelly_pct <= 0:
        return False, 'Kelly 倉位為 0'
    if health == 'KILL':
        return False, '策略健康度阻斷'
    if realized_ev <= 0:
        return False, 'Realized EV <= 0'
    if ai_proba < 0.50:
        return False, 'AI 勝率不足'
    if score_gap <= 0:
        return False, '加權分數差為負'
    if weighted_buy < max(2.0, trigger_score):
        return False, '多方加權分數不足'
    if weighted_sell >= weighted_buy:
        return False, '空方壓力未解除'
    return True, '通過訊號閘門'
