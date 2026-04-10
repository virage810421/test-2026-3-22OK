# -*- coding: utf-8 -*-
"""Compatibility bridge.
策略層已拆到 fts_strategy_policy_layer.py；模型層已拆到 fts_model_layer.py。
本檔保留舊介面，避免舊呼叫端中斷。
"""
from __future__ import annotations

from fts_model_layer import AI_MODELS, SELECTED_FEATURES, STRICT_PARITY, evaluate_model_signal
from fts_strategy_policy_layer import (
    BaseStrategy,
    DefensiveStrategy,
    MeanReversionStrategy,
    TrendBreakoutStrategy,
    describe_strategy_policy,
    get_active_strategy,
    get_strategy_policy,
)


def evaluate_ai_signal(latest_row, regime, strategy_config):
    decision = evaluate_model_signal(
        latest_row,
        regime,
        min_proba=float(strategy_config.get('Min_Proba', 0.5)),
        base_multiplier=float(strategy_config.get('Multiplier', 1.0)),
    )
    return float(decision.conviction_multiplier)
