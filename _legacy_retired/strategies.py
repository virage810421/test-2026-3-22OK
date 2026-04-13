# -*- coding: utf-8 -*-
"""Legacy facade for strategies.

核心主線禁止依賴本檔；請直接走 fts_strategy_policy_layer / fts_model_layer。
"""
from __future__ import annotations

import warnings

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

LEGACY_FACADE = True
SERVICE_ENTRYPOINT = 'fts_strategy_policy_layer'


def evaluate_ai_signal(latest_row, regime, strategy_config):
    warnings.warn('strategies.py 已退役為 legacy facade；新主線請改用 fts_model_layer / fts_strategy_policy_layer。', DeprecationWarning, stacklevel=2)
    decision = evaluate_model_signal(
        latest_row,
        regime,
        min_proba=float(strategy_config.get('Min_Proba', 0.5)),
        base_multiplier=float(strategy_config.get('Multiplier', 1.0)),
    )
    return float(decision.conviction_multiplier)
