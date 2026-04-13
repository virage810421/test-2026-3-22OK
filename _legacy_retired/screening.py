# -*- coding: utf-8 -*-
"""Legacy facade for screening.

核心主線禁止依賴本檔；請改走 fts_service_api / fts_* services。
"""
from __future__ import annotations

import warnings
from typing import Any, Mapping

import pandas as pd

from fts_service_api import (
    normalize_ticker_symbol,
    smart_download,
    extract_ai_features,
    add_chip_data,
    inspect_stock,
    add_fundamental_filter,
    apply_slippage,
    get_exit_price,
    get_tp_price,
    calculate_pnl,
    _get_score_weights,
    _apply_weighted_scores,
    _assign_golden_type,
    _compute_realized_signal_stats,
)

LEGACY_FACADE = True
SERVICE_ENTRYPOINT = 'fts_service_api'
LEGACY_SOURCE = 'advanced_chart(1).zip::screening.py'


def _warn():
    warnings.warn('screening.py 已退役為 legacy facade；新主線請改用 fts_service_api / fts_screening_engine。', DeprecationWarning, stacklevel=2)


def extract_ai_features_with_warning(row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, Any]:
    _warn()
    return extract_ai_features(row, history_df=history_df)


__all__ = [
    'LEGACY_FACADE', 'SERVICE_ENTRYPOINT', 'LEGACY_SOURCE',
    'normalize_ticker_symbol', 'smart_download', 'extract_ai_features', 'add_chip_data', 'inspect_stock',
    'add_fundamental_filter', 'apply_slippage', 'get_exit_price', 'get_tp_price', 'calculate_pnl',
    '_get_score_weights', '_apply_weighted_scores', '_assign_golden_type', '_compute_realized_signal_stats',
    'extract_ai_features_with_warning',
]
