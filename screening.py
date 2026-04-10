# -*- coding: utf-8 -*-
"""Level-1 legacy bridge for screening.

第一級橋接整合：保留 screening.py 舊門牌，
主線改由 fts_market_data_service / fts_feature_service / fts_chip_enrichment_service / fts_screening_engine 提供。
同時補回 advanced_chart(1).zip 中常被舊模組引用的 helper 名稱，降低相容性風險。
"""
from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

try:
    from config import PARAMS  # type: ignore
except Exception:
    PARAMS = {}

from fts_market_data_service import MarketDataService
from fts_feature_service import FeatureService
from fts_chip_enrichment_service import ChipEnrichmentService
from fts_screening_engine import ScreeningEngine
from fts_screening_legacy_compat import (
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

BRIDGE_LEVEL = 'level_1'
BRIDGE_TARGET = 'fts_* screening services'
LEGACY_SOURCE = 'advanced_chart(1).zip::screening.py'

_market = MarketDataService()
_feature = FeatureService()
_chip = ChipEnrichmentService()
_engine = ScreeningEngine()


def normalize_ticker_symbol(ticker: str, default_suffix: str = '.TW') -> str:
    return _market.normalize_ticker_symbol(ticker, default_suffix=default_suffix)


def smart_download(ticker: str, period: str = '1y') -> pd.DataFrame:
    return _market.smart_download(ticker, period=period)


def extract_ai_features(row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, Any]:
    return _feature.extract_ai_features(row, history_df=history_df)


def add_chip_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    return _chip.add_chip_data(df, ticker)


def inspect_stock(ticker: str, preloaded_df: pd.DataFrame | None = None, p: dict[str, Any] | None = None) -> dict[str, Any] | None:
    return _engine.inspect_stock(ticker, preloaded_df=preloaded_df, p=p)


__all__ = [
    'BRIDGE_LEVEL', 'BRIDGE_TARGET', 'LEGACY_SOURCE',
    'normalize_ticker_symbol', 'smart_download', 'extract_ai_features', 'add_chip_data', 'inspect_stock',
    'add_fundamental_filter', 'apply_slippage', 'get_exit_price', 'get_tp_price', 'calculate_pnl',
    '_get_score_weights', '_apply_weighted_scores', '_assign_golden_type', '_compute_realized_signal_stats',
]
