# -*- coding: utf-8 -*-
from __future__ import annotations

"""Service-first internal API.

核心主線請直接 import 本模組或各正式 service，
不要再反向依賴 screening.py / strategies.py / yahoo_csv_to_sql.py 等 legacy facade。
"""

from typing import Any, Mapping

import pandas as pd

from fts_market_data_service import MarketDataService
from fts_feature_service import FeatureService
from fts_chip_enrichment_service import ChipEnrichmentService
from fts_screening_engine import ScreeningEngine
from fts_strategy_policy_layer import get_active_strategy, get_strategy_policy
from fts_signal_primitives import (
    add_fundamental_filter,
    apply_slippage,
    calculate_pnl,
    get_exit_price,
    get_tp_price,
    _apply_weighted_scores,
    _assign_golden_type,
    _compute_realized_signal_stats,
    _get_score_weights,
)

_MARKET = MarketDataService()
_FEATURE = FeatureService()
_CHIP = ChipEnrichmentService()
_ENGINE = ScreeningEngine()

SERVICE_FIRST = True
LEGACY_IMPORTS_ALLOWED = False


def normalize_ticker_symbol(ticker: str, default_suffix: str = '.TW') -> str:
    return _MARKET.normalize_ticker_symbol(ticker, default_suffix=default_suffix)


def smart_download(ticker: str, period: str = '1y') -> pd.DataFrame:
    return _MARKET.smart_download(ticker, period=period)


def get_incremental_history(ticker: str, period: str = '2y') -> pd.DataFrame:
    return _MARKET.get_incremental_history(ticker, period=period)


def get_smart_klines(ticker_list, period: str = '2y') -> dict[str, pd.DataFrame]:
    return _MARKET.get_smart_klines(ticker_list, period=period)


def extract_ai_features(row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, Any]:
    return _FEATURE.extract_ai_features(row, history_df=history_df)


def add_chip_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    return _CHIP.add_chip_data(df, ticker)


def inspect_stock(ticker: str, preloaded_df: pd.DataFrame | None = None, p: dict[str, Any] | None = None) -> dict[str, Any] | None:
    return _ENGINE.inspect_stock(ticker, preloaded_df=preloaded_df, p=p)


def fundamentals_smart_sync(target_stocks: list[str] | None = None, enable_network_fetch: bool = False, write_sql: bool = True):
    from fts_fundamentals_etl_mainline import FundamentalsETLMainline
    return FundamentalsETLMainline().smart_sync(target_stocks=target_stocks, enable_network_fetch=enable_network_fetch, write_sql=write_sql)


__all__ = [
    'SERVICE_FIRST', 'LEGACY_IMPORTS_ALLOWED',
    'normalize_ticker_symbol', 'smart_download', 'get_incremental_history', 'get_smart_klines',
    'extract_ai_features', 'add_chip_data', 'inspect_stock',
    'get_active_strategy', 'get_strategy_policy',
    'add_fundamental_filter', 'apply_slippage', 'calculate_pnl', 'get_exit_price', 'get_tp_price',
    '_get_score_weights', '_apply_weighted_scores', '_assign_golden_type', '_compute_realized_signal_stats',
    'fundamentals_smart_sync',
]
