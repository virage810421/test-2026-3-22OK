# -*- coding: utf-8 -*-
"""Legacy-compatible wrapper.

舊模組若仍 import screening.py，現在會反向呼叫 v83 service。
也就是說：舊門牌保留，但 A 主線不再依賴舊 screening 內部實作。
"""
from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from fts_market_data_service import MarketDataService
from fts_feature_service import FeatureService
from fts_chip_enrichment_service import ChipEnrichmentService
from fts_screening_engine import ScreeningEngine

_market = MarketDataService()
_feature = FeatureService()
_chip = ChipEnrichmentService()
_engine = ScreeningEngine()


def normalize_ticker_symbol(ticker: str, default_suffix: str = '.TW') -> str:
    return _market.normalize_ticker_symbol(ticker, default_suffix=default_suffix)


def smart_download(ticker: str, period: str = '1y') -> pd.DataFrame:
    return _market.smart_download(ticker, period=period)


def extract_ai_features(row: Mapping[str, Any]) -> dict[str, Any]:
    return _feature.extract_ai_features(row)


def add_chip_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    return _chip.add_chip_data(df, ticker)


def inspect_stock(ticker: str, preloaded_df: pd.DataFrame | None = None, p: dict[str, Any] | None = None) -> dict[str, Any] | None:
    return _engine.inspect_stock(ticker, preloaded_df=preloaded_df, p=p)
