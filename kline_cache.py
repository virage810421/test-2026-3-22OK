# -*- coding: utf-8 -*-
from __future__ import annotations

from fts_market_data_service import MarketDataService

_service = MarketDataService()


def get_smart_klines(ticker_list, period: str = '2y'):
    """Research-safe wrapper: incremental OHLCV cache only, no live config writes."""
    return _service.get_smart_klines(ticker_list, period=period)
