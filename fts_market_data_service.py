# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import now_str, log

try:
    import yfinance as yf  # type: ignore
except Exception:  # pragma: no cover
    yf = None


class MarketDataService:
    MODULE_VERSION = 'v83_market_data_service_detached'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'market_data_service.json'
        self.cache_dir = PATHS.data_dir / 'kline_cache'
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def normalize_ticker_symbol(ticker: str, default_suffix: str = '.TW') -> str:
        ticker = str(ticker or '').strip()
        if not ticker:
            return ticker
        if ticker.endswith('.TW') or ticker.endswith('.TWO'):
            return ticker
        if ticker.isdigit():
            return f'{ticker}{default_suffix}'
        return ticker

    def _cache_file(self, ticker: str, period: str) -> Path:
        safe_name = self.normalize_ticker_symbol(ticker).replace('/', '_')
        return self.cache_dir / f'{safe_name}_{period}.csv'

    def _is_cache_fresh(self, cache_file: Path) -> bool:
        if not cache_file.exists():
            return False
        try:
            today = datetime.now().date()
            file_mtime_date = datetime.fromtimestamp(cache_file.stat().st_mtime).date()
            days_diff = (today - file_mtime_date).days
            return days_diff == 0 or (today.weekday() >= 5 and days_diff <= 3)
        except Exception:
            return False

    def _read_cache(self, cache_file: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(cache_file, index_col=0, parse_dates=True)
        except Exception:
            return pd.DataFrame()

    def _download_yfinance(self, ticker: str, period: str) -> pd.DataFrame:
        if yf is None:
            return pd.DataFrame()
        try:
            data = yf.download(ticker, period=period, progress=False, auto_adjust=False)
            if data.empty:
                return pd.DataFrame()
            df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()
            if not df.empty:
                df.to_csv(self._cache_file(ticker, period))
            return df
        except Exception:
            return pd.DataFrame()

    def smart_download(self, ticker: str, period: str = '1y') -> pd.DataFrame:
        ticker = self.normalize_ticker_symbol(ticker)
        cache_file = self._cache_file(ticker, period)
        if self._is_cache_fresh(cache_file):
            cached = self._read_cache(cache_file)
            if not cached.empty:
                return cached
        fresh = self._download_yfinance(ticker, period)
        if not fresh.empty:
            return fresh
        return self._read_cache(cache_file)

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'legacy_screening_dependency': False,
            'service_entrypoints': ['normalize_ticker_symbol', 'smart_download'],
            'cache_dir': str(self.cache_dir),
            'status': 'market_data_service_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'📦 market data service ready: {self.runtime_path}')
        return self.runtime_path, payload
