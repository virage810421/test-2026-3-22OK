# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime, timedelta
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
    MODULE_VERSION = 'v84_market_data_service_incremental_research_safe'

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

    def get_incremental_history(self, ticker: str, period: str = '2y') -> pd.DataFrame:
        ticker = self.normalize_ticker_symbol(ticker)
        cache_file = self._cache_file(ticker, period)
        today = datetime.now().date()
        existing = self._read_cache(cache_file) if cache_file.exists() else pd.DataFrame()
        if not existing.empty:
            try:
                existing.index = pd.to_datetime(existing.index).normalize()
                existing = existing[existing.index.date < today]
            except Exception:
                existing = pd.DataFrame()
        if existing.empty:
            fresh = self._download_yfinance(ticker, period)
            if not fresh.empty:
                return fresh
            return self._read_cache(cache_file)
        last_completed = pd.to_datetime(existing.index.max()).date()
        start_date = last_completed + timedelta(days=1)
        if start_date > today:
            return existing
        if yf is None:
            return existing
        try:
            new_df = yf.download(ticker, start=start_date.isoformat(), progress=False, auto_adjust=False)
            new_df = new_df.xs(ticker, axis=1, level=1).copy() if isinstance(new_df.columns, pd.MultiIndex) else new_df.copy()
            new_df.dropna(subset=['Close'], inplace=True)
            if new_df.empty:
                return existing
            new_df.index = pd.to_datetime(new_df.index).normalize()
            combined = pd.concat([existing, new_df])
            combined = combined[~combined.index.duplicated(keep='last')].sort_index()
            combined.to_csv(cache_file)
            return combined
        except Exception:
            return existing

    def get_smart_klines(self, ticker_list: list[str] | tuple[str, ...], period: str = '2y') -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        for ticker in ticker_list:
            df = self.get_incremental_history(str(ticker), period=period)
            if not df.empty:
                result[str(ticker)] = df
        return result

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'legacy_screening_dependency': False,
            'service_entrypoints': ['normalize_ticker_symbol', 'smart_download', 'get_incremental_history', 'get_smart_klines'],
            'cache_dir': str(self.cache_dir),
            'incremental_kline_cache_enabled': True,
            'status': 'market_data_service_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'📦 market data service ready: {self.runtime_path}')
        return self.runtime_path, payload
