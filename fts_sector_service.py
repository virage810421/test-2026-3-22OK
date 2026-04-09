# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import now_str, log

try:
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover
    pyodbc = None

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)

CYCLICAL_TICKERS = {'2409.TW', '3481.TW', '6116.TW', '2344.TW', '2408.TW', '2337.TW'}


def _map_industry_to_sector(ticker: str, ind_name: str) -> str:
    if ticker in CYCLICAL_TICKERS:
        return 'CYCLICAL'
    if ind_name in ['金融保險業']:
        return 'FINANCE'
    if ind_name in ['航運業']:
        return 'SHIPPING'
    if ind_name in ['生技醫療業', '農業科技業']:
        return 'BIO'
    if ind_name in ['半導體業', '電腦及週邊設備業', '光電業', '通信網路業', '電子零組件業', '電子通路業', '資訊服務業', '其他電子業', '數位雲端']:
        return 'TECH'
    return 'OTHERS'


class SectorService:
    MODULE_VERSION = 'v83_sector_service'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'sector_service.json'
        self._cache: dict[str, str] | None = None

    def load_sector_map(self) -> dict[str, str]:
        if self._cache is not None:
            return self._cache
        cache: dict[str, str] = {}
        if pyodbc is not None:
            try:
                with pyodbc.connect(DB_CONN_STR) as conn:  # type: ignore
                    df = pd.read_sql('SELECT DISTINCT [Ticker SYMBOL], [產業類別名稱] FROM stock_revenue_industry_tw WHERE [產業類別名稱] IS NOT NULL', conn)
                    for _, row in df.iterrows():
                        ticker = str(row.get('Ticker SYMBOL', '')).strip()
                        ind_name = str(row.get('產業類別名稱', '')).strip()
                        if ticker:
                            cache[ticker] = _map_industry_to_sector(ticker, ind_name)
            except Exception:
                pass
        if not cache:
            for candidate in [PATHS.base_dir / 'latest_monthly_revenue_with_industry.csv', PATHS.data_dir / 'latest_monthly_revenue_with_industry.csv', PATHS.base_dir / 'stock_revenue_industry_tw.csv']:
                if candidate.exists():
                    try:
                        df = pd.read_csv(candidate, encoding='utf-8-sig')
                    except Exception:
                        try:
                            df = pd.read_csv(candidate)
                        except Exception:
                            continue
                    ticker_col = 'Ticker SYMBOL' if 'Ticker SYMBOL' in df.columns else ('ticker' if 'ticker' in df.columns else None)
                    ind_col = '產業類別名稱' if '產業類別名稱' in df.columns else ('industry_name' if 'industry_name' in df.columns else None)
                    if not ticker_col or not ind_col:
                        continue
                    for _, row in df.iterrows():
                        ticker = str(row.get(ticker_col, '')).strip()
                        ind_name = str(row.get(ind_col, '')).strip()
                        if ticker:
                            cache[ticker] = _map_industry_to_sector(ticker, ind_name)
                    if cache:
                        break
        self._cache = cache
        return cache

    def get_stock_sector(self, ticker: str) -> str:
        return self.load_sector_map().get(str(ticker).strip(), 'OTHERS')

    def classify_tickers(self, ticker_list: list[str]) -> dict[str, str]:
        cache = self.load_sector_map()
        return {str(t).strip(): cache.get(str(t).strip(), 'OTHERS') for t in ticker_list}

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        cache = self.load_sector_map()
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'mapped_ticker_count': len(cache),
            'service_entrypoints': ['load_sector_map', 'get_stock_sector', 'classify_tickers'],
            'status': 'wave2_sector_service_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🏷️ sector service ready: {self.runtime_path}')
        return self.runtime_path, payload
