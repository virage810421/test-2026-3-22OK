# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_sector_service import SectorService
from fts_utils import log, now_str
from fts_training_universe_common import (
    safe_read_first_csv,
    safe_read_first_sql,
    normalize_ticker,
    infer_market,
    infer_is_etf,
    full_refresh_table,
    write_json,
)


class StockMasterService:
    MODULE_VERSION = 'v87_stock_master_service'

    def __init__(self):
        self.csv_path = PATHS.runtime_dir / 'stock_master.csv'
        self.summary_path = PATHS.runtime_dir / 'stock_master_service.json'
        self.sector_service = SectorService()

    def _load_seed_df(self) -> pd.DataFrame:
        sql_queries = [
            "SELECT DISTINCT [Ticker SYMBOL], [公司名稱], [產業類別], [產業類別名稱] FROM dbo.stock_revenue_industry_tw",
            "SELECT DISTINCT [Ticker SYMBOL], [公司名稱], [產業類別], [產業類別名稱] FROM dbo.monthly_revenue_simple",
            "SELECT DISTINCT [Ticker SYMBOL], [公司名稱], [產業類別], [產業類別名稱] FROM dbo.fundamentals_clean",
        ]
        df = safe_read_first_sql(sql_queries)
        if not df.empty:
            return df
        csvs = [
            PATHS.base_dir / 'latest_monthly_revenue_with_industry.csv',
            PATHS.data_dir / 'latest_monthly_revenue_with_industry.csv',
            PATHS.base_dir / 'latest_monthly_revenue_master.csv',
            PATHS.base_dir / 'stock_list_cache_listed.csv',
            PATHS.base_dir / 'stock_list_cache.csv',
        ]
        return safe_read_first_csv(csvs)

    def build(self, sync_sql: bool = True) -> tuple[Path, dict[str, Any]]:
        df = self._load_seed_df()
        rows = []
        if not df.empty:
            ticker_col = 'Ticker SYMBOL' if 'Ticker SYMBOL' in df.columns else ('ticker' if 'ticker' in df.columns else None)
            name_col = '公司名稱' if '公司名稱' in df.columns else ('company_name' if 'company_name' in df.columns else None)
            ind_code_col = '產業類別' if '產業類別' in df.columns else ('industry' if 'industry' in df.columns else None)
            ind_name_col = '產業類別名稱' if '產業類別名稱' in df.columns else ('industry_name' if 'industry_name' in df.columns else None)
            for _, row in df.iterrows():
                ticker = normalize_ticker(row.get(ticker_col, '') if ticker_col else '')
                if not ticker:
                    continue
                name = str(row.get(name_col, '') if name_col else '').strip()
                ind_code = str(row.get(ind_code_col, '') if ind_code_col else '').strip()
                ind_name = str(row.get(ind_name_col, '') if ind_name_col else '').strip()
                rows.append({
                    'Ticker SYMBOL': ticker,
                    '公司名稱': name,
                    '市場別': infer_market(ticker),
                    '產業類別': ind_code,
                    '產業類別名稱': ind_name,
                    '是否停牌': 0,
                    '是否下市': 0,
                    '是否ETF': infer_is_etf(ticker, name),
                    '是否普通股': 0 if infer_is_etf(ticker, name) else 1,
                    'SectorBucket': self.sector_service.get_stock_sector(ticker),
                    '來源': 'sql_or_csv_seed',
                    '更新時間': pd.Timestamp.now(),
                })
        out = pd.DataFrame(rows)
        if out.empty:
            try:
                import config  # type: ignore
                tickers = list(dict.fromkeys(getattr(config, 'WATCH_LIST', []) + getattr(config, 'TRAINING_POOL', [])))
            except Exception:
                tickers = []
            out = pd.DataFrame([
                {
                    'Ticker SYMBOL': t,
                    '公司名稱': '',
                    '市場別': infer_market(t),
                    '產業類別': '',
                    '產業類別名稱': '',
                    '是否停牌': 0,
                    '是否下市': 0,
                    '是否ETF': infer_is_etf(t),
                    '是否普通股': 0 if infer_is_etf(t) else 1,
                    'SectorBucket': self.sector_service.get_stock_sector(t),
                    '來源': 'config_fallback',
                    '更新時間': pd.Timestamp.now(),
                }
                for t in tickers
            ])
        out = out.sort_values(['Ticker SYMBOL']).drop_duplicates(['Ticker SYMBOL'], keep='last').reset_index(drop=True)
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        sql_sync = full_refresh_table('stock_master', out) if sync_sql else {'status': 'skip'}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': 'stock_master_ready' if not out.empty else 'stock_master_fallback_only',
            'csv_path': str(self.csv_path),
            'row_count': int(len(out)),
            'sector_counts': {str(k): int(v) for k, v in out['SectorBucket'].value_counts().to_dict().items()} if 'SectorBucket' in out.columns else {},
            'sql_sync': sql_sync,
        }
        write_json(self.summary_path, payload)
        log(f'🧭 stock master ready: {self.csv_path}')
        return self.csv_path, payload

    def load(self) -> pd.DataFrame:
        if self.csv_path.exists():
            try:
                return pd.read_csv(self.csv_path, encoding='utf-8-sig')
            except Exception:
                return pd.read_csv(self.csv_path)
        self.build(sync_sql=False)
        if self.csv_path.exists():
            return pd.read_csv(self.csv_path, encoding='utf-8-sig')
        return pd.DataFrame()
