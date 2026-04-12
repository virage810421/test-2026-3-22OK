# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import log, now_str
from fts_training_universe_common import full_refresh_table, latest_per_ticker, normalize_ticker, safe_read_first_csv, safe_read_first_sql, score_linear, write_json


class ChipFactorSnapshotService:
    MODULE_VERSION = 'v87_chip_factor_snapshot_service'

    def __init__(self):
        self.csv_path = PATHS.runtime_dir / 'chip_factors_daily.csv'
        self.summary_path = PATHS.runtime_dir / 'chip_factor_snapshot_service.json'

    def _load_raw(self) -> pd.DataFrame:
        queries = [
            "SELECT * FROM dbo.chip_factors_daily_source",
            "SELECT * FROM dbo.institutional_chip_daily",
            "SELECT * FROM dbo.stock_chip_daily",
            "SELECT * FROM dbo.daily_chip_data",
        ]
        df = safe_read_first_sql(queries)
        if not df.empty:
            return df
        csvs = [
            PATHS.base_dir / 'daily_chip.csv',
            PATHS.base_dir / 'institutional_chip_daily.csv',
            PATHS.data_dir / 'daily_chip.csv',
        ]
        return safe_read_first_csv(csvs)

    def build(self, sync_sql: bool = True) -> tuple[Path, dict[str, Any]]:
        raw = self._load_raw()
        if raw.empty:
            payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'status': 'source_missing', 'row_count': 0}
            write_json(self.summary_path, payload)
            return self.summary_path, payload
        ticker_col = next((c for c in ['Ticker SYMBOL', 'Ticker', 'ticker', 'symbol'] if c in raw.columns), None)
        if not ticker_col:
            payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'status': 'ticker_missing', 'row_count': 0}
            write_json(self.summary_path, payload)
            return self.summary_path, payload
        raw = raw.copy()
        raw['Ticker SYMBOL'] = raw[ticker_col].map(normalize_ticker)
        raw = latest_per_ticker(raw, 'Ticker SYMBOL', ['資料日期', 'Date'])
        def col(*names: str):
            return next((n for n in names if n in raw.columns), None)
        ext_c = col('外資買賣超', 'foreign_net_buy', 'Foreign_Net')
        inv_c = col('投信買賣超', 'investment_net_buy', 'Investment_Net')
        dealer_c = col('自營商買賣超', 'dealer_net_buy', 'Dealer_Net')
        total_c = col('三大法人合計', '三大法人買賣超', 'institution_total', 'Total_Net')
        conc_c = col('籌碼集中度', 'chip_concentration', 'Concentration')
        bigsmall_c = col('大戶散戶差', 'big_small_diff', 'BigSmallDiff')
        date_c = col('資料日期', 'Date')
        out = pd.DataFrame({'Ticker SYMBOL': raw['Ticker SYMBOL']})
        out['資料日期'] = pd.to_datetime(raw[date_c], errors='coerce').fillna(pd.Timestamp.now().normalize()) if date_c else pd.Timestamp.now().normalize()
        out['外資買賣超'] = pd.to_numeric(raw[ext_c], errors='coerce') if ext_c else 0.0
        out['投信買賣超'] = pd.to_numeric(raw[inv_c], errors='coerce') if inv_c else 0.0
        out['自營商買賣超'] = pd.to_numeric(raw[dealer_c], errors='coerce') if dealer_c else 0.0
        out['三大法人合計'] = pd.to_numeric(raw[total_c], errors='coerce') if total_c else (out['外資買賣超'].fillna(0)+out['投信買賣超'].fillna(0)+out['自營商買賣超'].fillna(0))
        out['籌碼集中度'] = pd.to_numeric(raw[conc_c], errors='coerce') if conc_c else pd.NA
        out['大戶散戶差'] = pd.to_numeric(raw[bigsmall_c], errors='coerce') if bigsmall_c else pd.NA
        out['Chip_Score'] = (
            0.55 * out['三大法人合計'].apply(lambda v: score_linear(v, 0, 50000))
            + 0.25 * out['籌碼集中度'].apply(lambda v: score_linear(v, 0, 100, default=0.5))
            + 0.20 * out['大戶散戶差'].apply(lambda v: score_linear(v, -20, 20, default=0.5))
        ).clip(lower=0.0, upper=1.0)
        out.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        sql_sync = full_refresh_table('chip_factors_daily', out) if sync_sql else {'status': 'skip'}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': 'chip_snapshot_ready',
            'csv_path': str(self.csv_path),
            'row_count': int(len(out)),
            'sql_sync': sql_sync,
        }
        write_json(self.summary_path, payload)
        log(f'🧲 chip factor snapshot ready: {self.csv_path}')
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
