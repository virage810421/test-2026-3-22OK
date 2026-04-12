# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import log, now_str
from fts_training_universe_common import (
    clip01,
    full_refresh_table,
    latest_per_ticker,
    normalize_ticker,
    safe_read_first_sql,
    score_linear,
    write_json,
)


class CompanyQualitySnapshotService:
    MODULE_VERSION = 'v87_company_quality_snapshot_service'

    def __init__(self):
        self.csv_path = PATHS.runtime_dir / 'company_quality_snapshot.csv'
        self.summary_path = PATHS.runtime_dir / 'company_quality_snapshot_service.json'

    def _load_raw(self) -> pd.DataFrame:
        queries = [
            "SELECT * FROM dbo.fundamentals_clean",
            "SELECT * FROM dbo.fundamental_data",
        ]
        return safe_read_first_sql(queries)

    def build(self, sync_sql: bool = True) -> tuple[Path, dict[str, Any]]:
        raw = self._load_raw()
        if raw.empty:
            payload = {
                'generated_at': now_str(),
                'module_version': self.MODULE_VERSION,
                'status': 'source_missing',
                'csv_path': str(self.csv_path),
                'row_count': 0,
                'sql_sync': {'status': 'skip'},
            }
            write_json(self.summary_path, payload)
            return self.summary_path, payload
        if 'Ticker SYMBOL' not in raw.columns:
            for alt in ['Ticker', 'ticker', 'symbol']:
                if alt in raw.columns:
                    raw['Ticker SYMBOL'] = raw[alt]
                    break
        raw['Ticker SYMBOL'] = raw['Ticker SYMBOL'].map(normalize_ticker)
        raw = latest_per_ticker(raw, 'Ticker SYMBOL', ['資料年月日', 'Date', '資料年月'])
        out = pd.DataFrame()
        out['Ticker SYMBOL'] = raw['Ticker SYMBOL']
        date_col = '資料年月日' if '資料年月日' in raw.columns else ('Date' if 'Date' in raw.columns else None)
        out['資料日期'] = pd.to_datetime(raw[date_col], errors='coerce').fillna(pd.Timestamp.now().normalize()) if date_col else pd.Timestamp.now().normalize()
        metric_map = {
            '單月營收年增率(%)': ['單月營收年增率(%)', '營收年增率(%)', 'Revenue_YoY'],
            '毛利率(%)': ['毛利率(%)', 'Gross_Margin'],
            '營業利益率(%)': ['營業利益率(%)', '營益率(%)', 'Operating_Margin'],
            '單季EPS': ['單季EPS', 'EPS'],
            'ROE(%)': ['ROE(%)', 'ROE'],
            '稅後淨利率(%)': ['稅後淨利率(%)', 'Net_Margin'],
            '負債比率(%)': ['負債比率(%)', 'Debt_Ratio'],
            '本業獲利比(%)': ['本業獲利比(%)', 'Core_Profit_Ratio'],
            '預估殖利率(%)': ['預估殖利率(%)', 'Dividend_Yield'],
        }
        for target, alts in metric_map.items():
            col = next((c for c in alts if c in raw.columns), None)
            out[target] = pd.to_numeric(raw[col], errors='coerce') if col else pd.NA
        out['Revenue_Growth_Score'] = out['單月營收年增率(%)'].apply(lambda v: score_linear(v, -10, 30, default=0.0))
        out['Profitability_Score'] = (
            out['毛利率(%)'].apply(lambda v: score_linear(v, 5, 50))
            + out['營業利益率(%)'].apply(lambda v: score_linear(v, 0, 25))
            + out['ROE(%)'].apply(lambda v: score_linear(v, 5, 25))
            + out['稅後淨利率(%)'].apply(lambda v: score_linear(v, 0, 20))
        ) / 4.0
        out['BalanceSheet_Score'] = (
            out['負債比率(%)'].apply(lambda v: score_linear(v, 30, 80, reverse=True))
            + out['本業獲利比(%)'].apply(lambda v: score_linear(v, 20, 80))
        ) / 2.0
        out['Dividend_Score'] = out['預估殖利率(%)'].apply(lambda v: score_linear(v, 0, 8))
        out['Quality_Total_Score'] = (
            0.25 * out['Revenue_Growth_Score'].fillna(0.0)
            + 0.35 * out['Profitability_Score'].fillna(0.0)
            + 0.25 * out['BalanceSheet_Score'].fillna(0.0)
            + 0.15 * out['Dividend_Score'].fillna(0.0)
        ).clip(lower=0.0, upper=1.0)
        out['資料來源'] = 'fundamentals_clean'
        out = out.sort_values(['Ticker SYMBOL']).reset_index(drop=True)
        out.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        sql_sync = full_refresh_table('company_quality_snapshot', out) if sync_sql else {'status': 'skip'}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': 'quality_snapshot_ready',
            'csv_path': str(self.csv_path),
            'row_count': int(len(out)),
            'sql_sync': sql_sync,
            'score_mean': float(pd.to_numeric(out['Quality_Total_Score'], errors='coerce').fillna(0).mean()) if len(out) else 0.0,
        }
        write_json(self.summary_path, payload)
        log(f'🏛️ company quality snapshot ready: {self.csv_path}')
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
