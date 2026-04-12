# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import log, now_str
from fts_training_universe_common import full_refresh_table, normalize_ticker, safe_read_first_csv, safe_read_first_sql, score_linear, write_json


class RevenueMomentumSnapshotService:
    MODULE_VERSION = 'v87_revenue_momentum_snapshot_service'

    def __init__(self):
        self.csv_path = PATHS.runtime_dir / 'revenue_momentum_snapshot.csv'
        self.summary_path = PATHS.runtime_dir / 'revenue_momentum_snapshot_service.json'

    def _load_raw(self) -> pd.DataFrame:
        queries = [
            "SELECT * FROM dbo.monthly_revenue_simple",
            "SELECT * FROM dbo.stock_revenue_industry_tw",
        ]
        df = safe_read_first_sql(queries)
        if not df.empty:
            return df
        csvs = [
            PATHS.base_dir / 'latest_monthly_revenue_with_industry.csv',
            PATHS.base_dir / 'latest_monthly_revenue_master.csv',
            PATHS.data_dir / 'latest_monthly_revenue_with_industry.csv',
        ]
        return safe_read_first_csv(csvs)

    def build(self, sync_sql: bool = True) -> tuple[Path, dict[str, Any]]:
        raw = self._load_raw()
        if raw.empty:
            payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'status': 'source_missing', 'row_count': 0}
            write_json(self.summary_path, payload)
            return self.summary_path, payload
        ticker_col = 'Ticker SYMBOL' if 'Ticker SYMBOL' in raw.columns else ('ticker' if 'ticker' in raw.columns else None)
        ym_col = '資料年月' if '資料年月' in raw.columns else ('年月' if '年月' in raw.columns else None)
        yoy_col = None
        for cand in ['單月營收年增率(%)', '營收年增率(%)', 'Revenue_YoY']:
            if cand in raw.columns:
                yoy_col = cand
                break
        if not ticker_col or not yoy_col:
            payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'status': 'required_columns_missing', 'row_count': 0}
            write_json(self.summary_path, payload)
            return self.summary_path, payload
        raw = raw.copy()
        raw['Ticker SYMBOL'] = raw[ticker_col].map(normalize_ticker)
        raw['__yoy'] = pd.to_numeric(raw[yoy_col], errors='coerce')
        if ym_col and ym_col in raw.columns:
            raw['__ym'] = raw[ym_col].astype(str)
            raw['__order'] = pd.to_datetime(raw['__ym'].astype(str).str.replace('/', '-', regex=False) + '-01', errors='coerce')
        else:
            raw['__ym'] = pd.Timestamp.now().strftime('%Y-%m')
            raw['__order'] = pd.Timestamp.now().normalize()
        raw = raw.sort_values(['Ticker SYMBOL', '__order']).reset_index(drop=True)
        rows = []
        for ticker, grp in raw.groupby('Ticker SYMBOL'):
            grp = grp.dropna(subset=['__yoy']).copy()
            if grp.empty:
                continue
            tail6 = grp.tail(6)
            tail3 = grp.tail(3)
            latest = tail6.iloc[-1]
            yoy_latest = float(latest['__yoy']) if pd.notna(latest['__yoy']) else 0.0
            avg3 = float(tail3['__yoy'].mean()) if len(tail3) else yoy_latest
            avg6 = float(tail6['__yoy'].mean()) if len(tail6) else yoy_latest
            accel = avg3 - avg6
            positive_3m = int((tail3['__yoy'] > 0).sum() >= min(3, len(tail3)))
            score = float((0.45 * score_linear(yoy_latest, -10, 30) + 0.35 * score_linear(avg3, -5, 25) + 0.20 * score_linear(accel, -10, 10)).clip(0,1) if hasattr(pd.Series([1]), 'clip') else 0.0)
            rows.append({
                'Ticker SYMBOL': ticker,
                '資料年月': str(latest['__ym']),
                '單月營收年增率(%)': yoy_latest,
                '三月平均年增(%)': avg3,
                '六月平均年增(%)': avg6,
                '營收加速度': accel,
                '是否連續三月正成長': positive_3m,
                '營收動能分數': max(0.0, min(1.0, 0.45 * score_linear(yoy_latest, -10, 30) + 0.35 * score_linear(avg3, -5, 25) + 0.20 * score_linear(accel, -10, 10))),
            })
        out = pd.DataFrame(rows).sort_values(['Ticker SYMBOL']).reset_index(drop=True)
        out.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        sql_sync = full_refresh_table('revenue_momentum_snapshot', out) if sync_sql else {'status': 'skip'}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': 'revenue_momentum_ready' if not out.empty else 'empty',
            'csv_path': str(self.csv_path),
            'row_count': int(len(out)),
            'sql_sync': sql_sync,
        }
        write_json(self.summary_path, payload)
        log(f'📈 revenue momentum snapshot ready: {self.csv_path}')
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
