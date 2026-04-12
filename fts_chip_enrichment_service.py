# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS, DB
from fts_utils import now_str, log
from fts_sql_table_name_map import sql_table

try:
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover
    pyodbc = None


class ChipEnrichmentService:
    MODULE_VERSION = 'v83_chip_enrichment_service_detached'

    RATIO_COLS = ['Foreign_Ratio', 'Trust_Ratio', 'Total_Ratio']
    CONSEC_COLS = ['Foreign_Consecutive', 'Trust_Consecutive']
    TABLE_DAILY_CHIP = sql_table('daily_chip_data')

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'chip_enrichment_service.json'
        self.csv_candidates = [
            PATHS.base_dir / 'daily_chip_data.csv',
            PATHS.data_dir / 'daily_chip_data.csv',
            PATHS.data_dir / 'daily_chip_snapshot.csv',
        ]

    def _db_conn_str(self) -> str:
        return (
            f'DRIVER={{{DB.driver}}};'
            f'SERVER={DB.server};'
            f'DATABASE={DB.database};'
            f'Trusted_Connection={DB.trusted_connection};'
        )

    def _zero_fill(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col in self.RATIO_COLS:
            if col not in out.columns:
                out[col] = 0.0
        for col in self.CONSEC_COLS:
            if col not in out.columns:
                out[col] = 0
        return out

    def _load_from_csv(self, ticker: str) -> pd.DataFrame:
        ticker = str(ticker)
        bare = ticker.split('.')[0]
        for path in self.csv_candidates:
            if not path.exists():
                continue
            try:
                df = pd.read_csv(path, encoding='utf-8-sig')
            except Exception:
                try:
                    df = pd.read_csv(path)
                except Exception:
                    continue
            cols = {c.lower(): c for c in df.columns}
            date_col = cols.get('日期') or cols.get('date')
            ticker_col = cols.get('ticker symbol') or cols.get('ticker') or cols.get('stock_id')
            if not date_col or not ticker_col:
                continue
            subset = df[df[ticker_col].astype(str).str.replace('.TW', '', regex=False).str.replace('.TWO', '', regex=False) == bare].copy()
            if subset.empty:
                continue
            subset[date_col] = pd.to_datetime(subset[date_col], errors='coerce').dt.normalize()
            subset = subset.dropna(subset=[date_col])
            subset = subset.sort_values(date_col).drop_duplicates(subset=[date_col], keep='last')
            subset = subset.set_index(date_col)
            rename = {}
            if '外資買賣超' in subset.columns:
                rename['外資買賣超'] = 'Foreign_Net'
            if '投信買賣超' in subset.columns:
                rename['投信買賣超'] = 'Trust_Net'
            if '自營商買賣超' in subset.columns:
                rename['自營商買賣超'] = 'Dealers_Net'
            subset = subset.rename(columns=rename)
            return subset
        return pd.DataFrame()

    def _load_from_sql(self, ticker: str) -> pd.DataFrame:
        if pyodbc is None:
            return pd.DataFrame()
        bare = str(ticker).split('.')[0]
        query = f"""
        SELECT [日期], [Ticker SYMBOL], [外資買賣超], [投信買賣超], [自營商買賣超]
        FROM {self.TABLE_DAILY_CHIP}
        WHERE [Ticker SYMBOL] LIKE ? OR [Ticker SYMBOL] LIKE ?
        """
        try:
            with pyodbc.connect(self._db_conn_str()) as conn:
                chip_df = pd.read_sql(query, conn, params=(f'{bare}%', f'{bare}%'))
            if chip_df.empty:
                return pd.DataFrame()
            chip_df['日期'] = pd.to_datetime(chip_df['日期'], errors='coerce').dt.normalize()
            chip_df = chip_df.dropna(subset=['日期']).sort_values(['日期', 'Ticker SYMBOL'])
            chip_df = chip_df.drop_duplicates(subset=['日期'], keep='last').set_index('日期')
            return chip_df.rename(columns={'外資買賣超': 'Foreign_Net', '投信買賣超': 'Trust_Net', '自營商買賣超': 'Dealers_Net'})
        except Exception:
            return pd.DataFrame()

    def add_chip_data(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        if getattr(out.index, 'tz', None) is not None:
            out.index = out.index.tz_localize(None)
        out.index = pd.to_datetime(out.index, errors='coerce').normalize()
        out = out[~out.index.isna()].copy()

        chip_df = self._load_from_csv(ticker)
        if chip_df.empty:
            chip_df = self._load_from_sql(ticker)

        if not chip_df.empty:
            for src, dst in [('Foreign_Net', 'Foreign_Net'), ('Trust_Net', 'Trust_Net'), ('Dealers_Net', 'Dealers_Net')]:
                if src in chip_df.columns:
                    out = out.join(chip_df[src].rename(dst), how='left')
            out['Foreign_Net'] = out.get('Foreign_Net', 0).ffill().fillna(0)
            out['Trust_Net'] = out.get('Trust_Net', 0).ffill().fillna(0)
            out['Dealers_Net'] = out.get('Dealers_Net', 0).ffill().fillna(0)
            close = out['Close'].abs().replace(0, pd.NA) if 'Close' in out.columns else pd.Series(index=out.index, dtype='float64')
            out['Foreign_Ratio'] = (out['Foreign_Net'] / close).fillna(0)
            out['Trust_Ratio'] = (out['Trust_Net'] / close).fillna(0)
            out['Total_Ratio'] = (out['Foreign_Net'] + out['Trust_Net'] + out['Dealers_Net']) / close
            out['Total_Ratio'] = out['Total_Ratio'].fillna(0)
            out['Foreign_Consecutive'] = (out['Foreign_Net'].fillna(0) > 0).astype(int).groupby((out['Foreign_Net'].fillna(0) <= 0).astype(int).cumsum()).cumsum()
            out['Trust_Consecutive'] = (out['Trust_Net'].fillna(0) > 0).astype(int).groupby((out['Trust_Net'].fillna(0) <= 0).astype(int).cumsum()).cumsum()
        return self._zero_fill(out)


    def enrich_row(self, ticker: str, row: dict[str, Any]) -> dict[str, Any]:
        out = dict(row or {})
        close_val = pd.to_numeric(pd.Series([out.get('Close')]), errors='coerce').iloc[0] if 'Close' in out else pd.NA

        chip_df = self._load_from_csv(ticker)
        if chip_df.empty:
            chip_df = self._load_from_sql(ticker)

        if chip_df.empty:
            for col in ['Foreign_Net', 'Trust_Net', 'Dealers_Net'] + self.RATIO_COLS + self.CONSEC_COLS:
                out.setdefault(col, 0.0 if 'Ratio' in col else 0)
            return out

        chip_df = chip_df.sort_index().copy()
        latest = chip_df.iloc[-1]

        foreign_net = float(pd.to_numeric(pd.Series([latest.get('Foreign_Net', 0)]), errors='coerce').fillna(0).iloc[0])
        trust_net = float(pd.to_numeric(pd.Series([latest.get('Trust_Net', 0)]), errors='coerce').fillna(0).iloc[0])
        dealers_net = float(pd.to_numeric(pd.Series([latest.get('Dealers_Net', 0)]), errors='coerce').fillna(0).iloc[0])

        def _consecutive_pos(series: pd.Series) -> int:
            s = pd.to_numeric(series, errors='coerce').fillna(0)
            count = 0
            for v in reversed(s.tolist()):
                if v > 0:
                    count += 1
                else:
                    break
            return int(count)

        out['Foreign_Net'] = foreign_net
        out['Trust_Net'] = trust_net
        out['Dealers_Net'] = dealers_net

        if pd.notna(close_val) and float(close_val) != 0:
            close_num = float(close_val)
            out['Foreign_Ratio'] = foreign_net / close_num
            out['Trust_Ratio'] = trust_net / close_num
            out['Total_Ratio'] = (foreign_net + trust_net + dealers_net) / close_num
        else:
            out.setdefault('Foreign_Ratio', 0.0)
            out.setdefault('Trust_Ratio', 0.0)
            out.setdefault('Total_Ratio', 0.0)

        out['Foreign_Consecutive'] = _consecutive_pos(chip_df.get('Foreign_Net', pd.Series(dtype='float64')))
        out['Trust_Consecutive'] = _consecutive_pos(chip_df.get('Trust_Net', pd.Series(dtype='float64')))
        return out

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        demo = pd.DataFrame({'Close': [100.0]}, index=[pd.Timestamp('2026-01-01')])
        enriched = self.add_chip_data(demo, '2330.TW')
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'legacy_screening_dependency': False,
            'chip_columns': [c for c in enriched.columns if c in (self.RATIO_COLS + self.CONSEC_COLS + ['Foreign_Net', 'Trust_Net', 'Dealers_Net'])],
            'status': 'chip_enrichment_service_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧲 chip enrichment service ready: {self.runtime_path}')
        return self.runtime_path, payload
