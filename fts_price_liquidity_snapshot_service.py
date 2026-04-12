# -*- coding: utf-8 -*-
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from fts_config import PATHS
from fts_utils import log, now_str
from fts_training_universe_common import full_refresh_table, normalize_ticker, score_linear, write_json


def _infer_ticker_from_path(path: Path) -> str:
    name = path.stem.upper().replace('_', '.').replace('-', '.')
    if name.endswith('.TW') or name.endswith('.TWO'):
        return name
    if len(name) >= 4 and name[:4].isdigit():
        return f'{name[:4]}.TW'
    return name


class PriceLiquiditySnapshotService:
    MODULE_VERSION = 'v87_price_liquidity_snapshot_service'

    def __init__(self):
        self.csv_path = PATHS.runtime_dir / 'price_liquidity_daily.csv'
        self.summary_path = PATHS.runtime_dir / 'price_liquidity_snapshot_service.json'

    def _candidate_files(self) -> list[Path]:
        files: list[Path] = []
        for root in PATHS.price_scan_dirs:
            for pattern in ['*.csv', 'kline_cache/*.csv', 'data/kline_cache/*.csv']:
                for path in root.glob(pattern):
                    if path.is_file() and path not in files:
                        files.append(path)
        return files[:3000]

    def _read_one(self, path: Path) -> pd.DataFrame:
        for kwargs in ({'encoding': 'utf-8-sig'}, {}):
            try:
                return pd.read_csv(path, **kwargs)
            except Exception:
                continue
        return pd.DataFrame()

    def build(self, sync_sql: bool = True) -> tuple[Path, dict[str, Any]]:
        rows = []
        for path in self._candidate_files():
            df = self._read_one(path)
            if df.empty:
                continue
            ticker_col = next((c for c in ['Ticker SYMBOL', 'Ticker', 'ticker', 'symbol'] if c in df.columns), None)
            ticker = normalize_ticker(df[ticker_col].iloc[-1] if ticker_col else _infer_ticker_from_path(path))
            if not ticker:
                continue
            date_col = next((c for c in ['Date', 'date', '資料日期'] if c in df.columns), None)
            close_col = next((c for c in ['Close', 'close', '收盤價'] if c in df.columns), None)
            vol_col = next((c for c in ['Volume', 'volume', '成交股數'] if c in df.columns), None)
            high_col = next((c for c in ['High', 'high', '最高價'] if c in df.columns), None)
            low_col = next((c for c in ['Low', 'low', '最低價'] if c in df.columns), None)
            if not close_col or not vol_col:
                continue
            work = df.copy()
            work[close_col] = pd.to_numeric(work[close_col], errors='coerce')
            work[vol_col] = pd.to_numeric(work[vol_col], errors='coerce')
            amount = work[close_col].fillna(0.0) * work[vol_col].fillna(0.0)
            tail20 = work.tail(20)
            adv20 = float(amount.tail(20).mean()) if len(amount) else 0.0
            turnover_ratio = float(tail20[vol_col].iloc[-1] / max(tail20[vol_col].mean(), 1.0)) if len(tail20) else 0.0
            if high_col and low_col:
                work[high_col] = pd.to_numeric(work[high_col], errors='coerce')
                work[low_col] = pd.to_numeric(work[low_col], errors='coerce')
                atr_pct = float(((work[high_col] - work[low_col]).tail(20) / work[close_col].replace(0, np.nan).tail(20)).replace([np.inf, -np.inf], np.nan).mean()) if len(tail20) else 0.0
            else:
                atr_pct = float(work[close_col].pct_change().tail(20).abs().mean()) if len(tail20) else 0.0
            missing_days = int(tail20[[close_col, vol_col]].isna().any(axis=1).sum()) if len(tail20) else 20
            abnormal = int(atr_pct >= 0.09)
            illiquid = int((tail20[vol_col].fillna(0) <= 0).tail(5).all()) if len(tail20) >= 5 else 0
            liquidity_score = max(0.0, min(1.0, 0.65 * score_linear(adv20, 10_000_000, 100_000_000) + 0.20 * score_linear(turnover_ratio, 0.5, 2.5) + 0.15 * score_linear(missing_days, 0, 5, reverse=True)))
            latest_date = pd.to_datetime(work[date_col].iloc[-1], errors='coerce').normalize() if date_col else pd.Timestamp.now().normalize()
            rows.append({
                'Ticker SYMBOL': ticker,
                '資料日期': latest_date,
                'Close': float(work[close_col].dropna().iloc[-1]) if work[close_col].notna().any() else None,
                'Volume': float(work[vol_col].dropna().iloc[-1]) if work[vol_col].notna().any() else None,
                'Amount': float(amount.dropna().iloc[-1]) if amount.notna().any() else None,
                'ADV20': adv20,
                'Turnover_Ratio': turnover_ratio,
                'ATR_Pct': atr_pct,
                '近20日缺資料天數': missing_days,
                '是否異常波動': abnormal,
                '是否連續無量': illiquid,
                'Liquidity_Score': liquidity_score,
            })
        out = pd.DataFrame(rows)
        if not out.empty and 'Ticker SYMBOL' in out.columns:
            out = out.sort_values(['Ticker SYMBOL']).drop_duplicates(['Ticker SYMBOL'], keep='last').reset_index(drop=True)
            out.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        sql_sync = full_refresh_table('price_liquidity_daily', out) if sync_sql and not out.empty else {'status': 'skip_or_empty', 'rows': int(len(out))}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': 'price_liquidity_ready' if not out.empty else 'no_price_csv_found',
            'csv_path': str(self.csv_path),
            'row_count': int(len(out)),
            'sql_sync': sql_sync,
        }
        write_json(self.summary_path, payload)
        log(f'💧 price liquidity snapshot status: {payload["status"]}')
        return self.csv_path if out is not None else self.summary_path, payload

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
