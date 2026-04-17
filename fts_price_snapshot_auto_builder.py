# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import now_str, log, safe_float
from fts_market_data_service import MarketDataService


class AutoPriceSnapshotBuilder:
    MODULE_VERSION = 'v100_auto_price_snapshot_builder'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'price_snapshot_auto_builder.json'
        self.output_path = PATHS.data_dir / 'last_price_snapshot.csv'
        self.market = MarketDataService()

    @staticmethod
    def _normalize_ticker(value: Any) -> str:
        s = str(value or '').strip().upper()
        if not s:
            return ''
        if s.endswith('.TW') or s.endswith('.TWO'):
            return s
        if s.isdigit():
            return f'{s}.TW'
        return s

    def _read_snapshot_csv(self, path: Path) -> pd.DataFrame:
        try:
            df = pd.read_csv(path, encoding='utf-8-sig')
        except Exception:
            try:
                df = pd.read_csv(path)
            except Exception:
                return pd.DataFrame(columns=['Ticker', 'Reference_Price', 'Source'])
        tcol = next((c for c in df.columns if str(c).strip().lower() in {'ticker', 'ticker symbol', 'symbol'}), None)
        pcol = next((c for c in df.columns if str(c).strip().lower() in {'reference_price', 'price', 'close', 'adj close'} or '收盤' in str(c)), None)
        if not tcol or not pcol:
            return pd.DataFrame(columns=['Ticker', 'Reference_Price', 'Source'])
        out = df[[tcol, pcol]].copy()
        out.columns = ['Ticker', 'Reference_Price']
        out['Ticker'] = out['Ticker'].map(self._normalize_ticker)
        out['Reference_Price'] = out['Reference_Price'].map(lambda x: safe_float(x, 0.0))
        out = out[out['Ticker'] != '']
        out = out[out['Reference_Price'] > 0]
        if out.empty:
            return pd.DataFrame(columns=['Ticker', 'Reference_Price', 'Source'])
        out['Source'] = path.name
        return out.drop_duplicates(['Ticker'], keep='first')

    def _load_existing_sources(self) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        candidates = [
            PATHS.data_dir / 'manual_price_snapshot_overrides.csv',
            PATHS.base_dir / 'manual_price_snapshot_overrides.csv',
            PATHS.data_dir / 'last_price_snapshot.csv',
            PATHS.base_dir / 'last_price_snapshot.csv',
            PATHS.base_dir / 'daily_price_snapshot.csv',
        ]
        for path in candidates:
            if path.exists():
                df = self._read_snapshot_csv(path)
                if not df.empty:
                    frames.append(df)
        if not frames:
            return pd.DataFrame(columns=['Ticker', 'Reference_Price', 'Source'])
        out = pd.concat(frames, ignore_index=True)
        return out.drop_duplicates(['Ticker'], keep='first')

    def _collect_tickers(self, tickers: list[str] | None = None) -> list[str]:
        merged: list[str] = []
        for item in tickers or []:
            t = self._normalize_ticker(item)
            if t and t not in merged:
                merged.append(t)
        candidates = [
            PATHS.data_dir / 'normalized_decision_output.csv',
            PATHS.data_dir / 'normalized_decision_output_enriched.csv',
            PATHS.base_dir / 'daily_decision_desk.csv',
            PATHS.data_dir / 'daily_decision_desk.csv',
            PATHS.base_dir / 'daily_decision_desk_prerisk.csv',
            PATHS.data_dir / 'daily_decision_desk_prerisk.csv',
        ]
        for path in candidates:
            if not path.exists():
                continue
            try:
                df = pd.read_csv(path, encoding='utf-8-sig')
            except Exception:
                try:
                    df = pd.read_csv(path)
                except Exception:
                    continue
            col = None
            for name in ('Ticker', 'Ticker SYMBOL', 'ticker', 'symbol'):
                if name in df.columns:
                    col = name
                    break
            if not col:
                continue
            for item in df[col].tolist():
                t = self._normalize_ticker(item)
                if t and t not in merged:
                    merged.append(t)
        return merged

    def build(self, tickers: list[str] | None = None, allow_online: bool = True, period: str = '3mo') -> tuple[Path, dict[str, Any], dict[str, float]]:
        required = self._collect_tickers(tickers)
        existing = self._load_existing_sources()
        existing_map = {str(r['Ticker']): float(r['Reference_Price']) for _, r in existing.iterrows()} if not existing.empty else {}
        rows: list[dict[str, Any]] = []
        missing: list[str] = []
        online_built = 0
        for ticker in required:
            price = safe_float(existing_map.get(ticker, 0.0), 0.0)
            source = 'existing_snapshot'
            if price <= 0:
                price, source = self.market.get_latest_reference_price(ticker, allow_online=allow_online, period=period)
                if price > 0 and source.startswith('online:'):
                    online_built += 1
            if price > 0:
                rows.append({'Ticker': ticker, 'Reference_Price': round(float(price), 4), 'Source': source})
            else:
                missing.append(ticker)
        out = pd.DataFrame(rows, columns=['Ticker', 'Reference_Price', 'Source']) if rows else pd.DataFrame(columns=['Ticker', 'Reference_Price', 'Source'])
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'required_ticker_count': len(required),
            'required_tickers_preview': required[:50],
            'resolved_ticker_count': int(len(out)),
            'missing_ticker_count': int(len(missing)),
            'missing_tickers_preview': missing[:50],
            'used_existing_snapshot_rows': int(len(existing)),
            'online_built_count': int(online_built),
            'output_path': str(self.output_path),
            'status': ('price_snapshot_ready' if rows and not missing else ('price_snapshot_partial' if rows else ('waiting_for_price_sources' if required else 'price_snapshot_missing'))),
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        if rows:
            log(f'💹 auto price snapshot ready: {self.output_path}')
        return self.output_path, payload, {str(r['Ticker']): float(r['Reference_Price']) for _, r in out.iterrows()}
