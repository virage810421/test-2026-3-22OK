# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        data_dir = base_dir / 'data'
        runtime_dir = base_dir / 'runtime'
    PATHS = _Paths()

try:
    from fts_utils import now_str, log  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')
    def log(msg: str) -> None:
        print(msg)

from fts_market_data_service import MarketDataService


class CrossSectionalPercentileService:
    MODULE_VERSION = 'v83_cross_sectional_percentile_full'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'cross_sectional_percentile_service.json'
        self.snapshot_path = PATHS.data_dir / 'feature_cross_section_snapshot.csv'
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)
        Path(PATHS.data_dir).mkdir(parents=True, exist_ok=True)
        self.market = MarketDataService()

    @staticmethod
    def _read_csv_if_exists(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()

    def _load_revenue_df(self) -> pd.DataFrame:
        for p in [PATHS.data_dir / 'latest_monthly_revenue_with_industry.csv', PATHS.base_dir / 'seed_data' / 'latest_monthly_revenue_with_industry.csv']:
            df = self._read_csv_if_exists(p)
            if not df.empty:
                return df
        return pd.DataFrame()

    def _load_chip_df(self) -> pd.DataFrame:
        for p in [PATHS.data_dir / 'chip_percentile_input.csv', PATHS.data_dir / 'daily_chip_snapshot.csv']:
            df = self._read_csv_if_exists(p)
            if not df.empty:
                return df
        return pd.DataFrame()

    @staticmethod
    def _norm_ticker(v: Any) -> str:
        s = str(v or '').strip()
        return s.replace('.TW', '').replace('.TWO', '') if s else ''

    def load_universe(self) -> list[str]:
        names: list[str] = []
        for p in [PATHS.data_dir / 'training_bootstrap_universe.csv', PATHS.data_dir / 'paper_execution_watchlist.csv']:
            df = self._read_csv_if_exists(p)
            if df.empty:
                continue
            for c in ['Ticker SYMBOL', 'ticker', 'Ticker', 'symbol', 'stock_id']:
                if c in df.columns:
                    names.extend([self._norm_ticker(v) for v in df[c].tolist()])
                    break
        rev = self._load_revenue_df()
        if not rev.empty:
            for c in ['Ticker SYMBOL', 'ticker', 'stock_id', '代號']:
                if c in rev.columns:
                    names.extend([self._norm_ticker(v) for v in rev[c].tolist()])
                    break
        for p in [PATHS.data_dir / 'stock_list_cache_listed.csv', PATHS.base_dir / 'stock_list_cache_listed.csv']:
            df = self._read_csv_if_exists(p)
            if df.empty:
                continue
            for c in ['Ticker SYMBOL', 'Code', '代號', 'ticker', 'stock_id']:
                if c in df.columns:
                    names.extend([self._norm_ticker(v) for v in df[c].tolist()])
                    break
        names = sorted({n for n in names if n and n.isdigit()})
        return names or ['2330', '2317', '2454', '2603', '2881']

    @staticmethod
    def _pct(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors='coerce')
        return s.rank(pct=True, method='average').fillna(0.5) if s.notna().sum() else pd.Series(0.5, index=series.index)

    def build_snapshot(self, tickers: Iterable[str] | None = None, period: str = '1y') -> tuple[Path, dict[str, Any]]:
        tickers = list(tickers or self.load_universe())
        revenue = self._load_revenue_df()
        chip = self._load_chip_df()
        market_df = self.market.smart_download('^TWII', period=period)
        market_ret20 = float(market_df['Close'].pct_change(20).iloc[-1]) if (not market_df.empty and 'Close' in market_df.columns and len(market_df) > 21) else 0.0
        from fts_feature_service import FeatureService
        fs = FeatureService()
        rows: list[dict[str, Any]] = []
        for bare in tickers:
            full = self.market.normalize_ticker_symbol(bare)
            hist = self.market.smart_download(full, period=period)
            if hist.empty or 'Close' not in hist.columns or len(hist) < 40:
                continue
            enriched = fs.enrich_from_history(hist)
            latest = enriched.iloc[-1]
            ret20 = float(hist['Close'].pct_change(20).iloc[-1]) if len(hist) > 21 else 0.0
            sector_name = 'Unknown'
            revenue_yoy = 0.0
            if not revenue.empty:
                tcol = next((c for c in ['Ticker SYMBOL', 'ticker', 'stock_id', '代號'] if c in revenue.columns), None)
                if tcol:
                    rsub = revenue[revenue[tcol].astype(str).map(self._norm_ticker) == bare]
                    if not rsub.empty:
                        rlast = rsub.iloc[-1]
                        sector_name = str(rlast.get('產業類別名稱', rlast.get('industry_name', 'Unknown')) or 'Unknown')
                        revenue_yoy = float(pd.to_numeric(pd.Series([rlast.get('營收年增率(%)', rlast.get('Revenue_YoY', 0.0))]), errors='coerce').fillna(0.0).iloc[0])
            total_ratio = 0.0
            if not chip.empty:
                tcol = next((c for c in ['Ticker SYMBOL', 'ticker', 'stock_id', '代號'] if c in chip.columns), None)
                if tcol:
                    csub = chip[chip[tcol].astype(str).map(self._norm_ticker) == bare]
                    if not csub.empty:
                        total_ratio = float(pd.to_numeric(pd.Series([csub.iloc[-1].get('Total_Ratio', 0.0)]), errors='coerce').fillna(0.0).iloc[0])
            rows.append({'Ticker SYMBOL': bare, 'Ticker Full': full, 'Sector': sector_name, 'Close': float(latest.get('Close', hist['Close'].iloc[-1])), 'Return_20': ret20, 'RS_vs_Market_20': ret20 - market_ret20, 'ATR_Pct': float(latest.get('ATR_Pct', 0.0)), 'RealizedVol_20': float(latest.get('RealizedVol_20', 0.0)), 'Turnover_Proxy': float(latest.get('Turnover_Proxy', 0.0)), 'ADV20_Proxy': float(latest.get('ADV20_Proxy', 0.0)), 'Revenue_YoY': revenue_yoy, 'Chip_Total_Ratio': total_ratio})
        snap = pd.DataFrame(rows)
        if snap.empty:
            payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'snapshot_path': str(self.snapshot_path), 'ticker_count': 0, 'status': 'snapshot_empty'}
            self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.runtime_path, payload
        sector_ret = snap.groupby('Sector')['Return_20'].median().rename('Sector_Return_20')
        snap = snap.merge(sector_ret, on='Sector', how='left')
        snap['RS_vs_Sector_20'] = snap['Return_20'] - snap['Sector_Return_20'].fillna(0.0)
        for src, dst in [('RS_vs_Market_20', 'RS_vs_Market_20_Pctl'), ('Revenue_YoY', 'Revenue_YoY_Pctl'), ('Chip_Total_Ratio', 'Chip_Total_Ratio_Pctl'), ('Turnover_Proxy', 'Turnover_Pctl'), ('ADV20_Proxy', 'ADV20_Pctl'), ('ATR_Pct', 'ATR_Pct_Pctl'), ('RealizedVol_20', 'RealizedVol_20_Pctl')]:
            snap[dst] = self._pct(snap[src])
        snap['RS_vs_Sector_20_Pctl'] = snap.groupby('Sector')['RS_vs_Sector_20'].transform(lambda s: self._pct(s))
        snap.to_csv(self.snapshot_path, index=False, encoding='utf-8-sig')
        payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'snapshot_path': str(self.snapshot_path), 'ticker_count': int(len(snap)), 'sector_count': int(snap['Sector'].nunique()), 'official_percentile_mode': True, 'status': 'cross_sectional_snapshot_ready'}
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'📊 cross-sectional snapshot ready: {self.runtime_path}')
        return self.runtime_path, payload

    def load_snapshot(self) -> pd.DataFrame:
        if not self.snapshot_path.exists():
            self.build_snapshot()
        return self._read_csv_if_exists(self.snapshot_path)

    def enrich_row(self, ticker: str, row: dict[str, Any]) -> dict[str, Any]:
        bare = self._norm_ticker(ticker)
        snap = self.load_snapshot()
        if snap.empty:
            return row
        sub = snap[snap['Ticker SYMBOL'].astype(str).map(self._norm_ticker) == bare]
        if sub.empty:
            return row
        s = sub.iloc[-1].to_dict()
        for col in ['RS_vs_Market_20', 'RS_vs_Sector_20', 'RS_vs_Market_20_Pctl', 'RS_vs_Sector_20_Pctl', 'Revenue_YoY', 'Revenue_YoY_Pctl', 'Chip_Total_Ratio', 'Chip_Total_Ratio_Pctl', 'Turnover_Pctl', 'ADV20_Pctl', 'ATR_Pct_Pctl', 'RealizedVol_20_Pctl', 'Sector']:
            if col in s:
                row[col] = s[col]
        row['Revenue_YoY_Rank'] = row.get('Revenue_YoY_Pctl', row.get('Revenue_YoY_Rank', 0.5))
        row['Chip_Total_Ratio_Rank'] = row.get('Chip_Total_Ratio_Pctl', row.get('Chip_Total_Ratio_Rank', 0.5))
        return row

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        if self.snapshot_path.exists():
            snap = self._read_csv_if_exists(self.snapshot_path)
            payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'snapshot_path': str(self.snapshot_path), 'ticker_count': int(len(snap)), 'official_percentile_mode': True, 'status': 'cross_sectional_snapshot_ready'}
        else:
            _, payload = self.build_snapshot()
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.runtime_path, payload
