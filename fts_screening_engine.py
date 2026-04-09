# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import now_str, log
from fts_market_data_service import MarketDataService
from fts_feature_service import FeatureService
from fts_chip_enrichment_service import ChipEnrichmentService


class ScreeningEngine:
    MODULE_VERSION = 'v83_screening_engine_detached'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'screening_engine.json'
        self.market = MarketDataService()
        self.features = FeatureService()
        self.chips = ChipEnrichmentService()

    @staticmethod
    def _rsi(close: pd.Series, window: int = 14) -> pd.Series:
        delta = close.diff().fillna(0)
        gain = delta.clip(lower=0).rolling(window, min_periods=1).mean()
        loss = (-delta.clip(upper=0)).rolling(window, min_periods=1).mean()
        rs = gain / loss.replace(0, pd.NA)
        return (100 - 100 / (1 + rs)).fillna(50)

    @staticmethod
    def _adx(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14) -> pd.Series:
        up_move = high.diff().fillna(0)
        down_move = -low.diff().fillna(0)
        plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
        tr = pd.concat([
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ], axis=1).max(axis=1)
        atr = tr.rolling(window, min_periods=1).mean().replace(0, pd.NA)
        plus_di = 100 * plus_dm.rolling(window, min_periods=1).mean() / atr
        minus_di = 100 * minus_dm.rolling(window, min_periods=1).mean() / atr
        dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA) * 100).fillna(0)
        return dx.rolling(window, min_periods=1).mean().fillna(25)

    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if 'Close' not in out.columns:
            return pd.DataFrame()
        out['MA20'] = out['Close'].rolling(20, min_periods=1).mean()
        out['STD20'] = out['Close'].rolling(20, min_periods=1).std().fillna(0)
        out['BB_Upper'] = out['MA20'] + out['STD20'] * 2
        out['BB_Lower'] = out['MA20'] - out['STD20'] * 2
        out['BB_Width'] = ((out['BB_Upper'] - out['BB_Lower']) / out['MA20'].replace(0, pd.NA)).fillna(0)
        out['Vol_MA20'] = out['Volume'].rolling(20, min_periods=1).mean() if 'Volume' in out.columns else 0
        out['RSI'] = self._rsi(out['Close'])
        ema12 = out['Close'].ewm(span=12, adjust=False).mean()
        ema26 = out['Close'].ewm(span=26, adjust=False).mean()
        out['MACD'] = ema12 - ema26
        out['MACD_Signal'] = out['MACD'].ewm(span=9, adjust=False).mean()
        out['MACD_Hist'] = out['MACD'] - out['MACD_Signal']
        high = out['High'] if 'High' in out.columns else out['Close']
        low = out['Low'] if 'Low' in out.columns else out['Close']
        out['ADX14'] = self._adx(high, low, out['Close'])
        out['Buy_Score'] = ((out['Close'] > out['MA20']).astype(int) + (out['RSI'] > 50).astype(int) + (out['MACD_Hist'] > 0).astype(int))
        out['Sell_Score'] = ((out['Close'] < out['MA20']).astype(int) + (out['RSI'] < 50).astype(int) + (out['MACD_Hist'] < 0).astype(int))
        out['Weighted_Buy_Score'] = out['Buy_Score'].astype(float)
        out['Weighted_Sell_Score'] = out['Sell_Score'].astype(float)
        out['Score_Gap'] = out['Weighted_Buy_Score'] - out['Weighted_Sell_Score']
        out['Signal_Conflict'] = ((out['Buy_Score'] > 0) & (out['Sell_Score'] > 0)).astype(int)
        out['Vol_Squeeze'] = (out['BB_Width'] < out['BB_Width'].rolling(20, min_periods=1).median()).astype(int)
        out['Absorption'] = ((out.get('Total_Ratio', 0) > 0) & (out['Close'].pct_change().fillna(0) < 0)).astype(int)
        out['Fake_Breakout'] = ((out['Close'] > out['BB_Upper']) & (out['MACD_Hist'] < 0))
        out['Bear_Trap'] = ((out['Close'] < out['BB_Lower']) & (out['MACD_Hist'] > 0))
        out['Golden_Type'] = out['Score_Gap'].apply(lambda x: '多方進場' if x > 0 else ('空方進場' if x < 0 else '無'))
        ret20 = out['Close'].pct_change(20).fillna(0)
        out['Regime'] = ret20.apply(lambda x: '趨勢多頭' if x > 0.05 else ('趨勢空頭' if x < -0.05 else '區間盤整'))
        out['AI_Proba'] = (0.5 + out['Score_Gap'] * 0.1).clip(0.0, 1.0)
        out['Realized_EV'] = out['Close'].pct_change(5).shift(-5).fillna(0) * 100
        out['Sample_Size'] = len(out)
        return out

    def inspect_stock(self, ticker: str, preloaded_df: pd.DataFrame | None = None, p: dict[str, Any] | None = None) -> dict[str, Any] | None:
        df = preloaded_df.copy() if isinstance(preloaded_df, pd.DataFrame) else self.market.smart_download(ticker, period='1y')
        if df is None or df.empty:
            return None
        df = self.chips.add_chip_data(df, ticker)
        out = self._prepare(df)
        if out.empty:
            return None
        latest = out.iloc[-1]
        latest_features = self.features.extract_ai_features(latest)
        return {
            'Ticker SYMBOL': self.market.normalize_ticker_symbol(ticker),
            'Structure': str(latest.get('Golden_Type', '無')),
            'Regime': str(latest.get('Regime', '區間盤整')),
            'AI_Proba': round(float(latest.get('AI_Proba', 0.5)), 4),
            'Realized_EV': round(float(latest.get('Realized_EV', 0.0)), 4),
            'Sample_Size': int(latest.get('Sample_Size', len(out))),
            '計算後資料': out,
            'ai_features_latest': latest_features,
        }

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'legacy_screening_dependency': False,
            'absorbed_components': [
                'normalize_ticker_symbol -> fts_market_data_service.py',
                'smart_download -> fts_market_data_service.py',
                'extract_ai_features -> fts_feature_service.py',
                'add_chip_data -> fts_chip_enrichment_service.py',
                'inspect_stock -> fts_screening_engine.py',
            ],
            'status': 'screening_detached_from_legacy',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🛰️ screening engine detached: {self.runtime_path}')
        return self.runtime_path, payload
