# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        base_dir = Path(__file__).resolve().parent
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
from fts_feature_service import FeatureService
from fts_chip_enrichment_service import ChipEnrichmentService


class ScreeningEngine:
    MODULE_VERSION = 'v83_screening_engine_percentile_mount_full'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'screening_engine.json'
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)
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
        tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
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
        out['Signal_Conflict'] = ((out['Weighted_Buy_Score'] > 0) & (out['Weighted_Sell_Score'] > 0)).astype(int)
        out['Vol_Squeeze'] = (out['BB_Width'] < out['BB_Width'].rolling(20, min_periods=5).mean()).fillna(False).astype(int)
        out['Fake_Breakout'] = ((out['Close'] < out['MA20']) & (out['RSI'] > 55)).fillna(False)
        out['Bear_Trap'] = ((out['Close'] > out['MA20']) & (out['RSI'] < 45)).fillna(False)
        out['Absorption'] = ((out['Close'].pct_change().fillna(0) < 0) & (out.get('Total_Ratio', 0) > 0)).astype(int)
        out['MR_Long_Spring'] = ((out['Low'] < out['BB_Lower']) & (out['RSI'] < 35)).fillna(False).astype(int)
        out['MR_Short_Trap'] = ((out['High'] > out['BB_Upper']) & (out['RSI'] > 65)).fillna(False).astype(int)
        out['MR_Long_Accumulation'] = ((out['Close'] > out['MA20']) & (out.get('Total_Ratio', 0) > 0)).astype(int)
        out['MR_Short_Distribution'] = ((out['Close'] < out['MA20']) & (out.get('Total_Ratio', 0) < 0)).astype(int)
        out['Regime'] = out['ADX14'].apply(lambda x: '趨勢多頭' if x >= 30 else ('區間盤整' if x < 20 else '趨勢空頭'))
        out['AI_Proba'] = (0.5 + out['Score_Gap'].clip(-2, 2) * 0.1).clip(0.01, 0.99)
        out['Realized_EV'] = (out['Close'].shift(-5) / out['Close'] - 1).fillna(0.0)
        return out

    def inspect_stock(self, ticker: str, period: str = '1y') -> dict[str, Any] | None:
        hist = self.market.smart_download(ticker, period=period)
        if hist.empty or 'Close' not in hist.columns:
            return None
        prepared = self._prepare(hist)
        if prepared.empty:
            return None
        latest = prepared.iloc[-1].to_dict()
        latest = self.chips.enrich_row(ticker, latest)
        all_features, mounted = self.features.mount_live_features(ticker, latest, history_df=prepared)
        latest.update(all_features)
        latest['Mounted_Feature_Count'] = len(mounted)
        latest['Mounted_Features'] = list(mounted.keys())[:100]
        latest['Official_Percentile_Mode'] = 1
        latest['Precise_Event_Calendar_Mode'] = 1
        latest['Selected_Features_Driven_Live'] = int(bool(self.features.load_selected_features()))
        return latest

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'legacy_screening_dependency': False, 'official_percentile_mode': True, 'precise_event_calendar_mode': True, 'selected_features_driven_live': bool(self.features.load_selected_features()), 'status': 'screening_engine_ready'}
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🛰️ screening engine detached: {self.runtime_path}')
        return self.runtime_path, payload
