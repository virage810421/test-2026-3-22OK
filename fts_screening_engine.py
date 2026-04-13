# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
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
from fts_signal_primitives import (
    _apply_weighted_scores,
    _assign_golden_type,
    _compute_realized_signal_stats,
)


class ScreeningEngine:
    MODULE_VERSION = 'v85_screening_engine_regime_upgrade'

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

    @staticmethod
    def _atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int = 14) -> pd.Series:
        tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
        return tr.rolling(window, min_periods=1).mean().fillna(0.0)

    @staticmethod
    def _rolling_percentile(series: pd.Series, window: int = 252) -> pd.Series:
        s = pd.to_numeric(series, errors='coerce').astype(float)
        return s.rolling(window, min_periods=min(30, window)).rank(pct=True).fillna(0.5)

    @staticmethod
    def _infer_regime(df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        out = df.copy()
        close = pd.to_numeric(out['Close'], errors='coerce').fillna(0.0)
        ma20 = pd.to_numeric(out['MA20'], errors='coerce').bfill().fillna(close)
        ma60 = pd.to_numeric(out['MA60'], errors='coerce').bfill().fillna(ma20)
        adx = pd.to_numeric(out['ADX14'], errors='coerce').fillna(20.0)
        atr_pct = pd.to_numeric(out['ATR_Pct'], errors='coerce').fillna(0.0)
        atr_pctl = pd.to_numeric(out['ATR_Pct_Pctl'], errors='coerce').fillna(0.5)
        bb_width = pd.to_numeric(out['BB_Width'], errors='coerce').fillna(0.0)
        macd_hist = pd.to_numeric(out['MACD_Hist'], errors='coerce').fillna(0.0)
        macd_line = pd.to_numeric(out['MACD'], errors='coerce').fillna(0.0)

        ma20_slope = ma20.diff(5).fillna(0.0) / ma20.shift(5).replace(0, pd.NA)
        ma60_slope = ma60.diff(10).fillna(0.0) / ma60.shift(10).replace(0, pd.NA)
        price_vs_ma60 = (close / ma60.replace(0, pd.NA) - 1.0).fillna(0.0)
        price_vs_ma20 = (close / ma20.replace(0, pd.NA) - 1.0).fillna(0.0)
        width_mean = bb_width.rolling(30, min_periods=10).mean().replace(0, pd.NA)
        squeeze_flag = (bb_width < width_mean * 0.85).fillna(False)

        direction_score = (
            np.sign(price_vs_ma60) * 1.6
            + np.sign(ma20_slope.fillna(0.0)) * 1.1
            + np.sign(ma60_slope.fillna(0.0)) * 0.9
            + np.sign(macd_hist) * 0.8
            + np.sign(macd_line) * 0.6
            + np.sign(price_vs_ma20) * 0.5
        )
        strength_score = (
            ((adx - float(params.get('ADX_TREND_THRESHOLD', 20))) / 10.0).clip(-1.5, 2.5)
            + ((atr_pctl - 0.5) * 1.2)
            + ((bb_width / width_mean).replace([np.inf, -np.inf], np.nan).fillna(1.0) - 1.0).clip(-0.8, 1.2)
        )
        environment_score = (
            (atr_pct * 100.0).clip(0.0, 8.0) * 0.08
            + squeeze_flag.astype(float) * -0.7
        )
        composite = direction_score + strength_score + environment_score

        bull = (direction_score >= 1.6) & (adx >= float(params.get('ADX_TREND_THRESHOLD', 20))) & (price_vs_ma60 > -0.01)
        bear = (direction_score <= -1.6) & (adx >= float(params.get('ADX_TREND_THRESHOLD', 20))) & (price_vs_ma60 < 0.01)
        side = (~bull & ~bear) | ((adx < max(18.0, float(params.get('ADX_TREND_THRESHOLD', 20)) - 2.0)) & squeeze_flag)

        out['Regime_Direction_Score'] = direction_score.round(4)
        out['Regime_Strength_Score'] = strength_score.round(4)
        out['Regime_Environment_Score'] = environment_score.round(4)
        out['Regime_Composite_Score'] = composite.round(4)
        out['Regime_Source'] = 'direction_strength_environment_v2'
        out['Regime'] = np.where(bull, '趨勢多頭', np.where(bear, '趨勢空頭', '區間盤整'))
        out.loc[side, 'Regime'] = '區間盤整'
        out['Regime_Intensity'] = np.where(
            out['Regime'] == '區間盤整',
            'sideways',
            np.where((adx >= 30) & (atr_pctl >= 0.55), 'aggressive', 'balanced')
        )
        return out

    def _prepare(self, df: pd.DataFrame, p: dict[str, Any] | None = None) -> pd.DataFrame:
        params = p or {}
        out = df.copy()
        if out.empty or 'Close' not in out.columns:
            return pd.DataFrame()

        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col not in out.columns:
                out[col] = 0.0
        out = out.sort_index()

        out['MA20'] = out['Close'].rolling(20, min_periods=1).mean()
        out['MA60'] = out['Close'].rolling(60, min_periods=1).mean()
        out['STD20'] = out['Close'].rolling(20, min_periods=1).std().fillna(0)
        out['BB_std'] = out['STD20']
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
        out['ATR14'] = self._atr(high, low, out['Close'])
        out['ATR_Pct'] = (out['ATR14'] / out['Close'].replace(0, pd.NA)).fillna(0.0)
        out['ATR_Pct_Pctl'] = self._rolling_percentile(out['ATR_Pct'], 252)

        out['BBI'] = (
            out['Close'].rolling(3, min_periods=1).mean()
            + out['Close'].rolling(6, min_periods=1).mean()
            + out['Close'].rolling(12, min_periods=1).mean()
            + out['Close'].rolling(24, min_periods=1).mean()
        ) / 4.0
        vol_ma20 = out['Vol_MA20'].replace(0, pd.NA)
        out['buy_c2'] = (out['RSI'] > 50).astype(int)
        out['sell_c2'] = (out['RSI'] < 50).astype(int)
        out['buy_c3'] = (out['Volume'] >= vol_ma20 * float(params.get('VOL_BREAKOUT_MULTIPLIER', 1.1))).fillna(False).astype(int)
        out['sell_c3'] = (out['Volume'] >= vol_ma20 * float(params.get('VOL_BREAKOUT_MULTIPLIER', 1.1))).fillna(False).astype(int)
        out['buy_c4'] = (out['MACD_Hist'] > 0).astype(int)
        out['sell_c4'] = (out['MACD_Hist'] < 0).astype(int)
        out['buy_c5'] = ((out['Close'] <= out['BB_Lower']) & (out['RSI'] < 40)).fillna(False).astype(int)
        out['sell_c5'] = ((out['Close'] >= out['BB_Upper']) & (out['RSI'] > 60)).fillna(False).astype(int)
        out['buy_c6'] = (out['Close'] > out['BBI']).astype(int)
        out['sell_c6'] = (out['Close'] < out['BBI']).astype(int)
        out['buy_c7'] = (pd.to_numeric(out.get('Foreign_Ratio', 0), errors='coerce').fillna(0) > 0).astype(int)
        out['sell_c7'] = (pd.to_numeric(out.get('Foreign_Ratio', 0), errors='coerce').fillna(0) < 0).astype(int)
        out['buy_c8'] = ((out['ADX14'] >= float(params.get('ADX_TREND_THRESHOLD', 20))) & (out['Close'] > out['MA20'])).astype(int)
        out['sell_c8'] = ((out['ADX14'] >= float(params.get('ADX_TREND_THRESHOLD', 20))) & (out['Close'] < out['MA20'])).astype(int)
        out['buy_c9'] = (pd.to_numeric(out.get('Total_Ratio', 0), errors='coerce').fillna(0) > 0).astype(int)
        out['sell_c9'] = (pd.to_numeric(out.get('Total_Ratio', 0), errors='coerce').fillna(0) < 0).astype(int)

        out['Buy_Score'] = out[[f'buy_c{i}' for i in range(2, 10)]].sum(axis=1)
        out['Sell_Score'] = out[[f'sell_c{i}' for i in range(2, 10)]].sum(axis=1)
        out = _apply_weighted_scores(out, params)
        out = _assign_golden_type(out, float(params.get('TRIGGER_SCORE', 2)))

        out['Vol_Squeeze'] = (out['BB_Width'] < out['BB_Width'].rolling(20, min_periods=5).mean()).fillna(False).astype(int)
        out['Fake_Breakout'] = ((out['Close'] < out['MA20']) & (out['RSI'] > 55)).fillna(False)
        out['Bear_Trap'] = ((out['Close'] > out['MA20']) & (out['RSI'] < 45)).fillna(False)
        out['Absorption'] = ((out['Close'].pct_change().fillna(0) < 0) & (pd.to_numeric(out.get('Total_Ratio', 0), errors='coerce').fillna(0) > 0)).astype(int)
        out['MR_Long_Spring'] = ((out['Low'] < out['BB_Lower']) & (out['RSI'] < 35)).fillna(False).astype(int)
        out['MR_Short_Trap'] = ((out['High'] > out['BB_Upper']) & (out['RSI'] > 65)).fillna(False).astype(int)
        out['MR_Long_Accumulation'] = ((out['Close'] > out['MA20']) & (pd.to_numeric(out.get('Total_Ratio', 0), errors='coerce').fillna(0) > 0)).astype(int)
        out['MR_Short_Distribution'] = ((out['Close'] < out['MA20']) & (pd.to_numeric(out.get('Total_Ratio', 0), errors='coerce').fillna(0) < 0)).astype(int)
        out = self._infer_regime(out, params)
        out['AI_Proba'] = (0.5 + out['Score_Gap'].clip(-2, 2) * 0.1).clip(0.01, 0.99)
        out['Realized_EV'] = (out['Close'].shift(-int(params.get('ML_LABEL_HOLD_DAYS', 5))) / out['Close'] - 1).fillna(0.0)

        stats = _compute_realized_signal_stats(out, params, hold_days=int(params.get('ML_LABEL_HOLD_DAYS', 5)))
        for k, v in stats.items():
            if k == 'Realized_Signal_Returns':
                continue
            out[k] = v
        out['訊號信心分數(%)'] = (out['AI_Proba'] * 100.0).round(2)
        return out

    def inspect_stock(self, ticker: str, preloaded_df: pd.DataFrame | None = None, p: dict[str, Any] | None = None, period: str = '1y') -> dict[str, Any] | None:
        hist = preloaded_df.copy() if isinstance(preloaded_df, pd.DataFrame) and not preloaded_df.empty else self.market.smart_download(ticker, period=period)
        if hist.empty or 'Close' not in hist.columns:
            return None

        if not any(col in hist.columns for col in ['Foreign_Ratio', 'Total_Ratio', 'Trust_Ratio']):
            try:
                hist = self.chips.add_chip_data(hist, ticker)
            except Exception:
                pass

        prepared = self._prepare(hist, p=p)
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
        latest['計算後資料'] = prepared
        return latest

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'legacy_screening_dependency': False,
            'official_percentile_mode': True,
            'precise_event_calendar_mode': True,
            'selected_features_driven_live': bool(self.features.load_selected_features()),
            'regime_engine': 'direction_strength_environment_v2',
            'status': 'screening_engine_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🛰️ screening engine detached: {self.runtime_path}')
        return self.runtime_path, payload
