# -*- coding: utf-8 -*-
from __future__ import annotations

import numpy as np
import pandas as pd


def _series(df: pd.DataFrame, key: str, fallback=0.0) -> pd.Series:
    if key in df.columns:
        return pd.to_numeric(df[key], errors='coerce')
    return pd.Series([fallback] * len(df), index=df.index, dtype='float64')


def _ensure_bbi(df: pd.DataFrame) -> pd.Series:
    if 'BBI' in df.columns:
        return pd.to_numeric(df['BBI'], errors='coerce').ffill().fillna(_series(df, 'Close', 0.0).fillna(0.0))
    close = _series(df, 'Close', 0.0).fillna(0.0)
    return (
        close.rolling(3, min_periods=1).mean()
        + close.rolling(6, min_periods=1).mean()
        + close.rolling(12, min_periods=1).mean()
        + close.rolling(24, min_periods=1).mean()
    ) / 4.0


def detect_trend_regime(df: pd.DataFrame, p: dict | None = None):
    p = p or {}
    adx = _series(df, 'ADX14', np.nan).fillna(_series(df, 'ADX', 0.0)).fillna(0.0)
    close = _series(df, 'Close', 0.0).fillna(0.0)
    bbi = _ensure_bbi(df)
    threshold = float(p.get('ADX_TREND_THRESHOLD', 20))
    adx_strong = adx >= threshold
    is_bull = (close > bbi) & adx_strong
    is_bear = (close < bbi) & adx_strong
    return np.where(is_bull, '趨勢多頭', np.where(is_bear, '趨勢空頭', '區間盤整'))


def is_vol_breakout(df: pd.DataFrame, multiplier: float = 1.5):
    vol = _series(df, 'Volume', 0.0).fillna(0.0)
    vol_ma = _series(df, 'Vol_MA20', 0.0).fillna(vol.rolling(20, min_periods=1).mean())
    return (vol > (vol_ma * float(multiplier))).fillna(False)


def is_price_breakout(df: pd.DataFrame):
    close = _series(df, 'Close', 0.0).fillna(0.0)
    bbi = _ensure_bbi(df)
    return ((close > bbi) & (close.shift(1) <= bbi.shift(1))).fillna(False)


def is_oversold(df: pd.DataFrame):
    low = _series(df, 'Low', 0.0).fillna(_series(df, 'Close', 0.0))
    bb_lower = _series(df, 'BB_Lower', 0.0).fillna(low)
    rsi = _series(df, 'RSI', 50.0).fillna(50.0)
    return ((low <= bb_lower) & (rsi < 30)).fillna(False)


def is_smart_money_buying(df: pd.DataFrame):
    foreign_net = _series(df, 'Foreign_Net', np.nan).fillna(_series(df, 'Foreign_Ratio', 0.0)).fillna(0.0)
    trust_net = _series(df, 'Trust_Net', np.nan).fillna(_series(df, 'Trust_Ratio', 0.0)).fillna(0.0)
    return ((foreign_net > 0) & (trust_net > 0)).fillna(False)


def apply_market_language_features(df: pd.DataFrame, p: dict | None = None) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        return out
    regime = detect_trend_regime(out, p)
    out['ML_Regime_Text'] = regime
    out['ML_Regime_Code'] = np.where(pd.Series(regime, index=out.index) == '趨勢多頭', 1, np.where(pd.Series(regime, index=out.index) == '趨勢空頭', -1, 0))
    out['ML_Trend_Bull'] = (out['ML_Regime_Code'] == 1).astype(int)
    out['ML_Trend_Bear'] = (out['ML_Regime_Code'] == -1).astype(int)
    out['ML_Trend_Side'] = (out['ML_Regime_Code'] == 0).astype(int)
    out['ML_Volume_Breakout'] = is_vol_breakout(out, float((p or {}).get('VOL_BREAKOUT_MULTIPLIER', 1.1))).astype(int)
    out['ML_Price_Breakout'] = is_price_breakout(out).astype(int)
    out['ML_Oversold'] = is_oversold(out).astype(int)
    out['ML_SmartMoney_Buying'] = is_smart_money_buying(out).astype(int)
    return out


def latest_feature_vector(df: pd.DataFrame, p: dict | None = None) -> dict:
    if df is None or df.empty:
        return {}
    enriched = apply_market_language_features(df, p)
    latest = enriched.iloc[-1]
    return {
        'ML_Regime_Code': float(latest.get('ML_Regime_Code', 0)),
        'ML_Trend_Bull': float(latest.get('ML_Trend_Bull', 0)),
        'ML_Trend_Bear': float(latest.get('ML_Trend_Bear', 0)),
        'ML_Trend_Side': float(latest.get('ML_Trend_Side', 0)),
        'ML_Volume_Breakout': float(latest.get('ML_Volume_Breakout', 0)),
        'ML_Price_Breakout': float(latest.get('ML_Price_Breakout', 0)),
        'ML_Oversold': float(latest.get('ML_Oversold', 0)),
        'ML_SmartMoney_Buying': float(latest.get('ML_SmartMoney_Buying', 0)),
    }
