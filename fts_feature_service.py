# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
import pickle
from bisect import bisect_left, bisect_right, insort
from collections import deque
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        runtime_dir = base_dir / 'runtime'
        data_dir = base_dir / 'data'
        model_dir = base_dir / 'models'
        models_dir = model_dir
    PATHS = _Paths()

try:
    from fts_utils import now_str, safe_float, safe_int, log  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')
    def safe_float(v: Any, default: float = 0.0) -> float:
        try:
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return default
            return float(v)
        except Exception:
            return default
    def safe_int(v: Any, default: int = 0) -> int:
        try:
            return int(float(v))
        except Exception:
            return default
    def log(msg: str) -> None:
        print(msg)

from fts_feature_catalog import FEATURE_BUCKETS, PRIORITY_NEW_FEATURES_20


class FeatureService:
    MODULE_VERSION = 'v83_feature_service_percentile_mount_full'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'feature_service.json'
        self.live_mount_path = PATHS.runtime_dir / 'live_feature_mount.json'
        model_dir = getattr(PATHS, 'models_dir', getattr(PATHS, 'model_dir', Path('models')))
        self.selected_features_path = Path(model_dir) / 'selected_features.pkl'
        self.live_mount_csv = PATHS.data_dir / 'selected_live_feature_mounts.csv'
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)
        Path(PATHS.data_dir).mkdir(parents=True, exist_ok=True)
        if not self.live_mount_csv.exists():
            pd.DataFrame(columns=['ticker', 'feature_name', 'feature_value']).to_csv(
                self.live_mount_csv, index=False, encoding='utf-8-sig'
            )

    @staticmethod
    def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
        """使用 sorted-window 而不是 rolling.apply(rank)，降低全市場 percentile 計算成本。"""
        s = pd.to_numeric(series, errors='coerce').astype(float)
        min_periods = max(5, min(window, 20))
        q = deque()
        sorted_vals: list[float] = []
        out: list[float] = []
        for raw_v in s.tolist():
            q.append(raw_v)
            if not pd.isna(raw_v):
                insort(sorted_vals, float(raw_v))
            if len(q) > window:
                old_v = q.popleft()
                if not pd.isna(old_v):
                    idx = bisect_left(sorted_vals, float(old_v))
                    if 0 <= idx < len(sorted_vals):
                        sorted_vals.pop(idx)
            if len(q) < min_periods or pd.isna(raw_v) or not sorted_vals:
                out.append(0.5)
                continue
            right = bisect_right(sorted_vals, float(raw_v))
            out.append(right / max(len(sorted_vals), 1))
        return pd.Series(out, index=series.index).fillna(0.5)

    @staticmethod
    def _compute_atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
        high = pd.to_numeric(df.get('High', df.get('Close', 0)), errors='coerce')
        low = pd.to_numeric(df.get('Low', df.get('Close', 0)), errors='coerce')
        close = pd.to_numeric(df.get('Close', 0), errors='coerce')
        tr = pd.concat([(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
        return tr.rolling(window, min_periods=1).mean().fillna(0.0)

    def load_selected_features(self) -> list[str]:
        if not self.selected_features_path.exists():
            return []
        try:
            with self.selected_features_path.open('rb') as fh:
                obj = pickle.load(fh)
            if isinstance(obj, (list, tuple)):
                return [str(x) for x in obj if str(x).strip()]
        except Exception:
            return []
        return []

    def _combo_feature(self, name: str, features: Mapping[str, Any]) -> float:
        parts = [p for p in str(name).split('_X_') if p]
        value = 1.0
        for part in parts:
            value *= safe_float(features.get(part, 0.0), 0.0)
        return float(value)

    def select_live_features(self, features: Mapping[str, Any], selected_features: Sequence[str] | None = None) -> dict[str, float]:
        picked = list(selected_features or self.load_selected_features())
        if not picked:
            return {k: safe_float(v, 0.0) for k, v in features.items()}
        out: dict[str, float] = {}
        for key in picked:
            out[key] = self._combo_feature(key, features) if '_X_' in key else safe_float(features.get(key, 0.0), 0.0)
        return out

    def feature_buckets(self) -> dict[str, list[str]]:
        return FEATURE_BUCKETS

    def current_feature_summary(self, features: Mapping[str, Any], selected_features: Sequence[str] | None = None) -> dict[str, Any]:
        selected = list(selected_features or self.load_selected_features())
        selected_set = set(selected)
        bucket_summary = {bucket: {'available': [c for c in cols if c in features], 'selected': [c for c in cols if c in selected_set]} for bucket, cols in FEATURE_BUCKETS.items()}
        return {'all_feature_count': len(features), 'selected_feature_count': len(selected), 'bucket_summary': bucket_summary, 'priority_new_features_20': PRIORITY_NEW_FEATURES_20}

    def enrich_from_history(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if out.empty or 'Close' not in out.columns:
            return out
        close = pd.to_numeric(out['Close'], errors='coerce').ffill().fillna(0.0)
        open_ = pd.to_numeric(out.get('Open', close), errors='coerce').fillna(close)
        volume = pd.to_numeric(out.get('Volume', 0), errors='coerce').fillna(0.0)
        out['ATR14'] = self._compute_atr(out, 14)
        out['ATR_Pct'] = (out['ATR14'] / close.replace(0, np.nan)).fillna(0.0)
        out['ATR_Pctl_252'] = self._rolling_percentile(out['ATR_Pct'], 252)
        logret = np.log(close / close.shift(1)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out['RealizedVol_20'] = (logret.rolling(20, min_periods=5).std() * np.sqrt(252)).fillna(0.0)
        out['RealizedVol_60'] = (logret.rolling(60, min_periods=10).std() * np.sqrt(252)).fillna(0.0)
        prev_close = close.shift(1)
        out['Gap_Pct'] = ((open_ - prev_close) / prev_close.replace(0, np.nan)).fillna(0.0)
        out['Overnight_Return'] = out['Gap_Pct']
        out['Intraday_Return'] = ((close - open_) / open_.replace(0, np.nan)).fillna(0.0)
        turnover = (close * volume).fillna(0.0)
        out['Turnover_Proxy'] = turnover
        out['ADV20_Proxy'] = turnover.rolling(20, min_periods=1).mean().fillna(0.0)
        out['DollarVol20_Proxy'] = out['ADV20_Proxy']
        vol_mean = volume.rolling(20, min_periods=5).mean()
        vol_std = volume.rolling(20, min_periods=5).std().replace(0, np.nan)
        out['Volume_Z20'] = ((volume - vol_mean) / vol_std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        ret1 = close.pct_change().fillna(0.0)
        ret_mean = ret1.rolling(20, min_periods=5).mean()
        ret_std = ret1.rolling(20, min_periods=5).std().replace(0, np.nan)
        out['Return_Z20'] = ((ret1 - ret_mean) / ret_std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        return out

    def extract_ai_features(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, Any]:
        close_p = safe_float(row.get('Close', 1), 1)
        open_p = safe_float(row.get('Open', 1), 1)
        high_p = safe_float(row.get('High', close_p), close_p)
        low_p = safe_float(row.get('Low', close_p), close_p)
        ma20 = safe_float(row.get('MA20', close_p), close_p)
        volume = safe_float(row.get('Volume', 0), 0)
        vol_ma20 = safe_float(row.get('Vol_MA20', 0), 0)
        features = {
            'K_Body_Pct': ((close_p - open_p) / open_p) if open_p else 0.0,
            'Upper_Shadow': ((high_p - max(close_p, open_p)) / close_p) if close_p else 0.0,
            'Lower_Shadow': ((min(close_p, open_p) - low_p) / close_p) if close_p else 0.0,
            'Dist_to_MA20': ((close_p - ma20) / (ma20 + 1e-4)),
            'Volume_Ratio': (volume / (vol_ma20 + 1e-3)) if vol_ma20 else 1.0,
            'BB_Width': safe_float(row.get('BB_Width', 0), 0),
            'RSI': safe_float(row.get('RSI', 50), 50),
            'MACD_Hist': safe_float(row.get('MACD_Hist', 0), 0),
            'ADX': safe_float(row.get('ADX14', row.get('ADX', 25)), 25),
            'Foreign_Ratio': safe_float(row.get('Foreign_Ratio', 0), 0),
            'Trust_Ratio': safe_float(row.get('Trust_Ratio', 0), 0),
            'Total_Ratio': safe_float(row.get('Total_Ratio', 0), 0),
            'Foreign_Consec_Days': safe_int(row.get('Foreign_Consecutive', row.get('Foreign_Consec_Days', 0)), 0),
            'Trust_Consec_Days': safe_int(row.get('Trust_Consecutive', row.get('Trust_Consec_Days', 0)), 0),
            'Weighted_Buy_Score': safe_float(row.get('Weighted_Buy_Score', row.get('Buy_Score', 0)), 0),
            'Weighted_Sell_Score': safe_float(row.get('Weighted_Sell_Score', row.get('Sell_Score', 0)), 0),
            'Score_Gap': safe_float(row.get('Score_Gap', 0), 0),
            'Signal_Conflict': safe_int(row.get('Signal_Conflict', 0), 0),
        }
        for key in ['buy_c2', 'buy_c3', 'buy_c4', 'buy_c5', 'buy_c6', 'buy_c7', 'buy_c8', 'buy_c9', 'sell_c2', 'sell_c3', 'sell_c4', 'sell_c5', 'sell_c6', 'sell_c7', 'sell_c8', 'sell_c9']:
            features[key] = safe_int(row.get(key, 0), 0)
        features['Trap_Signal'] = 1 if bool(row.get('Fake_Breakout', False)) else (-1 if bool(row.get('Bear_Trap', False)) else 0)
        features['Vol_Squeeze'] = safe_int(row.get('Vol_Squeeze', False), 0)
        features['Absorption'] = safe_int(row.get('Absorption', False), 0)
        features['MR_Long_Spring'] = safe_int(row.get('MR_Long_Spring', 0), 0)
        features['MR_Short_Trap'] = safe_int(row.get('MR_Short_Trap', 0), 0)
        features['MR_Long_Accumulation'] = safe_int(row.get('MR_Long_Accumulation', 0), 0)
        features['MR_Short_Distribution'] = safe_int(row.get('MR_Short_Distribution', 0), 0)
        for k in PRIORITY_NEW_FEATURES_20:
            if k in row:
                features[k] = safe_float(row.get(k, 0), 0)
        features.setdefault('ATR14', safe_float(row.get('ATR14', 0), 0))
        features.setdefault('ATR_Pct', safe_float(row.get('ATR_Pct', 0), 0))
        features.setdefault('ATR_Pctl_252', safe_float(row.get('ATR_Pctl_252', 0.5), 0.5))
        features.setdefault('RealizedVol_20', safe_float(row.get('RealizedVol_20', 0), 0))
        features.setdefault('RealizedVol_60', safe_float(row.get('RealizedVol_60', 0), 0))
        prev_close = safe_float(row.get('Prev_Close', close_p), close_p)
        features.setdefault('Gap_Pct', ((open_p - prev_close) / prev_close) if prev_close else 0.0)
        features.setdefault('Overnight_Return', features['Gap_Pct'])
        features.setdefault('Intraday_Return', ((close_p - open_p) / open_p) if open_p else 0.0)
        features.setdefault('Turnover_Proxy', close_p * volume)
        features.setdefault('ADV20_Proxy', safe_float(row.get('ADV20_Proxy', features['Turnover_Proxy']), features['Turnover_Proxy']))
        features.setdefault('DollarVol20_Proxy', safe_float(row.get('DollarVol20_Proxy', features['ADV20_Proxy']), features['ADV20_Proxy']))
        features.setdefault('Volume_Z20', safe_float(row.get('Volume_Z20', 0), 0))
        features.setdefault('Return_Z20', safe_float(row.get('Return_Z20', 0), 0))
        if history_df is not None and not history_df.empty:
            enriched = self.enrich_from_history(history_df)
            latest = enriched.iloc[-1].to_dict()
            for k in ['ATR14','ATR_Pct','ATR_Pctl_252','RealizedVol_20','RealizedVol_60','Gap_Pct','Overnight_Return','Intraday_Return','Turnover_Proxy','ADV20_Proxy','DollarVol20_Proxy','Volume_Z20','Return_Z20']:
                features[k] = safe_float(latest.get(k, features.get(k, 0.0)), features.get(k, 0.0))
        passthrough = ['RS_vs_Market_20','RS_vs_Sector_20','RS_vs_Market_20_Pctl','RS_vs_Sector_20_Pctl','Revenue_YoY','Revenue_YoY_Pctl','Chip_Total_Ratio','Chip_Total_Ratio_Pctl','Turnover_Pctl','ADV20_Pctl','ATR_Pct_Pctl','RealizedVol_20_Pctl','Event_Days_Since_Revenue','Event_Days_To_Revenue','Revenue_Window_1','Revenue_Window_3','Revenue_Window_5','Revenue_Window_10','Event_Days_Since_Earnings','Event_Days_To_Earnings','Earnings_Window_3','Earnings_Window_7','Earnings_Window_14','Earnings_Window_Flag','Dividend_Window_7']
        for k in passthrough:
            if k in row:
                features[k] = safe_float(row.get(k, 0), 0)
        features['Revenue_YoY_Rank'] = safe_float(row.get('Revenue_YoY_Rank', row.get('Revenue_YoY_Pctl', 0.5)), 0.5)
        features['Chip_Total_Ratio_Rank'] = safe_float(row.get('Chip_Total_Ratio_Rank', row.get('Chip_Total_Ratio_Pctl', 0.5)), 0.5)
        features['Regime_TrendStrength_X_ScoreGap'] = safe_float(row.get('Regime_TrendStrength_X_ScoreGap', (features['ADX'] / 100.0) * features['Score_Gap']), 0)
        features['Volatility_X_SignalConflict'] = safe_float(row.get('Volatility_X_SignalConflict', features['ATR_Pctl_252'] * features['Signal_Conflict']), 0)
        return features

    def mount_live_features(self, ticker: str, as_of_row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> tuple[dict[str, Any], dict[str, float]]:
        features = self.extract_ai_features(as_of_row, history_df=history_df)
        from fts_event_calendar_service import EventCalendarService
        event_service = EventCalendarService()
        if history_df is not None and not history_df.empty:
            as_of_date = history_df.index[-1]
        else:
            as_of_date = as_of_row.get('Date', now_str())
        features.update(event_service.event_vector(ticker, as_of_date))
        from fts_cross_sectional_percentile_service import CrossSectionalPercentileService
        features = CrossSectionalPercentileService().enrich_row(ticker, features)
        selected = self.load_selected_features()
        mounted = self.select_live_features(features, selected_features=selected)
        mount_payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'ticker': str(ticker), 'selected_feature_count': len(selected), 'mounted_feature_count': len(mounted), 'selected_features_present': bool(selected), 'official_percentile_mode': True, 'precise_event_calendar_mode': True, 'status': 'live_feature_mount_ready'}
        self.live_mount_path.write_text(json.dumps(mount_payload, ensure_ascii=False, indent=2), encoding='utf-8')
        mount_rows = [
            {
                'ticker': str(ticker),
                'feature_name': str(name),
                'feature_value': safe_float(value, 0.0),
            }
            for name, value in mounted.items()
        ]
        pd.DataFrame(mount_rows or [{'ticker': str(ticker), 'feature_name': '', 'feature_value': 0.0}]).to_csv(
            self.live_mount_csv, index=False, encoding='utf-8-sig'
        )
        return features, mounted

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        sample_df = pd.DataFrame({'Open':[98,100,101,102],'High':[101,103,104,105],'Low':[97,99,100,100],'Close':[100,101,103,104],'Volume':[1000,1100,900,1500]}, index=pd.date_range('2026-01-01', periods=4))
        sample = self.extract_ai_features(self.enrich_from_history(sample_df).iloc[-1].to_dict(), history_df=sample_df)
        selected = self.load_selected_features()
        payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'sample_feature_count': len(sample), 'selected_features_present': bool(selected), 'selected_feature_count': len(selected), 'feature_buckets': {k: len(v) for k, v in FEATURE_BUCKETS.items()}, 'priority_new_features_20': PRIORITY_NEW_FEATURES_20, 'official_percentile_mode': True, 'precise_event_calendar_mode': True, 'live_mount_path': str(self.live_mount_path), 'rolling_percentile_engine': 'sorted_window_fast', 'status': 'feature_service_ready'}
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧩 feature service ready: {self.runtime_path}')
        return self.runtime_path, payload
