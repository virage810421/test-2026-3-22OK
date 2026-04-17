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
    from fts_runtime_diagnostics import record_issue, write_summary as write_runtime_diagnostics_summary
except Exception:  # pragma: no cover
    def record_issue(*args, **kwargs):
        return {}
    def write_runtime_diagnostics_summary(*args, **kwargs):
        return None

try:
    from fts_config import PATHS, CONFIG  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        runtime_dir = base_dir / 'runtime'
        data_dir = base_dir / 'data'
        model_dir = base_dir / 'models'
        models_dir = model_dir
    class _Config:
        strict_feature_parity = True
        selected_features_required_for_live = True
        force_shared_feature_universe = False
        enable_directional_features_in_training = True
        enable_directional_features_in_live = True
    PATHS = _Paths()
    CONFIG = _Config()

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



def get_runtime_diagnostics_safe() -> dict:
    try:
        from fts_runtime_diagnostics import get_summary
        return get_summary()
    except Exception:
        return {}

from fts_feature_catalog import FEATURE_BUCKETS, PRIORITY_NEW_FEATURES_20, FEATURE_SPECS, LIVE_SAFE_FEATURES, APPROVED_LIVE_DIRECTIONAL_FEATURES, is_feature_live_approved, get_training_feature_groups, get_live_feature_groups
from fts_promoted_model_guard import resolve_runtime_model_dir


class FeatureService:
    MODULE_VERSION = 'v102_feature_service_structural_bias_and_diagnostics'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'feature_service.json'
        self.live_mount_path = PATHS.runtime_dir / 'live_feature_mount.json'
        model_dir = resolve_runtime_model_dir()
        self.selected_features_path = Path(model_dir) / 'selected_features.pkl'
        self.directional_selected_feature_paths = {
            'LONG': Path(model_dir) / 'selected_features_long.pkl',
            'SHORT': Path(model_dir) / 'selected_features_short.pkl',
            'RANGE': Path(model_dir) / 'selected_features_range.pkl',
        }
        self.live_mount_csv = PATHS.data_dir / 'selected_live_feature_mounts.csv'
        self.training_registry_csv = PATHS.runtime_dir / 'training_feature_registry.csv'
        self.training_registry_json = PATHS.runtime_dir / 'training_feature_registry.json'
        self.parity_status_path = PATHS.runtime_dir / 'feature_parity_status.json'
        self.feature_manifest_path = PATHS.runtime_dir / getattr(CONFIG, 'feature_manifest_filename', 'training_feature_manifest.json')
        self.artifact_max_age_days = int(getattr(CONFIG, 'feature_artifact_max_age_days', 7))
        self.min_live_feature_count = int(getattr(CONFIG, 'selected_features_min_count_for_live', 6))
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)
        Path(PATHS.data_dir).mkdir(parents=True, exist_ok=True)

        if not self.live_mount_csv.exists():
            pd.DataFrame(columns=['ticker', 'feature_name', 'feature_value']).to_csv(
                self.live_mount_csv, index=False, encoding='utf-8-sig'
            )

    @staticmethod
    def _is_directional_feature_name(name: str) -> bool:
        s = str(name or '')
        return s.startswith(('Short_', 'Range_'))

    @staticmethod
    def _load_pickle_feature_list(path: Path) -> list[str]:
        if not path.exists():
            return []
        try:
            with path.open('rb') as fh:
                obj = pickle.load(fh)
            if isinstance(obj, (list, tuple)):
                return [str(x) for x in obj if str(x).strip()]
        except Exception as exc:
            record_issue('feature_service', 'load_selected_features_pickle', exc, severity='ERROR', fail_mode='fail_closed', context={'path': str(path)})
            return []
        return []

    def _allow_directional_runtime(self, mode: str = 'live') -> bool:
        mode = str(mode or 'live').strip().lower()
        if bool(getattr(CONFIG, 'force_shared_feature_universe', False)):
            return False
        if mode == 'training':
            return bool(getattr(CONFIG, 'enable_directional_features_in_training', False))
        return bool(getattr(CONFIG, 'enable_directional_features_in_live', False))

    def _merge_selected_feature_sets(self, shared: Sequence[str], directional_map: Mapping[str, Sequence[str]] | None = None) -> list[str]:
        out: list[str] = []
        for name in list(shared or []):
            s = str(name).strip()
            if s and s not in out:
                out.append(s)
        for scope in ['LONG', 'SHORT', 'RANGE']:
            for name in list((directional_map or {}).get(scope, []) or []):
                s = str(name).strip()
                if s and s not in out:
                    out.append(s)
        return out

    def load_directional_selected_features(self) -> dict[str, list[str]]:
        return {scope: self._load_pickle_feature_list(path) for scope, path in self.directional_selected_feature_paths.items()}

    @staticmethod
    def _is_live_safe_feature_name(name: str) -> bool:
        s = str(name or '').strip()
        if not s:
            return False
        if s in LIVE_SAFE_FEATURES or is_feature_live_approved(s):
            return True
        if '_X_' in s:
            parts = [x for x in s.split('_X_') if x]
            return bool(parts) and all((part in LIVE_SAFE_FEATURES) or is_feature_live_approved(part) for part in parts)
        return False

    def _filter_selected_features_for_runtime(self, selected: Sequence[str], mode: str = 'live') -> list[str]:
        out = [str(x) for x in selected if str(x).strip()]
        out = list(dict.fromkeys(out))
        allow_directional = self._allow_directional_runtime(mode)
        require_approved = bool(getattr(CONFIG, 'directional_live_require_approved_features', True)) if str(mode).lower() == 'live' else False
        filtered: list[str] = []
        for name in out:
            if self._is_directional_feature_name(name):
                if not allow_directional:
                    continue
                if require_approved and not self._is_live_safe_feature_name(name):
                    continue
                filtered.append(name)
                continue
            if str(mode).lower() == 'live' and not self._is_live_safe_feature_name(name):
                continue
            filtered.append(name)
        return list(dict.fromkeys(filtered))

    @staticmethod
    def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
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

    @staticmethod
    def _safe_pct(numer: pd.Series, denom: pd.Series) -> pd.Series:
        denom = pd.to_numeric(denom, errors='coerce').replace(0, np.nan)
        numer = pd.to_numeric(numer, errors='coerce')
        return (numer / denom).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _series_or_default(self, df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors='coerce').ffill().fillna(default)
    return pd.Series([default] * len(df), index=df.index, dtype=float)


def _compute_structural_bias_frame(self, out: pd.DataFrame) -> pd.DataFrame:
    if out.empty:
        return out

    close = self._series_or_default(out, 'Close', 0.0)
    if (close == 0.0).all() and 'Adj Close' in out.columns:
        close = self._series_or_default(out, 'Adj Close', 0.0)
    if (close == 0.0).all():
        return out

    ma20 = self._series_or_default(out, 'MA20', float('nan'))
    if ma20.isna().all() and 'MA_20' in out.columns:
        ma20 = self._series_or_default(out, 'MA_20', float('nan'))
    if ma20.isna().all():
        ma20 = close.rolling(20, min_periods=1).mean()
    ma20 = pd.to_numeric(ma20, errors='coerce').ffill().fillna(close)

    ma60 = self._series_or_default(out, 'MA60', float('nan'))
    if ma60.isna().all() and 'MA_60' in out.columns:
        ma60 = self._series_or_default(out, 'MA_60', float('nan'))
    if ma60.isna().all():
        ma60 = close.rolling(60, min_periods=5).mean()
    ma60 = pd.to_numeric(ma60, errors='coerce').ffill().fillna(close)

    ma120 = self._series_or_default(out, 'MA120', float('nan'))
    if ma120.isna().all() and 'MA_120' in out.columns:
        ma120 = self._series_or_default(out, 'MA_120', float('nan'))
    if ma120.isna().all():
        ma120 = close.rolling(120, min_periods=10).mean()
    ma120 = pd.to_numeric(ma120, errors='coerce').ffill().fillna(close)

    if 'ADX14' in out.columns:
        adx14 = self._series_or_default(out, 'ADX14', 0.0)
    elif 'ADX' in out.columns:
        adx14 = self._series_or_default(out, 'ADX', 0.0)
    else:
        adx14 = pd.Series([0.0] * len(out), index=out.index, dtype=float)

    bb_width = self._series_or_default(out, 'BB_Width', 0.0)
    range_width_pct = self._series_or_default(out, 'Range_Width_Pct', 0.0)

    out['MA20'] = ma20
    out['MA60'] = ma60
    out['MA120'] = ma120
    out['Dist_MA60_Pct'] = self._safe_pct(close - ma60, ma60)
    out['Dist_MA120_Pct'] = self._safe_pct(close - ma120, ma120)
    out['MA20_vs_MA60_Pct'] = self._safe_pct(ma20 - ma60, ma60)
    out['MA60_vs_MA120_Pct'] = self._safe_pct(ma60 - ma120, ma120)
    out['MA20_Slope_5'] = self._safe_pct(ma20 - ma20.shift(5).fillna(ma20), ma20.shift(5).fillna(ma20))
    out['MA60_Slope_10'] = self._safe_pct(ma60 - ma60.shift(10).fillna(ma60), ma60.shift(10).fillna(ma60))

    long_cond = (close > ma60) & (ma20 > ma60) & (ma60 >= ma120)
    short_cond = (close < ma60) & (ma20 < ma60) & (ma60 <= ma120)
    bb_width_median = bb_width.rolling(40, min_periods=5).median().ffill().fillna(bb_width)
    range_width_median = range_width_pct.rolling(40, min_periods=5).median().ffill().fillna(range_width_pct)
    range_cond = (adx14 <= 18.0) & (bb_width <= bb_width_median) & (range_width_pct <= range_width_median)

    long_bias = (
        long_cond.astype(float) * 0.45
        + (out['Dist_MA60_Pct'].clip(lower=0.0) * 8.0).clip(0.0, 0.20)
        + (out['MA20_vs_MA60_Pct'].clip(lower=0.0) * 10.0).clip(0.0, 0.18)
        + (out['MA60_Slope_10'].clip(lower=0.0) * 20.0).clip(0.0, 0.17)
    ).clip(0.0, 1.0)
    short_bias = (
        short_cond.astype(float) * 0.45
        + ((-out['Dist_MA60_Pct']).clip(lower=0.0) * 8.0).clip(0.0, 0.20)
        + ((-out['MA20_vs_MA60_Pct']).clip(lower=0.0) * 10.0).clip(0.0, 0.18)
        + ((-out['MA60_Slope_10']).clip(lower=0.0) * 20.0).clip(0.0, 0.17)
    ).clip(0.0, 1.0)
    range_bias = (
        range_cond.astype(float) * 0.50
        + ((18.0 - adx14).clip(lower=0.0) / 18.0 * 0.20)
        + self._rolling_percentile(1.0 - bb_width.rank(pct=True, method='average'), 20).fillna(0.5) * 0.15
        + self._rolling_percentile(1.0 - range_width_pct.rank(pct=True, method='average'), 20).fillna(0.5) * 0.15
    ).clip(0.0, 1.0)

    out['Structural_Long_Bias'] = long_bias.round(6)
    out['Structural_Short_Bias'] = short_bias.round(6)
    out['Structural_Range_Bias'] = range_bias.round(6)
    structural_label = np.where(long_bias >= np.maximum(short_bias, range_bias), 'LONG', np.where(short_bias >= np.maximum(long_bias, range_bias), 'SHORT', 'RANGE'))
    out['Structural_Regime_Bias'] = pd.Series(structural_label, index=out.index).astype(str)
    return out


    def _compute_directional_features(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, float]:
        close = safe_float(row.get('Close', row.get('close', 0.0)), 0.0)
        open_ = safe_float(row.get('Open', row.get('open', close)), close)
        high = safe_float(row.get('High', row.get('high', max(close, open_))), max(close, open_))
        low = safe_float(row.get('Low', row.get('low', min(close, open_))), min(close, open_))
        ma20 = safe_float(row.get('MA20', row.get('MA_20', close)), close)
        rsi = safe_float(row.get('RSI', 50.0), 50.0)
        bb_width = safe_float(row.get('BB_Width', 0.0), 0.0)
        total_ratio = safe_float(row.get('Total_Ratio', 0.0), 0.0)
        upper_shadow = safe_float(row.get('Upper_Shadow_Pct', row.get('Upper_Shadow', 0.0)), 0.0)
        lower_shadow = safe_float(row.get('Lower_Shadow_Pct', row.get('Lower_Shadow', 0.0)), 0.0)
        intraday_return = safe_float(row.get('Intraday_Return', (close - open_) / open_ if open_ else 0.0), 0.0)
        gap_pct = safe_float(row.get('Gap_Pct', 0.0), 0.0)
        rs_market = safe_float(row.get('RS_vs_Market_20', 0.0), 0.0)

        if history_df is not None and not history_df.empty and len(history_df) >= 10 and 'Close' in history_df.columns:
            h = history_df.copy()
            c = pd.to_numeric(h['Close'], errors='coerce').ffill().fillna(close)
            high_s = pd.to_numeric(h.get('High', c), errors='coerce').fillna(c)
            low_s = pd.to_numeric(h.get('Low', c), errors='coerce').fillna(c)
            roll_high = float(high_s.rolling(20, min_periods=5).max().iloc[-1])
            roll_low = float(low_s.rolling(20, min_periods=5).min().iloc[-1])
            range_width = max(roll_high - roll_low, 1e-9)
            range_pos = (close - roll_low) / range_width
            range_top = max(0.0, (roll_high - close) / max(abs(close), 1e-9))
            range_bottom = max(0.0, (close - roll_low) / max(abs(close), 1e-9))
            center = (roll_high + roll_low) / 2.0
            center_dist = abs(close - center) / max(abs(close), 1e-9)
        else:
            range_pos = 0.5
            range_top = 0.0
            range_bottom = 0.0
            center_dist = 0.0
            range_width = bb_width

        mean_rev = max(0.0, (1.0 - abs(range_pos - 0.5) * 2.0)) * max(0.0, (60.0 - abs(rsi - 50.0)) / 60.0)
        exhaustion = max(0.0, abs(range_pos - 0.5) * 2.0) * max(0.0, abs(rsi - 50.0) / 50.0)
        bounce_quality = max(0.0, (1.0 - range_pos) * max(0.0, lower_shadow) * max(0.0, 1.0 - abs(intraday_return)))
        fade_quality = max(0.0, range_pos * max(0.0, upper_shadow) * max(0.0, 1.0 - abs(intraday_return)))

        out = {
            'Short_Failed_Rebound': float((close < ma20) and (intraday_return < 0) and (upper_shadow > lower_shadow)),
            'Short_Weak_Bounce': float((close < ma20) and (intraday_return <= 0.01) and (rsi < 55)),
            'Short_Distribution_Pressure': float((close < ma20) and (total_ratio < 0)),
            'Short_Breakdown_Followthrough': float((close < ma20) and (gap_pct < 0) and (intraday_return <= 0)),
            'Short_Upper_Shadow_Pressure': float(max(0.0, upper_shadow - lower_shadow)),
            'Short_GapDown_Continuation': float((gap_pct < 0) and (close <= open_)),
            'Short_Below_MA20_FailedRetake': float((close < ma20) and (high < ma20)),
            'Short_RS_Weakness': float(max(0.0, -rs_market)),
            'Range_Position_Pct': float(min(max(range_pos, 0.0), 1.0)),
            'Distance_To_Range_Top': float(range_top),
            'Distance_To_Range_Bottom': float(range_bottom),
            'Range_Mean_Reversion_Score': float(mean_rev),
            'Range_Exhaustion_Score': float(exhaustion),
            'Range_Width_Pct': float(range_width / max(abs(close), 1e-9) if close else 0.0),
            'Range_Center_Distance': float(center_dist),
            'Range_Bounce_Quality': float(bounce_quality),
            'Range_Fade_Quality': float(fade_quality),
        }
        try:
            from fts_regime_service import RegimeService
            regime_row = RegimeService().build_regime_row(row, history_df=history_df)
            out.update({k: safe_float(v, 0.0) for k, v in regime_row.items()})
        except Exception as exc:
            record_issue('feature_service', 'compute_regime_features', exc, severity='ERROR', fail_mode='fail_closed')
            out.setdefault('Range_Confidence', 0.0)
            out.setdefault('Trend_Confidence', 0.0)
            out.setdefault('Range_Width_Pctl', 0.5)
            out.setdefault('MA_Slope_Flatness', 0.0)
            out.setdefault('BB_Width_Pctl', 0.5)
            out.setdefault('ADX_Low_Regime_Flag', 0.0)
        return out

    def load_selected_features(self, mode: str = 'live', include_directional: bool | None = None) -> list[str]:
        shared = self._load_pickle_feature_list(self.selected_features_path)
        if include_directional is None:
            include_directional = self._allow_directional_runtime(mode)
        if not include_directional:
            return self._filter_selected_features_for_runtime(shared, mode=mode)
        directional = self.load_directional_selected_features()
        merged = self._merge_selected_feature_sets(shared, directional)
        return self._filter_selected_features_for_runtime(merged, mode=mode)

    def load_training_feature_registry(self) -> pd.DataFrame:
        if self.training_registry_csv.exists():
            try:
                return pd.read_csv(self.training_registry_csv, encoding='utf-8-sig')
            except Exception as exc:
                record_issue('feature_service', 'training_registry_utf8_read_failed', exc, severity='WARNING', fail_mode='fail_open', context={'path': str(self.training_registry_csv)})
                return pd.read_csv(self.training_registry_csv)
        return pd.DataFrame(columns=['feature_name', 'source', 'role', 'selected', 'present_in_sample'])

    def write_training_feature_registry(
        self,
        sample_row: Mapping[str, Any] | None = None,
        dataset_columns: Sequence[str] | None = None,
        selected_features: Sequence[str] | None = None,
    ) -> tuple[Path, dict[str, Any]]:
        selected = self._filter_selected_features_for_runtime(selected_features or self.load_selected_features(mode='live', include_directional=True), mode='live')
        dataset_cols = [str(c) for c in (dataset_columns or list(sample_row.keys()) if sample_row else [])]
        meta_cols = {
            'Ticker', 'Ticker SYMBOL', 'Date', 'Setup', 'Setup_Tag', 'Regime',
            'Label', 'Label_Y', 'Target_Return', 'Future_Return_Pct', 'Entry_Price',
            'Entry_Price_Basis', 'Exit_Price', 'Entry_Date', 'Exit_Date', 'Direction',
            'Stop_Hit', 'Hold_Days', 'Touched_TP', 'Touched_SL', 'Label_Reason',
            'Label_Exit_Type', 'Favorable_Move_Pct', 'Adverse_Move_Pct',
            'Max_Favorable_Excursion', 'Max_Adverse_Excursion',
            'Realized_Return_After_Cost', 'Mounted_Feature_Count'
        }
        rows = []
        selected_set = set(selected)
        dataset_set = set(dataset_cols)
        for col in sorted(dataset_set | selected_set):
            source = 'selected_features' if col in selected_set else 'dataset'
            role = 'candidate_feature'
            if col.startswith('MOUNT__'):
                role = 'mounted_feature'
            elif col in meta_cols:
                role = 'meta'
            rows.append({
                'feature_name': col,
                'source': source,
                'role': role,
                'selected': int(col in selected_set),
                'present_in_sample': int(col in dataset_set),
            })
        df = pd.DataFrame(rows)
        df.to_csv(self.training_registry_csv, index=False, encoding='utf-8-sig')
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'selected_feature_count': int(len(selected)),
            'registry_row_count': int(len(df)),
            'selected_features_present': bool(selected),
            'status': 'training_feature_registry_ready',
            'feature_manifest_path': str(self.feature_manifest_path),
        }
        self.training_registry_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.training_registry_csv, payload


    @staticmethod
    def _read_json_if_exists(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception as exc:
            record_issue('feature_service', 'read_json_if_exists', exc, severity='WARNING', fail_mode='fail_open', context={'path': str(path)})
            return {}
    
    def _artifact_status(self, path: Path, required_columns: Sequence[str] | None = None) -> dict[str, Any]:
        exists = path.exists()
        status = {
            'path': str(path),
            'exists': bool(exists),
            'fresh': False,
            'age_days': None,
            'missing_columns': [],
            'row_count': None,
            'coverage_ratio': None,
            'source_data_cutoff': None,
            'status': 'missing',
            'ok': False,
        }
        if not exists:
            return status
        try:
            age_seconds = max(0.0, __import__('time').time() - path.stat().st_mtime)
            age_days = age_seconds / 86400.0
            status['age_days'] = round(age_days, 4)
            status['fresh'] = bool(age_days <= float(self.artifact_max_age_days))
        except Exception as exc:
            record_issue('feature_service', 'artifact_stat_failed', exc, severity='WARNING', fail_mode='fail_open', context={'path': str(path)})
            status['fresh'] = False
        if path.suffix.lower() == '.json':
            payload = self._read_json_if_exists(path)
            if isinstance(payload, dict):
                status['source_data_cutoff'] = payload.get('as_of') or payload.get('generated_at') or payload.get('snapshot_date')
                status['coverage_ratio'] = payload.get('coverage_ratio')
                status['row_count'] = payload.get('row_count') or payload.get('total_count')
        elif path.suffix.lower() == '.csv':
            try:
                df = pd.read_csv(path)
                status['row_count'] = int(len(df))
                if required_columns:
                    status['missing_columns'] = [c for c in required_columns if c not in df.columns]
                status['coverage_ratio'] = 1.0 if status['row_count'] else 0.0
                if 'Date' in df.columns and not df.empty:
                    status['source_data_cutoff'] = str(df['Date'].iloc[-1])
            except Exception as exc:
                record_issue('feature_service', 'artifact_csv_read_failed', exc, severity='ERROR', fail_mode='fail_closed', context={'path': str(path)})
                if required_columns:
                    status['missing_columns'] = list(required_columns)
        elif required_columns:
            status['missing_columns'] = list(required_columns)
        if not status['fresh']:
            status['status'] = 'stale'
        elif status['missing_columns']:
            status['status'] = 'partial'
        else:
            status['status'] = 'ready'
        status['ok'] = bool(status['exists'] and status['fresh'] and not status['missing_columns'])
        return status
    
    def build_runtime_artifact_status(self) -> dict[str, Any]:
        required = {
            'event_runtime': self._artifact_status(PATHS.runtime_dir / 'event_calendar_service.json'),
            'event_table': self._artifact_status(PATHS.data_dir / 'feature_event_calendar.csv', ['ticker', 'event_date', 'event_type']),
            'percentile_runtime': self._artifact_status(PATHS.runtime_dir / 'cross_sectional_percentile_service.json'),
            'percentile_snapshot': self._artifact_status(PATHS.data_dir / 'feature_cross_section_snapshot.csv', ['Ticker SYMBOL', 'RS_vs_Market_20_Pctl', 'Revenue_YoY_Pctl', 'Chip_Total_Ratio_Pctl']),
            'selected_features': self._artifact_status(self.selected_features_path),
            'selected_features_long': self._artifact_status(self.directional_selected_feature_paths['LONG']),
            'selected_features_short': self._artifact_status(self.directional_selected_feature_paths['SHORT']),
            'selected_features_range': self._artifact_status(self.directional_selected_feature_paths['RANGE']),
        }
        ok = all(v.get('ok', False) for v in required.values())
        return {
            'generated_at': now_str(),
            'artifact_max_age_days': self.artifact_max_age_days,
            'require_runtime_artifacts': bool(getattr(CONFIG, 'feature_parity_require_runtime_artifacts', True)),
            'artifacts': required,
            'runtime_artifacts_ready': ok,
        }
    
    def write_feature_manifest(
        self,
        sample_row: Mapping[str, Any] | None = None,
        dataset_columns: Sequence[str] | None = None,
        selected_features: Sequence[str] | None = None,
    ) -> tuple[Path, dict[str, Any]]:
        selected = self._filter_selected_features_for_runtime(selected_features or self.load_selected_features(mode='live', include_directional=True), mode='live')
        dataset_cols = [str(c) for c in (dataset_columns or list(sample_row.keys()) if sample_row else [])]
        directional_by_scope = {scope: self._filter_selected_features_for_runtime(cols, mode='live') for scope, cols in self.load_directional_selected_features().items()}
        selected_directional = [c for c in selected if self._is_directional_feature_name(c) or c in APPROVED_LIVE_DIRECTIONAL_FEATURES]
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'selected_features': selected,
            'shared_selected_features': self._filter_selected_features_for_runtime(self._load_pickle_feature_list(self.selected_features_path), mode='live'),
            'directional_selected_features_by_scope': directional_by_scope,
            'selected_feature_count': int(len(selected)),
            'dataset_columns': dataset_cols,
            'dataset_column_count': int(len(dataset_cols)),
            'strict_feature_parity': bool(getattr(CONFIG, 'strict_feature_parity', True)),
            'selected_features_required_for_live': bool(getattr(CONFIG, 'selected_features_required_for_live', True)),
            'directional_features_used_in_training': bool(selected_directional) and bool(getattr(CONFIG, 'enable_directional_features_in_training', False)),
            'directional_features_required_in_live': bool(selected_directional) and bool(getattr(CONFIG, 'enable_directional_features_in_live', True)),
            'directional_selected_features': selected_directional,
            'approved_live_directional_features': sorted(set(selected_directional).intersection(APPROVED_LIVE_DIRECTIONAL_FEATURES)),
            'live_feature_policy': str(getattr(CONFIG, 'live_feature_policy', 'shared_plus_vetted_directional')),
            'required_runtime_artifacts': [
                'event_calendar_service.json',
                'feature_event_calendar.csv',
                'cross_sectional_percentile_service.json',
                'feature_cross_section_snapshot.csv',
                str(self.selected_features_path.name),
            ],
            'status': 'training_feature_manifest_ready',
        }
        self.feature_manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.feature_manifest_path, payload
    
    def load_feature_manifest(self) -> dict[str, Any]:
        return self._read_json_if_exists(self.feature_manifest_path)
    
    def validate_live_feature_parity(
        self,
        selected_features: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        selected = self._filter_selected_features_for_runtime(selected_features or self.load_selected_features())
        manifest = self.load_feature_manifest()
        artifact_status = self.build_runtime_artifact_status()
        selected_from_manifest = [str(x) for x in manifest.get('selected_features', []) if str(x).strip()]
        missing_from_live = [x for x in selected_from_manifest if x not in selected]
        extra_in_live = [x for x in selected if x not in selected_from_manifest]
        directional_required = bool(manifest.get('directional_features_required_in_live', False))
        live_directional_enabled = bool(getattr(CONFIG, 'enable_directional_features_in_live', False))
        manifest_present = bool(manifest)
        checks = {
            'manifest_present': manifest_present,
            'manifest_selected_feature_count': int(len(selected_from_manifest)),
            'selected_feature_count_live': int(len(selected)),
            'selected_features_match': (set(selected) == set(selected_from_manifest)) if manifest_present else False,
            'missing_from_live': missing_from_live,
            'extra_in_live': extra_in_live,
            'directional_required_in_live': directional_required,
            'directional_live_enabled': live_directional_enabled,
            'directional_alignment_ok': (not directional_required) or live_directional_enabled,
            'runtime_artifacts_ready': bool(artifact_status.get('runtime_artifacts_ready', False)),
            'artifact_status': artifact_status,
        }
        checks['status'] = 'ready' if bool(
            checks['manifest_present']
            and checks['selected_features_match']
            and checks['directional_alignment_ok']
            and (checks['runtime_artifacts_ready'] or not bool(getattr(CONFIG, 'feature_parity_require_runtime_artifacts', True)))
        ) else 'blocked'
        checks['ok'] = checks['status'] == 'ready'
        return checks

    def _combo_feature(self, name: str, features: Mapping[str, Any]) -> float:
        parts = [p for p in str(name).split('_X_') if p]
        value = 1.0
        for part in parts:
            value *= safe_float(features.get(part, 0.0), 0.0)
        return float(value)

    def _write_parity_status(self, payload: dict[str, Any]) -> None:
        self.parity_status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

    def select_live_features(
        self,
        features: Mapping[str, Any],
        selected_features: Sequence[str] | None = None,
        strict: bool | None = None,
    ) -> dict[str, float]:
        picked = self._filter_selected_features_for_runtime(selected_features or self.load_selected_features(mode='live', include_directional=True), mode='live')
        strict_mode = bool(getattr(CONFIG, 'strict_feature_parity', True)) if strict is None else bool(strict)

        if not picked or len(picked) < self.min_live_feature_count:
            payload = {
                'generated_at': now_str(),
                'module_version': self.MODULE_VERSION,
                'strict_feature_parity': strict_mode,
                'selected_features_present': False if not picked else True,
                'fallback_all_features_blocked': bool(strict_mode),
                'selected_feature_count': int(len(picked)),
                'selected_feature_min_required': int(self.min_live_feature_count),
                'status': 'selected_features_missing',
            }
            self._write_parity_status(payload)
            if strict_mode:
                return {}
            return {k: safe_float(v, 0.0) for k, v in features.items()}

        out: dict[str, float] = {}
        missing: list[str] = []
        allow_directional_live = self._allow_directional_runtime('live')
        require_approved_live = bool(getattr(CONFIG, 'directional_live_require_approved_features', True))
        for key in picked:
            if self._is_directional_feature_name(key) and not allow_directional_live:
                missing.append(key)
                continue
            if self._is_directional_feature_name(key) and require_approved_live and not self._is_live_safe_feature_name(key):
                missing.append(key)
                continue
            if (not self._is_directional_feature_name(key)) and (not self._is_live_safe_feature_name(key)):
                missing.append(key)
                continue
            if '_X_' in key:
                out[key] = self._combo_feature(key, features)
                continue
            if key not in features:
                missing.append(key)
            out[key] = safe_float(features.get(key, 0.0), 0.0)

        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'strict_feature_parity': strict_mode,
            'selected_features_present': True,
            'selected_feature_count': int(len(picked)),
            'mounted_feature_count': int(len(out)),
            'missing_feature_count': int(len(missing)),
            'missing_features_sample': missing[:20],
            'status': 'feature_parity_locked',
        }
        self._write_parity_status(payload)
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
        open_ = pd.to_numeric(out['Open'], errors='coerce').fillna(close) if 'Open' in out.columns else close.copy()
        volume = pd.to_numeric(out['Volume'], errors='coerce').fillna(0.0) if 'Volume' in out.columns else pd.Series([0.0] * len(out), index=out.index, dtype=float)

        atr14 = self._compute_atr(out, 14)
        out['ATR14'] = atr14
        out['ATR_Pct'] = (atr14 / close.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out['ATR_Pctl_252'] = self._rolling_percentile(out['ATR_Pct'], 252)
        ret = close.pct_change().fillna(0.0)
        out['RealizedVol_20'] = ret.rolling(20, min_periods=5).std().fillna(0.0) * np.sqrt(20)
        out['RealizedVol_60'] = ret.rolling(60, min_periods=10).std().fillna(0.0) * np.sqrt(60)
        out['Gap_Pct'] = ((open_ - close.shift(1)) / close.shift(1).replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out['Overnight_Return'] = out['Gap_Pct']
        out['Intraday_Return'] = ((close - open_) / open_.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out['Turnover_Proxy'] = (close * volume).fillna(0.0)
        out['ADV20_Proxy'] = volume.rolling(20, min_periods=5).mean().fillna(0.0)
        out['DollarVol20_Proxy'] = (close * volume).rolling(20, min_periods=5).mean().fillna(0.0)
        out['Volume_Z20'] = ((volume - volume.rolling(20, min_periods=5).mean()) / volume.rolling(20, min_periods=5).std().replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out['Return_Z20'] = ((ret - ret.rolling(20, min_periods=5).mean()) / ret.rolling(20, min_periods=5).std().replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        out['Turnover_Pctl'] = self._rolling_percentile(out['Turnover_Proxy'], 252)
        out['ADV20_Pctl'] = self._rolling_percentile(out['ADV20_Proxy'], 252)
        out['ATR_Pct_Pctl'] = self._rolling_percentile(out['ATR_Pct'], 252)
        out['RealizedVol_20_Pctl'] = self._rolling_percentile(out['RealizedVol_20'], 252)
        out = self._compute_structural_bias_frame(out)

        # 正式 class 方法：原本尾端 monkey patch 的提前布局 / 出場 / regime transition 特徵，
        # 現在直接落在 FeatureService.enrich_from_history() 內，不再靠執行期覆寫方法。
        for col, default in [
            ('Buy_Score', 0.0), ('Sell_Score', 0.0), ('Score_Gap', 0.0),
            ('ADX14', 0.0), ('MACD_Hist', 0.0), ('RSI', 50.0),
            ('BB_Width', 0.0), ('ATR_Pct', 0.0), ('Volume_Z20', 0.0),
            ('Foreign_Ratio', 0.0), ('Total_Ratio', 0.0), ('AI_Proba', 0.5),
        ]:
            if col not in out.columns:
                out[col] = default
        out['Buy_Score_Slope_3d'] = self._history_delta_series(out, 'Buy_Score', 3)
        out['Buy_Score_Slope_5d'] = self._history_delta_series(out, 'Buy_Score', 5)
        out['Sell_Score_Slope_3d'] = self._history_delta_series(out, 'Sell_Score', 3)
        out['Sell_Score_Slope_5d'] = self._history_delta_series(out, 'Sell_Score', 5)
        out['Score_Gap_Slope_3d'] = self._history_delta_series(out, 'Score_Gap', 3)
        out['Score_Gap_Slope_5d'] = self._history_delta_series(out, 'Score_Gap', 5)
        out['ADX_Delta_3d'] = self._history_delta_series(out, 'ADX14', 3)
        out['MACD_Hist_Delta_3d'] = self._history_delta_series(out, 'MACD_Hist', 3)
        out['Volume_Z20_Delta'] = self._history_delta_series(out, 'Volume_Z20', 3)
        out['Foreign_Ratio_Delta_3d'] = self._history_delta_series(out, 'Foreign_Ratio', 3)
        out['Total_Ratio_Delta_3d'] = self._history_delta_series(out, 'Total_Ratio', 3)

        rsi = pd.to_numeric(out['RSI'], errors='coerce').fillna(50.0)
        bb_width = pd.to_numeric(out['BB_Width'], errors='coerce').fillna(0.0)
        atr_pct = pd.to_numeric(out['ATR_Pct'], errors='coerce').fillna(0.0)
        out['RSI_Reclaim_Speed'] = ((rsi - rsi.shift(3).fillna(rsi)) / 20.0).clip(-1.0, 1.0).fillna(0.0)
        out['BB_Squeeze_Release'] = self._clip01_series((bb_width.rolling(3, min_periods=1).mean() - bb_width.rolling(10, min_periods=3).mean()).fillna(0.0) * 8.0)
        out['ATR_Expansion_Start'] = self._clip01_series((atr_pct - atr_pct.rolling(10, min_periods=3).mean()).fillna(0.0) * 20.0)

        if 'Transition_Label' not in out.columns or 'Hysteresis_Regime_Label' not in out.columns:
            try:
                from fts_regime_service import RegimeService
                out = RegimeService().enrich_dataframe(out)
            except Exception as exc:
                record_issue('feature_service', 'regime_enrich_dataframe_failed', exc, severity='ERROR', fail_mode='fail_closed')
                out['FeatureService_Regime_Enrich_Failed'] = True
        return out



    @staticmethod
    def _history_delta_series(df: pd.DataFrame, col: str, periods: int) -> pd.Series:
        if df is None or df.empty or col not in df.columns:
            return pd.Series([0.0] * len(df), index=df.index if df is not None else None, dtype=float)
        s = pd.to_numeric(df[col], errors='coerce').ffill().fillna(0.0)
        return (s - s.shift(periods)).fillna(0.0)

    @staticmethod
    def _clip01_series(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors='coerce').fillna(0.0).clip(0.0, 1.0)

    @staticmethod
    def _history_delta(history_df: pd.DataFrame | None, col: str, periods: int = 3) -> float:
        if history_df is None or history_df.empty or col not in history_df.columns or len(history_df) <= periods:
            return 0.0
        s = pd.to_numeric(history_df[col], errors='coerce').ffill().fillna(0.0)
        try:
            return float(s.iloc[-1] - s.iloc[-(periods + 1)])
        except Exception as exc:
            record_issue('feature_service', 'history_delta_failed', exc, severity='WARNING', fail_mode='fail_open', context={'column': col, 'periods': periods})
            return 0.0

    def _compute_exit_timing_features(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, float]:
        trend_conf = safe_float(row.get('Trend_Confidence', 0.5), 0.5)
        range_conf = safe_float(row.get('Range_Confidence', 0.5), 0.5)
        ai_proba = safe_float(row.get('AI_Proba', row.get('訊號信心分數(%)', 50.0) / 100.0), 0.5)
        score_gap = safe_float(row.get('Score_Gap', 0.0), 0.0)
        upper_shadow = safe_float(row.get('Upper_Shadow_Pct', row.get('Upper_Shadow', 0.0)), 0.0)
        intraday = safe_float(row.get('Intraday_Return', 0.0), 0.0)
        gap_pct = safe_float(row.get('Gap_Pct', 0.0), 0.0)
        total_ratio = safe_float(row.get('Total_Ratio', 0.0), 0.0)
        foreign_ratio = safe_float(row.get('Foreign_Ratio', 0.0), 0.0)
        rsi = safe_float(row.get('RSI', 50.0), 50.0)
        macd_hist = safe_float(row.get('MACD_Hist', 0.0), 0.0)

        trend_conf_delta = self._history_delta(history_df, 'Trend_Confidence', 3)
        range_conf_delta = self._history_delta(history_df, 'Range_Confidence', 3)
        proba_delta = self._history_delta(history_df, 'AI_Proba', 3)
        foreign_delta = self._history_delta(history_df, 'Foreign_Ratio', 3)
        total_delta = self._history_delta(history_df, 'Total_Ratio', 3)
        macd_delta = self._history_delta(history_df, 'MACD_Hist', 3)

        breakout_risk = (
            max(0.0, upper_shadow) * 0.20
            + max(0.0, -intraday) * 0.20
            + max(0.0, -gap_pct) * 0.10
            + max(0.0, -macd_delta) * 0.20
            + max(0.0, -proba_delta) * 0.15
            + max(0.0, -trend_conf_delta) * 0.15
        )
        reversal_risk = (
            max(0.0, (rsi - 68.0) / 20.0) * 0.15
            + max(0.0, -foreign_delta) * 0.20
            + max(0.0, -total_delta) * 0.20
            + max(0.0, range_conf - trend_conf) * 0.20
            + max(0.0, -score_gap) * 0.10
            + max(0.0, -macd_hist) * 0.15
        )
        exit_trend_decay = max(0.0, -trend_conf_delta) * 0.45 + max(0.0, -proba_delta) * 0.35 + max(0.0, -macd_delta) * 0.20
        exit_chip_weakening = max(0.0, -foreign_delta) * 0.55 + max(0.0, -total_delta) * 0.45
        exit_regime_det = max(0.0, range_conf - trend_conf) * 0.70 + max(0.0, -trend_conf_delta) * 0.30
        hazard_raw = (
            breakout_risk * 0.30
            + reversal_risk * 0.25
            + exit_trend_decay * 0.20
            + exit_chip_weakening * 0.10
            + exit_regime_det * 0.10
            + max(0.0, 0.55 - ai_proba) * 0.25
        )
        hazard = max(0.0, min(1.0, hazard_raw * 1.85))
        stop_tighten = max(0.35, 1.0 - hazard * 0.60)
        return {
            'Breakout_Risk_Next3': round(float(breakout_risk), 6),
            'Reversal_Risk_Next3': round(float(reversal_risk), 6),
            'Exit_Trend_Decay': round(float(exit_trend_decay), 6),
            'Exit_Chip_Weakening': round(float(exit_chip_weakening), 6),
            'Exit_Regime_Deterioration': round(float(exit_regime_det), 6),
            'Exit_Hazard_Score': round(float(hazard), 6),
            'Exit_Stop_Tighten_Suggested': round(float(stop_tighten), 6),
            'Trend_Confidence_Delta': round(float(trend_conf_delta), 6),
            'Range_Confidence_Delta': round(float(range_conf_delta), 6),
            'Proba_Delta_3d': round(float(proba_delta), 6),
        }

    def extract_ai_features(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None, ticker: str | None = None, as_of_date: Any | None = None) -> dict[str, Any]:
        features = {
            'Body_Pct': safe_float(row.get('Body_Pct', row.get('K_Body_Ratio', 0.0)), 0.0),
            'Upper_Shadow_Pct': safe_float(row.get('Upper_Shadow_Pct', row.get('Upper_Shadow', 0.0)), 0.0),
            'Lower_Shadow_Pct': safe_float(row.get('Lower_Shadow_Pct', row.get('Lower_Shadow', 0.0)), 0.0),
            'Dist_MA20_Pct': safe_float(row.get('Dist_MA20_Pct', row.get('Distance_to_MA20', 0.0)), 0.0),
            'Volume_Ratio': safe_float(row.get('Volume_Ratio', row.get('Vol_Ratio', 0.0)), 0.0),
            'BB_Width': safe_float(row.get('BB_Width', 0.0), 0.0),
            'RSI': safe_float(row.get('RSI', 0.0), 0.0),
            'MACD_Hist': safe_float(row.get('MACD_Hist', 0.0), 0.0),
            'ADX': safe_float(row.get('ADX14', row.get('ADX', 0.0)), 0.0),
            'Foreign_Ratio': safe_float(row.get('Foreign_Ratio', 0.0), 0.0),
            'Trust_Ratio': safe_float(row.get('Trust_Ratio', 0.0), 0.0),
            'Total_Ratio': safe_float(row.get('Total_Ratio', 0.0), 0.0),
            'Foreign_Continuous': safe_float(row.get('Foreign_Continuous', 0.0), 0.0),
            'Trust_Continuous': safe_float(row.get('Trust_Continuous', 0.0), 0.0),
            'Weighted_Buy_Score': safe_float(row.get('Weighted_Buy_Score', row.get('Buy_Score', 0.0)), 0.0),
            'Weighted_Sell_Score': safe_float(row.get('Weighted_Sell_Score', row.get('Sell_Score', 0.0)), 0.0),
            'Score_Gap': safe_float(row.get('Score_Gap', 0.0), 0.0),
            'Signal_Conflict': safe_float(row.get('Signal_Conflict', 0.0), 0.0),
            'Vol_Squeeze': safe_float(row.get('Vol_Squeeze', 0.0), 0.0),
            'Absorption': safe_float(row.get('Absorption', 0.0), 0.0),
            'MR_Long_Spring': safe_float(row.get('MR_Long_Spring', 0.0), 0.0),
            'MR_Short_Trap': safe_float(row.get('MR_Short_Trap', 0.0), 0.0),
            'MR_Long_Accumulation': safe_float(row.get('MR_Long_Accumulation', 0.0), 0.0),
            'MR_Short_Distribution': safe_float(row.get('MR_Short_Distribution', 0.0), 0.0),
            'buy_c2': safe_float(row.get('buy_c2', 0.0), 0.0),
            'buy_c3': safe_float(row.get('buy_c3', 0.0), 0.0),
            'buy_c4': safe_float(row.get('buy_c4', 0.0), 0.0),
            'buy_c5': safe_float(row.get('buy_c5', 0.0), 0.0),
            'buy_c6': safe_float(row.get('buy_c6', 0.0), 0.0),
            'buy_c7': safe_float(row.get('buy_c7', 0.0), 0.0),
            'buy_c8': safe_float(row.get('buy_c8', 0.0), 0.0),
            'buy_c9': safe_float(row.get('buy_c9', 0.0), 0.0),
            'sell_c2': safe_float(row.get('sell_c2', 0.0), 0.0),
            'sell_c3': safe_float(row.get('sell_c3', 0.0), 0.0),
            'sell_c4': safe_float(row.get('sell_c4', 0.0), 0.0),
            'sell_c5': safe_float(row.get('sell_c5', 0.0), 0.0),
            'sell_c6': safe_float(row.get('sell_c6', 0.0), 0.0),
            'sell_c7': safe_float(row.get('sell_c7', 0.0), 0.0),
            'sell_c8': safe_float(row.get('sell_c8', 0.0), 0.0),
            'sell_c9': safe_float(row.get('sell_c9', 0.0), 0.0),
        }
        if history_df is not None and not history_df.empty:
            enriched = self.enrich_from_history(history_df)
            latest = enriched.iloc[-1].to_dict()
            for k in ['ATR14','ATR_Pct','ATR_Pctl_252','RealizedVol_20','RealizedVol_60','Gap_Pct','Overnight_Return','Intraday_Return','Turnover_Proxy','ADV20_Proxy','DollarVol20_Proxy','Volume_Z20','Return_Z20','MA20','MA60','MA120','Dist_MA60_Pct','Dist_MA120_Pct','MA20_vs_MA60_Pct','MA60_vs_MA120_Pct','MA20_Slope_5','MA60_Slope_10','Structural_Long_Bias','Structural_Short_Bias','Structural_Range_Bias']:
                features[k] = safe_float(latest.get(k, features.get(k, 0.0)), features.get(k, 0.0))
        passthrough = ['RS_vs_Market_20','RS_vs_Sector_20','RS_vs_Market_20_Pctl','RS_vs_Sector_20_Pctl','Revenue_YoY','Revenue_YoY_Pctl','Chip_Total_Ratio','Chip_Total_Ratio_Pctl','Turnover_Pctl','ADV20_Pctl','ATR_Pct_Pctl','RealizedVol_20_Pctl','Event_Days_Since_Revenue','Event_Days_To_Revenue','Revenue_Window_1','Revenue_Window_3','Revenue_Window_5','Revenue_Window_10','Event_Days_Since_Earnings','Event_Days_To_Earnings','Earnings_Window_3','Earnings_Window_7','Earnings_Window_14','Earnings_Window_Flag','Dividend_Window_7']
        for k in passthrough:
            if k in row:
                features[k] = safe_float(row.get(k, 0), 0)
        features['Revenue_YoY_Rank'] = safe_float(row.get('Revenue_YoY_Rank', row.get('Revenue_YoY_Pctl', 0.5)), 0.5)
        features['Chip_Total_Ratio_Rank'] = safe_float(row.get('Chip_Total_Ratio_Rank', row.get('Chip_Total_Ratio_Pctl', 0.5)), 0.5)
        features['Regime_TrendStrength_X_ScoreGap'] = safe_float(row.get('Regime_TrendStrength_X_ScoreGap', (features['ADX'] / 100.0) * features['Score_Gap']), 0)
        features['Volatility_X_SignalConflict'] = safe_float(row.get('Volatility_X_SignalConflict', features.get('ATR_Pctl_252', 0.0) * features['Signal_Conflict']), 0)
        directional = self._compute_directional_features(row, history_df=history_df)
        if bool(getattr(CONFIG, 'enable_directional_features_in_training', False)):
            features.update(directional)
        features.update(self._compute_exit_timing_features({**row, **features}, history_df=history_df))

        hist_enriched = pd.DataFrame()
        if isinstance(history_df, pd.DataFrame) and not history_df.empty:
            try:
                hist_enriched = self.enrich_from_history(history_df)
                if 'AI_Proba' not in hist_enriched.columns:
                    hist_enriched['AI_Proba'] = pd.Series([0.5] * len(hist_enriched), index=hist_enriched.index, dtype=float)
            except Exception as exc:
                record_issue('feature_service', 'extract_ai_features_history_enrich_failed', exc, severity='ERROR', fail_mode='fail_closed')
                hist_enriched = pd.DataFrame()

        def _latest(name: str, default: float = 0.0) -> float:
            if name in row:
                return safe_float(row.get(name, default), default)
            if not hist_enriched.empty and name in hist_enriched.columns:
                return safe_float(hist_enriched[name].iloc[-1], default)
            return default

        features.update({
            'Buy_Score_Slope_3d': _latest('Buy_Score_Slope_3d', 0.0),
            'Buy_Score_Slope_5d': _latest('Buy_Score_Slope_5d', 0.0),
            'Sell_Score_Slope_3d': _latest('Sell_Score_Slope_3d', 0.0),
            'Sell_Score_Slope_5d': _latest('Sell_Score_Slope_5d', 0.0),
            'Score_Gap_Slope_3d': _latest('Score_Gap_Slope_3d', 0.0),
            'Score_Gap_Slope_5d': _latest('Score_Gap_Slope_5d', 0.0),
            'ADX_Delta_3d': _latest('ADX_Delta_3d', 0.0),
            'MACD_Hist_Delta_3d': _latest('MACD_Hist_Delta_3d', 0.0),
            'RSI_Reclaim_Speed': _latest('RSI_Reclaim_Speed', 0.0),
            'BB_Squeeze_Release': _latest('BB_Squeeze_Release', 0.0),
            'ATR_Expansion_Start': _latest('ATR_Expansion_Start', 0.0),
            'Volume_Z20_Delta': _latest('Volume_Z20_Delta', 0.0),
            'Foreign_Ratio_Delta_3d': _latest('Foreign_Ratio_Delta_3d', 0.0),
            'Total_Ratio_Delta_3d': _latest('Total_Ratio_Delta_3d', 0.0),
            'Bull_Emerging_Score': _latest('Bull_Emerging_Score', 0.0),
            'Bear_Emerging_Score': _latest('Bear_Emerging_Score', 0.0),
            'Range_Compression_Score': _latest('Range_Compression_Score', 0.0),
            'Breakout_Readiness': _latest('Breakout_Readiness', 0.0),
            'Trend_Exhaustion_Score': _latest('Trend_Exhaustion_Score', 0.0),
            'Entry_Readiness': _latest('Entry_Readiness', 0.0),
            'Transition_Label': str(row.get('Transition_Label', hist_enriched['Transition_Label'].iloc[-1] if not hist_enriched.empty and 'Transition_Label' in hist_enriched.columns else 'Stable')),
            'Next_Regime_Prob_Bull': _latest('Next_Regime_Prob_Bull', 0.34),
            'Next_Regime_Prob_Bear': _latest('Next_Regime_Prob_Bear', 0.33),
            'Next_Regime_Prob_Range': _latest('Next_Regime_Prob_Range', 0.33),
            'Hysteresis_Regime_Label': str(row.get('Hysteresis_Regime_Label', hist_enriched['Hysteresis_Regime_Label'].iloc[-1] if not hist_enriched.empty and 'Hysteresis_Regime_Label' in hist_enriched.columns else row.get('Regime', '區間盤整'))),
            'Proba_Delta_3d': _latest('Proba_Delta_3d', 0.0),
            'Trend_Confidence_Delta': _latest('Trend_Confidence_Delta', 0.0),
            'Range_Confidence_Delta': _latest('Range_Confidence_Delta', 0.0),
            'MA20': _latest('MA20', safe_float(row.get('MA20', row.get('MA_20', 0.0)), 0.0)),
            'MA60': _latest('MA60', safe_float(row.get('MA60', 0.0), 0.0)),
            'MA120': _latest('MA120', safe_float(row.get('MA120', 0.0), 0.0)),
            'Dist_MA60_Pct': _latest('Dist_MA60_Pct', 0.0),
            'Dist_MA120_Pct': _latest('Dist_MA120_Pct', 0.0),
            'MA20_vs_MA60_Pct': _latest('MA20_vs_MA60_Pct', 0.0),
            'MA60_vs_MA120_Pct': _latest('MA60_vs_MA120_Pct', 0.0),
            'MA20_Slope_5': _latest('MA20_Slope_5', 0.0),
            'MA60_Slope_10': _latest('MA60_Slope_10', 0.0),
            'Structural_Long_Bias': _latest('Structural_Long_Bias', 0.0),
            'Structural_Short_Bias': _latest('Structural_Short_Bias', 0.0),
            'Structural_Range_Bias': _latest('Structural_Range_Bias', 0.0),
            'Structural_Regime_Bias': str(row.get('Structural_Regime_Bias', hist_enriched['Structural_Regime_Bias'].iloc[-1] if not hist_enriched.empty and 'Structural_Regime_Bias' in hist_enriched.columns else 'RANGE')),
        })
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
        selected = self._filter_selected_features_for_runtime(self.load_selected_features(mode='live', include_directional=True), mode='live')
        mounted = self.select_live_features(features, selected_features=selected, strict=getattr(CONFIG, 'strict_feature_parity', True))
        mount_checks = self.validate_live_feature_parity(selected)
        mount_payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'ticker': str(ticker),
            'selected_feature_count': len(selected),
            'mounted_feature_count': len(mounted),
            'selected_features_present': bool(selected),
            'strict_feature_parity': bool(getattr(CONFIG, 'strict_feature_parity', True)),
            'official_percentile_mode': True,
            'precise_event_calendar_mode': True,
            'feature_manifest_path': str(self.feature_manifest_path),
            'manifest_checks': mount_checks,
            'status': 'live_feature_mount_ready' if mount_checks.get('ok', False) and mounted else ('live_feature_mount_blocked' if not mount_checks.get('ok', False) else 'live_feature_mount_waiting_for_selected_features'),
        }
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
        selected = self._filter_selected_features_for_runtime(self.load_selected_features(mode='live', include_directional=True), mode='live')
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'sample_feature_count': len(sample),
            'selected_features_present': bool(selected),
            'selected_feature_count': len(selected),
            'feature_buckets': {k: len(v) for k, v in FEATURE_BUCKETS.items()},
            'directional_training_enabled': bool(getattr(CONFIG, 'enable_directional_features_in_training', False)),
            'directional_live_enabled': bool(getattr(CONFIG, 'enable_directional_features_in_live', False)),
            'live_feature_groups': get_live_feature_groups(),
            'approved_live_directional_features': APPROVED_LIVE_DIRECTIONAL_FEATURES,
            'training_feature_groups': get_training_feature_groups(),
            'priority_new_features_20': PRIORITY_NEW_FEATURES_20,
            'official_percentile_mode': True,
            'precise_event_calendar_mode': True,
            'live_mount_path': str(self.live_mount_path),
            'training_registry_csv': str(self.training_registry_csv),
            'rolling_percentile_engine': 'sorted_window_fast',
            'strict_feature_parity': bool(getattr(CONFIG, 'strict_feature_parity', True)),
            'feature_manifest_path': str(self.feature_manifest_path),
            'status': 'feature_service_ready',
            'runtime_diagnostics': get_runtime_diagnostics_safe() if 'get_runtime_diagnostics_safe' in globals() else {},
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧩 feature service ready: {self.runtime_path}')
        return self.runtime_path, payload


# ===== HOTFIX: rebind mis-indented FeatureService methods =====
# This block restores methods that were accidentally nested under
# _compute_structural_bias_frame() instead of being attached to FeatureService.

FeatureService._series_or_default = _series_or_default
FeatureService._compute_structural_bias_frame = _compute_structural_bias_frame

def _compute_directional_features(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, float]:
    close = safe_float(row.get('Close', row.get('close', 0.0)), 0.0)
    open_ = safe_float(row.get('Open', row.get('open', close)), close)
    high = safe_float(row.get('High', row.get('high', max(close, open_))), max(close, open_))
    low = safe_float(row.get('Low', row.get('low', min(close, open_))), min(close, open_))
    ma20 = safe_float(row.get('MA20', row.get('MA_20', close)), close)
    rsi = safe_float(row.get('RSI', 50.0), 50.0)
    bb_width = safe_float(row.get('BB_Width', 0.0), 0.0)
    total_ratio = safe_float(row.get('Total_Ratio', 0.0), 0.0)
    upper_shadow = safe_float(row.get('Upper_Shadow_Pct', row.get('Upper_Shadow', 0.0)), 0.0)
    lower_shadow = safe_float(row.get('Lower_Shadow_Pct', row.get('Lower_Shadow', 0.0)), 0.0)
    intraday_return = safe_float(row.get('Intraday_Return', (close - open_) / open_ if open_ else 0.0), 0.0)
    gap_pct = safe_float(row.get('Gap_Pct', 0.0), 0.0)
    rs_market = safe_float(row.get('RS_vs_Market_20', 0.0), 0.0)

    if history_df is not None and not history_df.empty and len(history_df) >= 10 and 'Close' in history_df.columns:
        h = history_df.copy()
        c = pd.to_numeric(h['Close'], errors='coerce').ffill().fillna(close)
        high_s = pd.to_numeric(h.get('High', c), errors='coerce').fillna(c)
        low_s = pd.to_numeric(h.get('Low', c), errors='coerce').fillna(c)
        roll_high = float(high_s.rolling(20, min_periods=5).max().iloc[-1])
        roll_low = float(low_s.rolling(20, min_periods=5).min().iloc[-1])
        range_width = max(roll_high - roll_low, 1e-9)
        range_pos = (close - roll_low) / range_width
        range_top = max(0.0, (roll_high - close) / max(abs(close), 1e-9))
        range_bottom = max(0.0, (close - roll_low) / max(abs(close), 1e-9))
        center = (roll_high + roll_low) / 2.0
        center_dist = abs(close - center) / max(abs(close), 1e-9)
    else:
        range_pos = 0.5
        range_top = 0.0
        range_bottom = 0.0
        center_dist = 0.0
        range_width = bb_width

    mean_rev = max(0.0, (1.0 - abs(range_pos - 0.5) * 2.0)) * max(0.0, (60.0 - abs(rsi - 50.0)) / 60.0)
    exhaustion = max(0.0, abs(range_pos - 0.5) * 2.0) * max(0.0, abs(rsi - 50.0) / 50.0)
    bounce_quality = max(0.0, (1.0 - range_pos) * max(0.0, lower_shadow) * max(0.0, 1.0 - abs(intraday_return)))
    fade_quality = max(0.0, range_pos * max(0.0, upper_shadow) * max(0.0, 1.0 - abs(intraday_return)))

    out = {
        'Short_Failed_Rebound': float((close < ma20) and (intraday_return < 0) and (upper_shadow > lower_shadow)),
        'Short_Weak_Bounce': float((close < ma20) and (intraday_return <= 0.01) and (rsi < 55)),
        'Short_Distribution_Pressure': float((close < ma20) and (total_ratio < 0)),
        'Short_Breakdown_Followthrough': float((close < ma20) and (gap_pct < 0) and (intraday_return <= 0)),
        'Short_Upper_Shadow_Pressure': float(max(0.0, upper_shadow - lower_shadow)),
        'Short_GapDown_Continuation': float((gap_pct < 0) and (close <= open_)),
        'Short_Below_MA20_FailedRetake': float((close < ma20) and (high < ma20)),
        'Short_RS_Weakness': float(max(0.0, -rs_market)),
        'Range_Position_Pct': float(min(max(range_pos, 0.0), 1.0)),
        'Distance_To_Range_Top': float(range_top),
        'Distance_To_Range_Bottom': float(range_bottom),
        'Range_Mean_Reversion_Score': float(mean_rev),
        'Range_Exhaustion_Score': float(exhaustion),
        'Range_Width_Pct': float(range_width / max(abs(close), 1e-9) if close else 0.0),
        'Range_Center_Distance': float(center_dist),
        'Range_Bounce_Quality': float(bounce_quality),
        'Range_Fade_Quality': float(fade_quality),
    }
    try:
        from fts_regime_service import RegimeService
        regime_row = RegimeService().build_regime_row(row, history_df=history_df)
        out.update({k: safe_float(v, 0.0) for k, v in regime_row.items()})
    except Exception as exc:
        record_issue('feature_service', 'compute_regime_features', exc, severity='ERROR', fail_mode='fail_closed')
        out.setdefault('Range_Confidence', 0.0)
        out.setdefault('Trend_Confidence', 0.0)
        out.setdefault('Range_Width_Pctl', 0.5)
        out.setdefault('MA_Slope_Flatness', 0.0)
        out.setdefault('BB_Width_Pctl', 0.5)
        out.setdefault('ADX_Low_Regime_Flag', 0.0)
    return out

def load_selected_features(self, mode: str = 'live', include_directional: bool | None = None) -> list[str]:
    shared = self._load_pickle_feature_list(self.selected_features_path)
    if include_directional is None:
        include_directional = self._allow_directional_runtime(mode)
    if not include_directional:
        return self._filter_selected_features_for_runtime(shared, mode=mode)
    directional = self.load_directional_selected_features()
    merged = self._merge_selected_feature_sets(shared, directional)
    return self._filter_selected_features_for_runtime(merged, mode=mode)

def load_training_feature_registry(self) -> pd.DataFrame:
    if self.training_registry_csv.exists():
        try:
            return pd.read_csv(self.training_registry_csv, encoding='utf-8-sig')
        except Exception as exc:
            record_issue('feature_service', 'training_registry_utf8_read_failed', exc, severity='WARNING', fail_mode='fail_open', context={'path': str(self.training_registry_csv)})
            return pd.read_csv(self.training_registry_csv)
    return pd.DataFrame(columns=['feature_name', 'source', 'role', 'selected', 'present_in_sample'])

def write_training_feature_registry(
    self,
    sample_row: Mapping[str, Any] | None = None,
    dataset_columns: Sequence[str] | None = None,
    selected_features: Sequence[str] | None = None,
) -> tuple[Path, dict[str, Any]]:
    selected = self._filter_selected_features_for_runtime(selected_features or self.load_selected_features(mode='live', include_directional=True), mode='live')
    dataset_cols = [str(c) for c in (dataset_columns or list(sample_row.keys()) if sample_row else [])]
    meta_cols = {
        'Ticker', 'Ticker SYMBOL', 'Date', 'Setup', 'Setup_Tag', 'Regime',
        'Label', 'Label_Y', 'Target_Return', 'Future_Return_Pct', 'Entry_Price',
        'Entry_Price_Basis', 'Exit_Price', 'Entry_Date', 'Exit_Date', 'Direction',
        'Stop_Hit', 'Hold_Days', 'Touched_TP', 'Touched_SL', 'Label_Reason',
        'Label_Exit_Type', 'Favorable_Move_Pct', 'Adverse_Move_Pct',
        'Max_Favorable_Excursion', 'Max_Adverse_Excursion',
        'Realized_Return_After_Cost', 'Mounted_Feature_Count'
    }
    rows = []
    selected_set = set(selected)
    dataset_set = set(dataset_cols)
    for col in sorted(dataset_set | selected_set):
        source = 'selected_features' if col in selected_set else 'dataset'
        role = 'candidate_feature'
        if col.startswith('MOUNT__'):
            role = 'mounted_feature'
        elif col in meta_cols:
            role = 'meta'
        rows.append({
            'feature_name': col,
            'source': source,
            'role': role,
            'selected': int(col in selected_set),
            'present_in_sample': int(col in dataset_set),
        })
    df = pd.DataFrame(rows)
    df.to_csv(self.training_registry_csv, index=False, encoding='utf-8-sig')
    payload = {
        'generated_at': now_str(),
        'module_version': self.MODULE_VERSION,
        'selected_feature_count': int(len(selected)),
        'registry_row_count': int(len(df)),
        'selected_features_present': bool(selected),
        'status': 'training_feature_registry_ready',
        'feature_manifest_path': str(self.feature_manifest_path),
    }
    self.training_registry_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return self.training_registry_csv, payload

def _read_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception as exc:
        record_issue('feature_service', 'read_json_if_exists', exc, severity='WARNING', fail_mode='fail_open', context={'path': str(path)})
        return {}

def _artifact_status(self, path: Path, required_columns: Sequence[str] | None = None) -> dict[str, Any]:
    exists = path.exists()
    status = {
        'path': str(path),
        'exists': bool(exists),
        'fresh': False,
        'age_days': None,
        'missing_columns': [],
        'row_count': None,
        'coverage_ratio': None,
        'source_data_cutoff': None,
        'status': 'missing',
        'ok': False,
    }
    if not exists:
        return status
    try:
        age_seconds = max(0.0, __import__('time').time() - path.stat().st_mtime)
        age_days = age_seconds / 86400.0
        status['age_days'] = round(age_days, 4)
        status['fresh'] = bool(age_days <= float(self.artifact_max_age_days))
    except Exception as exc:
        record_issue('feature_service', 'artifact_stat_failed', exc, severity='WARNING', fail_mode='fail_open', context={'path': str(path)})
        status['fresh'] = False
    if path.suffix.lower() == '.json':
        payload = self._read_json_if_exists(path)
        if isinstance(payload, dict):
            status['source_data_cutoff'] = payload.get('as_of') or payload.get('generated_at') or payload.get('snapshot_date')
            status['coverage_ratio'] = payload.get('coverage_ratio')
            status['row_count'] = payload.get('row_count') or payload.get('total_count')
    elif path.suffix.lower() == '.csv':
        try:
            df = pd.read_csv(path)
            status['row_count'] = int(len(df))
            if required_columns:
                status['missing_columns'] = [c for c in required_columns if c not in df.columns]
            status['coverage_ratio'] = 1.0 if status['row_count'] else 0.0
            if 'Date' in df.columns and not df.empty:
                status['source_data_cutoff'] = str(df['Date'].iloc[-1])
        except Exception as exc:
            record_issue('feature_service', 'artifact_csv_read_failed', exc, severity='ERROR', fail_mode='fail_closed', context={'path': str(path)})
            if required_columns:
                status['missing_columns'] = list(required_columns)
    elif required_columns:
        status['missing_columns'] = list(required_columns)
    if not status['fresh']:
        status['status'] = 'stale'
    elif status['missing_columns']:
        status['status'] = 'partial'
    else:
        status['status'] = 'ready'
    status['ok'] = bool(status['exists'] and status['fresh'] and not status['missing_columns'])
    return status

def build_runtime_artifact_status(self) -> dict[str, Any]:
    required = {
        'event_runtime': self._artifact_status(PATHS.runtime_dir / 'event_calendar_service.json'),
        'event_table': self._artifact_status(PATHS.data_dir / 'feature_event_calendar.csv', ['ticker', 'event_date', 'event_type']),
        'percentile_runtime': self._artifact_status(PATHS.runtime_dir / 'cross_sectional_percentile_service.json'),
        'percentile_snapshot': self._artifact_status(PATHS.data_dir / 'feature_cross_section_snapshot.csv', ['Ticker SYMBOL', 'RS_vs_Market_20_Pctl', 'Revenue_YoY_Pctl', 'Chip_Total_Ratio_Pctl']),
        'selected_features': self._artifact_status(self.selected_features_path),
        'selected_features_long': self._artifact_status(self.directional_selected_feature_paths['LONG']),
        'selected_features_short': self._artifact_status(self.directional_selected_feature_paths['SHORT']),
        'selected_features_range': self._artifact_status(self.directional_selected_feature_paths['RANGE']),
    }
    ok = all(v.get('ok', False) for v in required.values())
    return {
        'generated_at': now_str(),
        'artifact_max_age_days': self.artifact_max_age_days,
        'require_runtime_artifacts': bool(getattr(CONFIG, 'feature_parity_require_runtime_artifacts', True)),
        'artifacts': required,
        'runtime_artifacts_ready': ok,
    }

def write_feature_manifest(
    self,
    sample_row: Mapping[str, Any] | None = None,
    dataset_columns: Sequence[str] | None = None,
    selected_features: Sequence[str] | None = None,
) -> tuple[Path, dict[str, Any]]:
    selected = self._filter_selected_features_for_runtime(selected_features or self.load_selected_features(mode='live', include_directional=True), mode='live')
    dataset_cols = [str(c) for c in (dataset_columns or list(sample_row.keys()) if sample_row else [])]
    directional_by_scope = {scope: self._filter_selected_features_for_runtime(cols, mode='live') for scope, cols in self.load_directional_selected_features().items()}
    selected_directional = [c for c in selected if self._is_directional_feature_name(c) or c in APPROVED_LIVE_DIRECTIONAL_FEATURES]
    payload = {
        'generated_at': now_str(),
        'module_version': self.MODULE_VERSION,
        'selected_features': selected,
        'shared_selected_features': self._filter_selected_features_for_runtime(self._load_pickle_feature_list(self.selected_features_path), mode='live'),
        'directional_selected_features_by_scope': directional_by_scope,
        'selected_feature_count': int(len(selected)),
        'dataset_columns': dataset_cols,
        'dataset_column_count': int(len(dataset_cols)),
        'strict_feature_parity': bool(getattr(CONFIG, 'strict_feature_parity', True)),
        'selected_features_required_for_live': bool(getattr(CONFIG, 'selected_features_required_for_live', True)),
        'directional_features_used_in_training': bool(selected_directional) and bool(getattr(CONFIG, 'enable_directional_features_in_training', False)),
        'directional_features_required_in_live': bool(selected_directional) and bool(getattr(CONFIG, 'enable_directional_features_in_live', True)),
        'directional_selected_features': selected_directional,
        'approved_live_directional_features': sorted(set(selected_directional).intersection(APPROVED_LIVE_DIRECTIONAL_FEATURES)),
        'live_feature_policy': str(getattr(CONFIG, 'live_feature_policy', 'shared_plus_vetted_directional')),
        'required_runtime_artifacts': [
            'event_calendar_service.json',
            'feature_event_calendar.csv',
            'cross_sectional_percentile_service.json',
            'feature_cross_section_snapshot.csv',
            str(self.selected_features_path.name),
        ],
        'status': 'training_feature_manifest_ready',
    }
    self.feature_manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return self.feature_manifest_path, payload

def load_feature_manifest(self) -> dict[str, Any]:
    return self._read_json_if_exists(self.feature_manifest_path)

def validate_live_feature_parity(
    self,
    selected_features: Sequence[str] | None = None,
) -> dict[str, Any]:
    selected = self._filter_selected_features_for_runtime(selected_features or self.load_selected_features())
    manifest = self.load_feature_manifest()
    artifact_status = self.build_runtime_artifact_status()
    selected_from_manifest = [str(x) for x in manifest.get('selected_features', []) if str(x).strip()]
    missing_from_live = [x for x in selected_from_manifest if x not in selected]
    extra_in_live = [x for x in selected if x not in selected_from_manifest]
    directional_required = bool(manifest.get('directional_features_required_in_live', False))
    live_directional_enabled = bool(getattr(CONFIG, 'enable_directional_features_in_live', False))
    manifest_present = bool(manifest)
    checks = {
        'manifest_present': manifest_present,
        'manifest_selected_feature_count': int(len(selected_from_manifest)),
        'selected_feature_count_live': int(len(selected)),
        'selected_features_match': (set(selected) == set(selected_from_manifest)) if manifest_present else False,
        'missing_from_live': missing_from_live,
        'extra_in_live': extra_in_live,
        'directional_required_in_live': directional_required,
        'directional_live_enabled': live_directional_enabled,
        'directional_alignment_ok': (not directional_required) or live_directional_enabled,
        'runtime_artifacts_ready': bool(artifact_status.get('runtime_artifacts_ready', False)),
        'artifact_status': artifact_status,
    }
    checks['status'] = 'ready' if bool(
        checks['manifest_present']
        and checks['selected_features_match']
        and checks['directional_alignment_ok']
        and (checks['runtime_artifacts_ready'] or not bool(getattr(CONFIG, 'feature_parity_require_runtime_artifacts', True)))
    ) else 'blocked'
    checks['ok'] = checks['status'] == 'ready'
    return checks

def _combo_feature(self, name: str, features: Mapping[str, Any]) -> float:
    parts = [p for p in str(name).split('_X_') if p]
    value = 1.0
    for part in parts:
        value *= safe_float(features.get(part, 0.0), 0.0)
    return float(value)

def _write_parity_status(self, payload: dict[str, Any]) -> None:
    self.parity_status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

def select_live_features(
    self,
    features: Mapping[str, Any],
    selected_features: Sequence[str] | None = None,
    strict: bool | None = None,
) -> dict[str, float]:
    picked = self._filter_selected_features_for_runtime(selected_features or self.load_selected_features(mode='live', include_directional=True), mode='live')
    strict_mode = bool(getattr(CONFIG, 'strict_feature_parity', True)) if strict is None else bool(strict)

    if not picked or len(picked) < self.min_live_feature_count:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'strict_feature_parity': strict_mode,
            'selected_features_present': False if not picked else True,
            'fallback_all_features_blocked': bool(strict_mode),
            'selected_feature_count': int(len(picked)),
            'selected_feature_min_required': int(self.min_live_feature_count),
            'status': 'selected_features_missing',
        }
        self._write_parity_status(payload)
        if strict_mode:
            return {}
        return {k: safe_float(v, 0.0) for k, v in features.items()}

    out: dict[str, float] = {}
    missing: list[str] = []
    allow_directional_live = self._allow_directional_runtime('live')
    require_approved_live = bool(getattr(CONFIG, 'directional_live_require_approved_features', True))
    for key in picked:
        if self._is_directional_feature_name(key) and not allow_directional_live:
            missing.append(key)
            continue
        if self._is_directional_feature_name(key) and require_approved_live and not self._is_live_safe_feature_name(key):
            missing.append(key)
            continue
        if (not self._is_directional_feature_name(key)) and (not self._is_live_safe_feature_name(key)):
            missing.append(key)
            continue
        if '_X_' in key:
            out[key] = self._combo_feature(key, features)
            continue
        if key not in features:
            missing.append(key)
        out[key] = safe_float(features.get(key, 0.0), 0.0)

    payload = {
        'generated_at': now_str(),
        'module_version': self.MODULE_VERSION,
        'strict_feature_parity': strict_mode,
        'selected_features_present': True,
        'selected_feature_count': int(len(picked)),
        'mounted_feature_count': int(len(out)),
        'missing_feature_count': int(len(missing)),
        'missing_features_sample': missing[:20],
        'status': 'feature_parity_locked',
    }
    self._write_parity_status(payload)
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
    open_ = pd.to_numeric(out['Open'], errors='coerce').fillna(close) if 'Open' in out.columns else close.copy()
    volume = pd.to_numeric(out['Volume'], errors='coerce').fillna(0.0) if 'Volume' in out.columns else pd.Series([0.0] * len(out), index=out.index, dtype=float)

    atr14 = self._compute_atr(out, 14)
    out['ATR14'] = atr14
    out['ATR_Pct'] = (atr14 / close.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out['ATR_Pctl_252'] = self._rolling_percentile(out['ATR_Pct'], 252)
    ret = close.pct_change().fillna(0.0)
    out['RealizedVol_20'] = ret.rolling(20, min_periods=5).std().fillna(0.0) * np.sqrt(20)
    out['RealizedVol_60'] = ret.rolling(60, min_periods=10).std().fillna(0.0) * np.sqrt(60)
    out['Gap_Pct'] = ((open_ - close.shift(1)) / close.shift(1).replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out['Overnight_Return'] = out['Gap_Pct']
    out['Intraday_Return'] = ((close - open_) / open_.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out['Turnover_Proxy'] = (close * volume).fillna(0.0)
    out['ADV20_Proxy'] = volume.rolling(20, min_periods=5).mean().fillna(0.0)
    out['DollarVol20_Proxy'] = (close * volume).rolling(20, min_periods=5).mean().fillna(0.0)
    out['Volume_Z20'] = ((volume - volume.rolling(20, min_periods=5).mean()) / volume.rolling(20, min_periods=5).std().replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out['Return_Z20'] = ((ret - ret.rolling(20, min_periods=5).mean()) / ret.rolling(20, min_periods=5).std().replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out['Turnover_Pctl'] = self._rolling_percentile(out['Turnover_Proxy'], 252)
    out['ADV20_Pctl'] = self._rolling_percentile(out['ADV20_Proxy'], 252)
    out['ATR_Pct_Pctl'] = self._rolling_percentile(out['ATR_Pct'], 252)
    out['RealizedVol_20_Pctl'] = self._rolling_percentile(out['RealizedVol_20'], 252)
    out = self._compute_structural_bias_frame(out)

    # 正式 class 方法：原本尾端 monkey patch 的提前布局 / 出場 / regime transition 特徵，
    # 現在直接落在 FeatureService.enrich_from_history() 內，不再靠執行期覆寫方法。
    for col, default in [
        ('Buy_Score', 0.0), ('Sell_Score', 0.0), ('Score_Gap', 0.0),
        ('ADX14', 0.0), ('MACD_Hist', 0.0), ('RSI', 50.0),
        ('BB_Width', 0.0), ('ATR_Pct', 0.0), ('Volume_Z20', 0.0),
        ('Foreign_Ratio', 0.0), ('Total_Ratio', 0.0), ('AI_Proba', 0.5),
    ]:
        if col not in out.columns:
            out[col] = default
    out['Buy_Score_Slope_3d'] = self._history_delta_series(out, 'Buy_Score', 3)
    out['Buy_Score_Slope_5d'] = self._history_delta_series(out, 'Buy_Score', 5)
    out['Sell_Score_Slope_3d'] = self._history_delta_series(out, 'Sell_Score', 3)
    out['Sell_Score_Slope_5d'] = self._history_delta_series(out, 'Sell_Score', 5)
    out['Score_Gap_Slope_3d'] = self._history_delta_series(out, 'Score_Gap', 3)
    out['Score_Gap_Slope_5d'] = self._history_delta_series(out, 'Score_Gap', 5)
    out['ADX_Delta_3d'] = self._history_delta_series(out, 'ADX14', 3)
    out['MACD_Hist_Delta_3d'] = self._history_delta_series(out, 'MACD_Hist', 3)
    out['Volume_Z20_Delta'] = self._history_delta_series(out, 'Volume_Z20', 3)
    out['Foreign_Ratio_Delta_3d'] = self._history_delta_series(out, 'Foreign_Ratio', 3)
    out['Total_Ratio_Delta_3d'] = self._history_delta_series(out, 'Total_Ratio', 3)

    rsi = pd.to_numeric(out['RSI'], errors='coerce').fillna(50.0)
    bb_width = pd.to_numeric(out['BB_Width'], errors='coerce').fillna(0.0)
    atr_pct = pd.to_numeric(out['ATR_Pct'], errors='coerce').fillna(0.0)
    out['RSI_Reclaim_Speed'] = ((rsi - rsi.shift(3).fillna(rsi)) / 20.0).clip(-1.0, 1.0).fillna(0.0)
    out['BB_Squeeze_Release'] = self._clip01_series((bb_width.rolling(3, min_periods=1).mean() - bb_width.rolling(10, min_periods=3).mean()).fillna(0.0) * 8.0)
    out['ATR_Expansion_Start'] = self._clip01_series((atr_pct - atr_pct.rolling(10, min_periods=3).mean()).fillna(0.0) * 20.0)

    if 'Transition_Label' not in out.columns or 'Hysteresis_Regime_Label' not in out.columns:
        try:
            from fts_regime_service import RegimeService
            out = RegimeService().enrich_dataframe(out)
        except Exception as exc:
            record_issue('feature_service', 'regime_enrich_dataframe_failed', exc, severity='ERROR', fail_mode='fail_closed')
            out['FeatureService_Regime_Enrich_Failed'] = True
    return out

def _history_delta_series(df: pd.DataFrame, col: str, periods: int) -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index if df is not None else None, dtype=float)
    s = pd.to_numeric(df[col], errors='coerce').ffill().fillna(0.0)
    return (s - s.shift(periods)).fillna(0.0)

def _clip01_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors='coerce').fillna(0.0).clip(0.0, 1.0)

def _history_delta(history_df: pd.DataFrame | None, col: str, periods: int = 3) -> float:
    if history_df is None or history_df.empty or col not in history_df.columns or len(history_df) <= periods:
        return 0.0
    s = pd.to_numeric(history_df[col], errors='coerce').ffill().fillna(0.0)
    try:
        return float(s.iloc[-1] - s.iloc[-(periods + 1)])
    except Exception as exc:
        record_issue('feature_service', 'history_delta_failed', exc, severity='WARNING', fail_mode='fail_open', context={'column': col, 'periods': periods})
        return 0.0

def _compute_exit_timing_features(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, float]:
    trend_conf = safe_float(row.get('Trend_Confidence', 0.5), 0.5)
    range_conf = safe_float(row.get('Range_Confidence', 0.5), 0.5)
    ai_proba = safe_float(row.get('AI_Proba', row.get('訊號信心分數(%)', 50.0) / 100.0), 0.5)
    score_gap = safe_float(row.get('Score_Gap', 0.0), 0.0)
    upper_shadow = safe_float(row.get('Upper_Shadow_Pct', row.get('Upper_Shadow', 0.0)), 0.0)
    intraday = safe_float(row.get('Intraday_Return', 0.0), 0.0)
    gap_pct = safe_float(row.get('Gap_Pct', 0.0), 0.0)
    total_ratio = safe_float(row.get('Total_Ratio', 0.0), 0.0)
    foreign_ratio = safe_float(row.get('Foreign_Ratio', 0.0), 0.0)
    rsi = safe_float(row.get('RSI', 50.0), 50.0)
    macd_hist = safe_float(row.get('MACD_Hist', 0.0), 0.0)

    trend_conf_delta = self._history_delta(history_df, 'Trend_Confidence', 3)
    range_conf_delta = self._history_delta(history_df, 'Range_Confidence', 3)
    proba_delta = self._history_delta(history_df, 'AI_Proba', 3)
    foreign_delta = self._history_delta(history_df, 'Foreign_Ratio', 3)
    total_delta = self._history_delta(history_df, 'Total_Ratio', 3)
    macd_delta = self._history_delta(history_df, 'MACD_Hist', 3)

    breakout_risk = (
        max(0.0, upper_shadow) * 0.20
        + max(0.0, -intraday) * 0.20
        + max(0.0, -gap_pct) * 0.10
        + max(0.0, -macd_delta) * 0.20
        + max(0.0, -proba_delta) * 0.15
        + max(0.0, -trend_conf_delta) * 0.15
    )
    reversal_risk = (
        max(0.0, (rsi - 68.0) / 20.0) * 0.15
        + max(0.0, -foreign_delta) * 0.20
        + max(0.0, -total_delta) * 0.20
        + max(0.0, range_conf - trend_conf) * 0.20
        + max(0.0, -score_gap) * 0.10
        + max(0.0, -macd_hist) * 0.15
    )
    exit_trend_decay = max(0.0, -trend_conf_delta) * 0.45 + max(0.0, -proba_delta) * 0.35 + max(0.0, -macd_delta) * 0.20
    exit_chip_weakening = max(0.0, -foreign_delta) * 0.55 + max(0.0, -total_delta) * 0.45
    exit_regime_det = max(0.0, range_conf - trend_conf) * 0.70 + max(0.0, -trend_conf_delta) * 0.30
    hazard_raw = (
        breakout_risk * 0.30
        + reversal_risk * 0.25
        + exit_trend_decay * 0.20
        + exit_chip_weakening * 0.10
        + exit_regime_det * 0.10
        + max(0.0, 0.55 - ai_proba) * 0.25
    )
    hazard = max(0.0, min(1.0, hazard_raw * 1.85))
    stop_tighten = max(0.35, 1.0 - hazard * 0.60)
    return {
        'Breakout_Risk_Next3': round(float(breakout_risk), 6),
        'Reversal_Risk_Next3': round(float(reversal_risk), 6),
        'Exit_Trend_Decay': round(float(exit_trend_decay), 6),
        'Exit_Chip_Weakening': round(float(exit_chip_weakening), 6),
        'Exit_Regime_Deterioration': round(float(exit_regime_det), 6),
        'Exit_Hazard_Score': round(float(hazard), 6),
        'Exit_Stop_Tighten_Suggested': round(float(stop_tighten), 6),
        'Trend_Confidence_Delta': round(float(trend_conf_delta), 6),
        'Range_Confidence_Delta': round(float(range_conf_delta), 6),
        'Proba_Delta_3d': round(float(proba_delta), 6),
    }

def extract_ai_features(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None, ticker: str | None = None, as_of_date: Any | None = None) -> dict[str, Any]:
    features = {
        'Body_Pct': safe_float(row.get('Body_Pct', row.get('K_Body_Ratio', 0.0)), 0.0),
        'Upper_Shadow_Pct': safe_float(row.get('Upper_Shadow_Pct', row.get('Upper_Shadow', 0.0)), 0.0),
        'Lower_Shadow_Pct': safe_float(row.get('Lower_Shadow_Pct', row.get('Lower_Shadow', 0.0)), 0.0),
        'Dist_MA20_Pct': safe_float(row.get('Dist_MA20_Pct', row.get('Distance_to_MA20', 0.0)), 0.0),
        'Volume_Ratio': safe_float(row.get('Volume_Ratio', row.get('Vol_Ratio', 0.0)), 0.0),
        'BB_Width': safe_float(row.get('BB_Width', 0.0), 0.0),
        'RSI': safe_float(row.get('RSI', 0.0), 0.0),
        'MACD_Hist': safe_float(row.get('MACD_Hist', 0.0), 0.0),
        'ADX': safe_float(row.get('ADX14', row.get('ADX', 0.0)), 0.0),
        'Foreign_Ratio': safe_float(row.get('Foreign_Ratio', 0.0), 0.0),
        'Trust_Ratio': safe_float(row.get('Trust_Ratio', 0.0), 0.0),
        'Total_Ratio': safe_float(row.get('Total_Ratio', 0.0), 0.0),
        'Foreign_Continuous': safe_float(row.get('Foreign_Continuous', 0.0), 0.0),
        'Trust_Continuous': safe_float(row.get('Trust_Continuous', 0.0), 0.0),
        'Weighted_Buy_Score': safe_float(row.get('Weighted_Buy_Score', row.get('Buy_Score', 0.0)), 0.0),
        'Weighted_Sell_Score': safe_float(row.get('Weighted_Sell_Score', row.get('Sell_Score', 0.0)), 0.0),
        'Score_Gap': safe_float(row.get('Score_Gap', 0.0), 0.0),
        'Signal_Conflict': safe_float(row.get('Signal_Conflict', 0.0), 0.0),
        'Vol_Squeeze': safe_float(row.get('Vol_Squeeze', 0.0), 0.0),
        'Absorption': safe_float(row.get('Absorption', 0.0), 0.0),
        'MR_Long_Spring': safe_float(row.get('MR_Long_Spring', 0.0), 0.0),
        'MR_Short_Trap': safe_float(row.get('MR_Short_Trap', 0.0), 0.0),
        'MR_Long_Accumulation': safe_float(row.get('MR_Long_Accumulation', 0.0), 0.0),
        'MR_Short_Distribution': safe_float(row.get('MR_Short_Distribution', 0.0), 0.0),
        'buy_c2': safe_float(row.get('buy_c2', 0.0), 0.0),
        'buy_c3': safe_float(row.get('buy_c3', 0.0), 0.0),
        'buy_c4': safe_float(row.get('buy_c4', 0.0), 0.0),
        'buy_c5': safe_float(row.get('buy_c5', 0.0), 0.0),
        'buy_c6': safe_float(row.get('buy_c6', 0.0), 0.0),
        'buy_c7': safe_float(row.get('buy_c7', 0.0), 0.0),
        'buy_c8': safe_float(row.get('buy_c8', 0.0), 0.0),
        'buy_c9': safe_float(row.get('buy_c9', 0.0), 0.0),
        'sell_c2': safe_float(row.get('sell_c2', 0.0), 0.0),
        'sell_c3': safe_float(row.get('sell_c3', 0.0), 0.0),
        'sell_c4': safe_float(row.get('sell_c4', 0.0), 0.0),
        'sell_c5': safe_float(row.get('sell_c5', 0.0), 0.0),
        'sell_c6': safe_float(row.get('sell_c6', 0.0), 0.0),
        'sell_c7': safe_float(row.get('sell_c7', 0.0), 0.0),
        'sell_c8': safe_float(row.get('sell_c8', 0.0), 0.0),
        'sell_c9': safe_float(row.get('sell_c9', 0.0), 0.0),
    }
    if history_df is not None and not history_df.empty:
        enriched = self.enrich_from_history(history_df)
        latest = enriched.iloc[-1].to_dict()
        for k in ['ATR14','ATR_Pct','ATR_Pctl_252','RealizedVol_20','RealizedVol_60','Gap_Pct','Overnight_Return','Intraday_Return','Turnover_Proxy','ADV20_Proxy','DollarVol20_Proxy','Volume_Z20','Return_Z20','MA20','MA60','MA120','Dist_MA60_Pct','Dist_MA120_Pct','MA20_vs_MA60_Pct','MA60_vs_MA120_Pct','MA20_Slope_5','MA60_Slope_10','Structural_Long_Bias','Structural_Short_Bias','Structural_Range_Bias']:
            features[k] = safe_float(latest.get(k, features.get(k, 0.0)), features.get(k, 0.0))
    passthrough = ['RS_vs_Market_20','RS_vs_Sector_20','RS_vs_Market_20_Pctl','RS_vs_Sector_20_Pctl','Revenue_YoY','Revenue_YoY_Pctl','Chip_Total_Ratio','Chip_Total_Ratio_Pctl','Turnover_Pctl','ADV20_Pctl','ATR_Pct_Pctl','RealizedVol_20_Pctl','Event_Days_Since_Revenue','Event_Days_To_Revenue','Revenue_Window_1','Revenue_Window_3','Revenue_Window_5','Revenue_Window_10','Event_Days_Since_Earnings','Event_Days_To_Earnings','Earnings_Window_3','Earnings_Window_7','Earnings_Window_14','Earnings_Window_Flag','Dividend_Window_7']
    for k in passthrough:
        if k in row:
            features[k] = safe_float(row.get(k, 0), 0)
    features['Revenue_YoY_Rank'] = safe_float(row.get('Revenue_YoY_Rank', row.get('Revenue_YoY_Pctl', 0.5)), 0.5)
    features['Chip_Total_Ratio_Rank'] = safe_float(row.get('Chip_Total_Ratio_Rank', row.get('Chip_Total_Ratio_Pctl', 0.5)), 0.5)
    features['Regime_TrendStrength_X_ScoreGap'] = safe_float(row.get('Regime_TrendStrength_X_ScoreGap', (features['ADX'] / 100.0) * features['Score_Gap']), 0)
    features['Volatility_X_SignalConflict'] = safe_float(row.get('Volatility_X_SignalConflict', features.get('ATR_Pctl_252', 0.0) * features['Signal_Conflict']), 0)
    directional = self._compute_directional_features(row, history_df=history_df)
    if bool(getattr(CONFIG, 'enable_directional_features_in_training', False)):
        features.update(directional)
    features.update(self._compute_exit_timing_features({**row, **features}, history_df=history_df))

    hist_enriched = pd.DataFrame()
    if isinstance(history_df, pd.DataFrame) and not history_df.empty:
        try:
            hist_enriched = self.enrich_from_history(history_df)
            if 'AI_Proba' not in hist_enriched.columns:
                hist_enriched['AI_Proba'] = pd.Series([0.5] * len(hist_enriched), index=hist_enriched.index, dtype=float)
        except Exception as exc:
            record_issue('feature_service', 'extract_ai_features_history_enrich_failed', exc, severity='ERROR', fail_mode='fail_closed')
            hist_enriched = pd.DataFrame()

    def _latest(name: str, default: float = 0.0) -> float:
        if name in row:
            return safe_float(row.get(name, default), default)
        if not hist_enriched.empty and name in hist_enriched.columns:
            return safe_float(hist_enriched[name].iloc[-1], default)
        return default

    features.update({
        'Buy_Score_Slope_3d': _latest('Buy_Score_Slope_3d', 0.0),
        'Buy_Score_Slope_5d': _latest('Buy_Score_Slope_5d', 0.0),
        'Sell_Score_Slope_3d': _latest('Sell_Score_Slope_3d', 0.0),
        'Sell_Score_Slope_5d': _latest('Sell_Score_Slope_5d', 0.0),
        'Score_Gap_Slope_3d': _latest('Score_Gap_Slope_3d', 0.0),
        'Score_Gap_Slope_5d': _latest('Score_Gap_Slope_5d', 0.0),
        'ADX_Delta_3d': _latest('ADX_Delta_3d', 0.0),
        'MACD_Hist_Delta_3d': _latest('MACD_Hist_Delta_3d', 0.0),
        'RSI_Reclaim_Speed': _latest('RSI_Reclaim_Speed', 0.0),
        'BB_Squeeze_Release': _latest('BB_Squeeze_Release', 0.0),
        'ATR_Expansion_Start': _latest('ATR_Expansion_Start', 0.0),
        'Volume_Z20_Delta': _latest('Volume_Z20_Delta', 0.0),
        'Foreign_Ratio_Delta_3d': _latest('Foreign_Ratio_Delta_3d', 0.0),
        'Total_Ratio_Delta_3d': _latest('Total_Ratio_Delta_3d', 0.0),
        'Bull_Emerging_Score': _latest('Bull_Emerging_Score', 0.0),
        'Bear_Emerging_Score': _latest('Bear_Emerging_Score', 0.0),
        'Range_Compression_Score': _latest('Range_Compression_Score', 0.0),
        'Breakout_Readiness': _latest('Breakout_Readiness', 0.0),
        'Trend_Exhaustion_Score': _latest('Trend_Exhaustion_Score', 0.0),
        'Entry_Readiness': _latest('Entry_Readiness', 0.0),
        'Transition_Label': str(row.get('Transition_Label', hist_enriched['Transition_Label'].iloc[-1] if not hist_enriched.empty and 'Transition_Label' in hist_enriched.columns else 'Stable')),
        'Next_Regime_Prob_Bull': _latest('Next_Regime_Prob_Bull', 0.34),
        'Next_Regime_Prob_Bear': _latest('Next_Regime_Prob_Bear', 0.33),
        'Next_Regime_Prob_Range': _latest('Next_Regime_Prob_Range', 0.33),
        'Hysteresis_Regime_Label': str(row.get('Hysteresis_Regime_Label', hist_enriched['Hysteresis_Regime_Label'].iloc[-1] if not hist_enriched.empty and 'Hysteresis_Regime_Label' in hist_enriched.columns else row.get('Regime', '區間盤整'))),
        'Proba_Delta_3d': _latest('Proba_Delta_3d', 0.0),
        'Trend_Confidence_Delta': _latest('Trend_Confidence_Delta', 0.0),
        'Range_Confidence_Delta': _latest('Range_Confidence_Delta', 0.0),
        'MA20': _latest('MA20', safe_float(row.get('MA20', row.get('MA_20', 0.0)), 0.0)),
        'MA60': _latest('MA60', safe_float(row.get('MA60', 0.0), 0.0)),
        'MA120': _latest('MA120', safe_float(row.get('MA120', 0.0), 0.0)),
        'Dist_MA60_Pct': _latest('Dist_MA60_Pct', 0.0),
        'Dist_MA120_Pct': _latest('Dist_MA120_Pct', 0.0),
        'MA20_vs_MA60_Pct': _latest('MA20_vs_MA60_Pct', 0.0),
        'MA60_vs_MA120_Pct': _latest('MA60_vs_MA120_Pct', 0.0),
        'MA20_Slope_5': _latest('MA20_Slope_5', 0.0),
        'MA60_Slope_10': _latest('MA60_Slope_10', 0.0),
        'Structural_Long_Bias': _latest('Structural_Long_Bias', 0.0),
        'Structural_Short_Bias': _latest('Structural_Short_Bias', 0.0),
        'Structural_Range_Bias': _latest('Structural_Range_Bias', 0.0),
        'Structural_Regime_Bias': str(row.get('Structural_Regime_Bias', hist_enriched['Structural_Regime_Bias'].iloc[-1] if not hist_enriched.empty and 'Structural_Regime_Bias' in hist_enriched.columns else 'RANGE')),
    })
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
    selected = self._filter_selected_features_for_runtime(self.load_selected_features(mode='live', include_directional=True), mode='live')
    mounted = self.select_live_features(features, selected_features=selected, strict=getattr(CONFIG, 'strict_feature_parity', True))
    mount_checks = self.validate_live_feature_parity(selected)
    mount_payload = {
        'generated_at': now_str(),
        'module_version': self.MODULE_VERSION,
        'ticker': str(ticker),
        'selected_feature_count': len(selected),
        'mounted_feature_count': len(mounted),
        'selected_features_present': bool(selected),
        'strict_feature_parity': bool(getattr(CONFIG, 'strict_feature_parity', True)),
        'official_percentile_mode': True,
        'precise_event_calendar_mode': True,
        'feature_manifest_path': str(self.feature_manifest_path),
        'manifest_checks': mount_checks,
        'status': 'live_feature_mount_ready' if mount_checks.get('ok', False) and mounted else ('live_feature_mount_blocked' if not mount_checks.get('ok', False) else 'live_feature_mount_waiting_for_selected_features'),
    }
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
    selected = self._filter_selected_features_for_runtime(self.load_selected_features(mode='live', include_directional=True), mode='live')
    payload = {
        'generated_at': now_str(),
        'module_version': self.MODULE_VERSION,
        'sample_feature_count': len(sample),
        'selected_features_present': bool(selected),
        'selected_feature_count': len(selected),
        'feature_buckets': {k: len(v) for k, v in FEATURE_BUCKETS.items()},
        'directional_training_enabled': bool(getattr(CONFIG, 'enable_directional_features_in_training', False)),
        'directional_live_enabled': bool(getattr(CONFIG, 'enable_directional_features_in_live', False)),
        'live_feature_groups': get_live_feature_groups(),
        'approved_live_directional_features': APPROVED_LIVE_DIRECTIONAL_FEATURES,
        'training_feature_groups': get_training_feature_groups(),
        'priority_new_features_20': PRIORITY_NEW_FEATURES_20,
        'official_percentile_mode': True,
        'precise_event_calendar_mode': True,
        'live_mount_path': str(self.live_mount_path),
        'training_registry_csv': str(self.training_registry_csv),
        'rolling_percentile_engine': 'sorted_window_fast',
        'strict_feature_parity': bool(getattr(CONFIG, 'strict_feature_parity', True)),
        'feature_manifest_path': str(self.feature_manifest_path),
        'status': 'feature_service_ready',
        'runtime_diagnostics': get_runtime_diagnostics_safe() if 'get_runtime_diagnostics_safe' in globals() else {},
    }
    self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    log(f'🧩 feature service ready: {self.runtime_path}')
    return self.runtime_path, payload

FeatureService._compute_directional_features = _compute_directional_features
FeatureService.load_selected_features = load_selected_features
FeatureService.load_training_feature_registry = load_training_feature_registry
FeatureService.write_training_feature_registry = write_training_feature_registry
FeatureService._read_json_if_exists = staticmethod(_read_json_if_exists)
FeatureService._artifact_status = _artifact_status
FeatureService.build_runtime_artifact_status = build_runtime_artifact_status
FeatureService.write_feature_manifest = write_feature_manifest
FeatureService.load_feature_manifest = load_feature_manifest
FeatureService.validate_live_feature_parity = validate_live_feature_parity
FeatureService._combo_feature = _combo_feature
FeatureService._write_parity_status = _write_parity_status
FeatureService.select_live_features = select_live_features
FeatureService.feature_buckets = feature_buckets
FeatureService.current_feature_summary = current_feature_summary
FeatureService.enrich_from_history = enrich_from_history
FeatureService._history_delta_series = staticmethod(_history_delta_series)
FeatureService._clip01_series = staticmethod(_clip01_series)
FeatureService._history_delta = _history_delta
FeatureService._compute_exit_timing_features = _compute_exit_timing_features
FeatureService.extract_ai_features = extract_ai_features
FeatureService.mount_live_features = mount_live_features
FeatureService.build_summary = build_summary
