# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

try:
    from fts_config import PATHS, CONFIG  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
    class _Config:
        enable_range_confidence_service = True
    PATHS = _Paths()
    CONFIG = _Config()

try:
    from fts_utils import now_str, safe_float, log  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')
    def safe_float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return default
    def log(msg: str) -> None:
        print(msg)


class RegimeService:
    MODULE_VERSION = 'v85_regime_confidence_directional_safe'

    def __init__(self) -> None:
        self.runtime_path = Path(PATHS.runtime_dir) / 'regime_service.json'
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)

    def build_regime_row(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, float]:
        adx = safe_float(row.get('ADX14', row.get('ADX', 0.0)), 0.0)
        bb_width = safe_float(row.get('BB_Width', 0.0), 0.0)
        close = safe_float(row.get('Close', 0.0), 0.0)
        ma20 = safe_float(row.get('MA20', row.get('MA_20', close)), close)
        ma60 = safe_float(row.get('MA60', row.get('MA_60', ma20)), ma20)
        if history_df is not None and not history_df.empty and 'Close' in history_df.columns:
            c = pd.to_numeric(history_df['Close'], errors='coerce').ffill().fillna(close)
            roll_high = c.rolling(20, min_periods=5).max().iloc[-1]
            roll_low = c.rolling(20, min_periods=5).min().iloc[-1]
            width_pct = float((roll_high - roll_low) / max(abs(close), 1e-9)) if close else 0.0
            last10 = c.tail(10)
            slope = float((last10.iloc[-1] - last10.iloc[0]) / max(abs(last10.iloc[0]), 1e-9)) if len(last10) >= 2 else 0.0
            width_series = (c.rolling(20, min_periods=5).max() - c.rolling(20, min_periods=5).min()) / c.replace(0, pd.NA)
            width_pctl = float((width_series.rank(pct=True).iloc[-1]) if len(width_series.dropna()) else 0.5)
        else:
            width_pct = bb_width
            slope = 0.0 if not ma20 else (ma20 - ma60) / max(abs(ma60), 1e-9)
            width_pctl = 0.5
        ma_flatness = max(0.0, 1.0 - min(abs(slope) / 0.08, 1.0))
        bb_width_pctl = max(0.0, min(1.0, 1.0 - min(bb_width / 0.25, 1.0)))
        adx_low_flag = 1.0 if adx <= 18 else 0.0
        range_conf = (0.40 * ma_flatness) + (0.30 * bb_width_pctl) + (0.30 * adx_low_flag)
        trend_conf = (0.45 * min(adx / 35.0, 1.0)) + (0.35 * min(abs(slope) / 0.08, 1.0)) + (0.20 * min(width_pct / 0.15, 1.0))
        return {
            'Range_Confidence': round(max(0.0, min(range_conf, 1.0)), 6),
            'Trend_Confidence': round(max(0.0, min(trend_conf, 1.0)), 6),
            'Range_Width_Pctl': round(max(0.0, min(width_pctl, 1.0)), 6),
            'MA_Slope_Flatness': round(max(0.0, min(ma_flatness, 1.0)), 6),
            'BB_Width_Pctl': round(max(0.0, min(bb_width_pctl, 1.0)), 6),
            'ADX_Low_Regime_Flag': float(adx_low_flag),
        }

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'enabled': bool(getattr(CONFIG, 'enable_range_confidence_service', True)),
            'status': 'regime_service_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧭 regime service ready: {self.runtime_path}')
        return self.runtime_path, payload


def main() -> int:
    RegimeService().build_summary()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
