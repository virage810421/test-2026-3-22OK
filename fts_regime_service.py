# -*- coding: utf-8 -*-
from __future__ import annotations

import json
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
    MODULE_VERSION = 'v88_regime_transition_engine'

    def __init__(self) -> None:
        self.runtime_path = Path(PATHS.runtime_dir) / 'regime_service.json'
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _calc_base(row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, float]:
        adx = safe_float(row.get('ADX14', row.get('ADX', 0.0)), 0.0)
        bb_width = safe_float(row.get('BB_Width', 0.0), 0.0)
        close = safe_float(row.get('Close', 0.0), 0.0)
        ma20 = safe_float(row.get('MA20', row.get('MA_20', close)), close)
        ma60 = safe_float(row.get('MA60', row.get('MA_60', ma20)), ma20)
        macd_hist = safe_float(row.get('MACD_Hist', 0.0), 0.0)
        score_gap = safe_float(row.get('Score_Gap', 0.0), 0.0)
        atr_pct = safe_float(row.get('ATR_Pct', 0.0), 0.0)

        width_pct = bb_width
        width_pctl = safe_float(row.get('Range_Width_Pctl', 0.5), 0.5)
        slope = 0.0 if not ma20 else (ma20 - ma60) / max(abs(ma60), 1e-9)
        if history_df is not None and not history_df.empty and 'Close' in history_df.columns:
            c = pd.to_numeric(history_df['Close'], errors='coerce').ffill().fillna(close)
            roll_high = c.rolling(20, min_periods=5).max().iloc[-1]
            roll_low = c.rolling(20, min_periods=5).min().iloc[-1]
            width_pct = float((roll_high - roll_low) / max(abs(close), 1e-9)) if close else 0.0
            last10 = c.tail(10)
            slope = float((last10.iloc[-1] - last10.iloc[0]) / max(abs(last10.iloc[0]), 1e-9)) if len(last10) >= 2 else slope
            width_series = (c.rolling(20, min_periods=5).max() - c.rolling(20, min_periods=5).min()) / c.replace(0, pd.NA)
            if len(width_series.dropna()):
                width_pctl = float(width_series.rank(pct=True).iloc[-1])

        ma_flatness = max(0.0, 1.0 - min(abs(slope) / 0.08, 1.0))
        bb_width_pctl = max(0.0, min(1.0, 1.0 - min(bb_width / 0.25, 1.0)))
        adx_low_flag = 1.0 if adx <= 18 else 0.0
        range_conf = (0.40 * ma_flatness) + (0.30 * bb_width_pctl) + (0.30 * adx_low_flag)
        trend_conf = (0.45 * min(adx / 35.0, 1.0)) + (0.35 * min(abs(slope) / 0.08, 1.0)) + (0.20 * min(width_pct / 0.15, 1.0))
        direction_bias = (0.55 * (1.0 if close >= ma60 else -1.0)) + (0.25 * (1.0 if slope >= 0 else -1.0)) + (0.20 * (1.0 if macd_hist >= 0 else -1.0))
        return {
            'adx': adx,
            'bb_width': bb_width,
            'close': close,
            'ma20': ma20,
            'ma60': ma60,
            'slope': slope,
            'macd_hist': macd_hist,
            'score_gap': score_gap,
            'atr_pct': atr_pct,
            'width_pct': width_pct,
            'width_pctl': width_pctl,
            'ma_flatness': ma_flatness,
            'bb_width_pctl': bb_width_pctl,
            'adx_low_flag': adx_low_flag,
            'range_conf': max(0.0, min(range_conf, 1.0)),
            'trend_conf': max(0.0, min(trend_conf, 1.0)),
            'direction_bias': max(-1.0, min(direction_bias, 1.0)),
        }

    def build_regime_row(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, Any]:
        current = self._calc_base(row, history_df=history_df)
        prev = current
        if history_df is not None and len(history_df) >= 2:
            prev_hist = history_df.iloc[:-1].copy()
            prev_row = prev_hist.iloc[-1].to_dict() if not prev_hist.empty else dict(row)
            prev = self._calc_base(prev_row, history_df=prev_hist)

        trend_delta = current['trend_conf'] - prev['trend_conf']
        range_delta = current['range_conf'] - prev['range_conf']
        adx_delta = current['adx'] - prev['adx']
        score_gap_delta = current['score_gap'] - prev['score_gap']
        macd_delta = current['macd_hist'] - prev['macd_hist']

        bull_emerging = (0.30 * max(current['direction_bias'], 0.0) + 0.25 * max(trend_delta, 0.0) + 0.25 * max(score_gap_delta, 0.0) + 0.20 * max(macd_delta, 0.0))
        bear_emerging = (0.30 * max(-current['direction_bias'], 0.0) + 0.25 * max(trend_delta, 0.0) + 0.25 * max(-score_gap_delta, 0.0) + 0.20 * max(-macd_delta, 0.0))
        range_compression = (0.45 * current['range_conf'] + 0.30 * current['bb_width_pctl'] + 0.25 * current['adx_low_flag'])
        breakout_readiness = range_compression * max(0.0, min(1.0, 0.45 * max(adx_delta / 6.0, 0.0) + 0.30 * max(abs(score_gap_delta) / 1.5, 0.0) + 0.25 * max(abs(macd_delta) / 0.5, 0.0)))
        trend_exhaustion = max(0.0, min(1.0, 0.45 * current['trend_conf'] + 0.25 * max(-trend_delta, 0.0) + 0.20 * max(range_delta, 0.0) + 0.10 * max(-adx_delta / 6.0, 0.0)))

        bull_raw = max(current['direction_bias'], 0.0) * (0.45 + 0.55 * current['trend_conf']) + bull_emerging * 0.35
        bear_raw = max(-current['direction_bias'], 0.0) * (0.45 + 0.55 * current['trend_conf']) + bear_emerging * 0.35
        range_raw = current['range_conf'] + range_compression * 0.35
        total = max(bull_raw + bear_raw + range_raw, 1e-9)
        bull_prob = bull_raw / total
        bear_prob = bear_raw / total
        range_prob = range_raw / total
        probs = {'趨勢多頭': bull_prob, '趨勢空頭': bear_prob, '區間盤整': range_prob}
        regime_label = max(probs, key=probs.get)
        regime_conf = probs[regime_label]

        transition_label = 'Stable'
        if regime_label == '區間盤整' and breakout_readiness >= 0.55:
            transition_label = 'Range_Breakout_Risk'
        elif range_compression >= 0.60 and regime_label != '區間盤整':
            transition_label = 'Range_Compressing'
        elif bull_emerging >= 0.55 and bull_prob < 0.60:
            transition_label = 'Bull_Emerging'
        elif bear_emerging >= 0.55 and bear_prob < 0.60:
            transition_label = 'Bear_Emerging'
        elif trend_exhaustion >= 0.60 and regime_label == '趨勢多頭':
            transition_label = 'Bull_Exhausting'
        elif trend_exhaustion >= 0.60 and regime_label == '趨勢空頭':
            transition_label = 'Bear_Exhausting'
        elif range_compression >= 0.60:
            transition_label = 'Range_Compressing'

        proba_now = safe_float(row.get('AI_Proba', 0.5 + current['score_gap'] * 0.1), 0.5)
        proba_prev = safe_float(row.get('AI_Proba', 0.5 + prev['score_gap'] * 0.1), 0.5)
        proba_delta = proba_now - proba_prev
        entry_readiness = max(0.0, min(1.0, 0.35 * breakout_readiness + 0.25 * max(bull_emerging, bear_emerging) + 0.20 * max(proba_delta, 0.0) + 0.20 * max(current['trend_conf'] - current['range_conf'], 0.0)))
        breakout_risk_next3 = max(0.0, min(1.0, 0.65 * breakout_readiness + 0.35 * max(abs(score_gap_delta) / 1.5, 0.0)))
        reversal_risk_next3 = max(0.0, min(1.0, 0.60 * trend_exhaustion + 0.20 * max(-proba_delta, 0.0) + 0.20 * max(range_delta, 0.0)))
        exit_hazard = max(0.0, min(1.0, 0.45 * trend_exhaustion + 0.25 * reversal_risk_next3 + 0.20 * max(-proba_delta, 0.0) + 0.10 * max(range_delta, 0.0)))

        return {
            'Range_Confidence': round(current['range_conf'], 6),
            'Trend_Confidence': round(current['trend_conf'], 6),
            'Range_Width_Pctl': round(max(0.0, min(current['width_pctl'], 1.0)), 6),
            'MA_Slope_Flatness': round(current['ma_flatness'], 6),
            'BB_Width_Pctl': round(current['bb_width_pctl'], 6),
            'ADX_Low_Regime_Flag': float(current['adx_low_flag']),
            'Bull_Emerging_Score': round(max(0.0, min(bull_emerging, 1.0)), 6),
            'Bear_Emerging_Score': round(max(0.0, min(bear_emerging, 1.0)), 6),
            'Range_Compression_Score': round(max(0.0, min(range_compression, 1.0)), 6),
            'Breakout_Readiness': round(max(0.0, min(breakout_readiness, 1.0)), 6),
            'Trend_Exhaustion_Score': round(max(0.0, min(trend_exhaustion, 1.0)), 6),
            'Entry_Readiness': round(entry_readiness, 6),
            'Breakout_Risk_Next3': round(breakout_risk_next3, 6),
            'Reversal_Risk_Next3': round(reversal_risk_next3, 6),
            'Exit_Hazard_Score': round(exit_hazard, 6),
            'Proba_Delta_3d': round(proba_delta, 6),
            'Trend_Confidence_Delta': round(trend_delta, 6),
            'Range_Confidence_Delta': round(range_delta, 6),
            'Regime_Label': regime_label,
            'Regime_Confidence': round(regime_conf, 6),
            'Next_Regime_Prob_Bull': round(max(0.0, min(bull_prob, 1.0)), 6),
            'Next_Regime_Prob_Bear': round(max(0.0, min(bear_prob, 1.0)), 6),
            'Next_Regime_Prob_Range': round(max(0.0, min(range_prob, 1.0)), 6),
            'Transition_Label': transition_label,
        }

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'enabled': bool(getattr(CONFIG, 'enable_range_confidence_service', True)),
            'status': 'regime_service_ready',
            'outputs': [
                'Range_Confidence', 'Trend_Confidence', 'Bull_Emerging_Score', 'Bear_Emerging_Score',
                'Range_Compression_Score', 'Breakout_Readiness', 'Trend_Exhaustion_Score', 'Entry_Readiness',
                'Breakout_Risk_Next3', 'Reversal_Risk_Next3', 'Exit_Hazard_Score', 'Proba_Delta_3d',
                'Trend_Confidence_Delta', 'Range_Confidence_Delta', 'Regime_Label', 'Regime_Confidence',
                'Next_Regime_Prob_Bull', 'Next_Regime_Prob_Bear', 'Next_Regime_Prob_Range', 'Transition_Label'
            ],
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧭 regime service ready: {self.runtime_path}')
        return self.runtime_path, payload


def main() -> int:
    RegimeService().build_summary()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
