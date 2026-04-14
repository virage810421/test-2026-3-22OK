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
        regime_hysteresis_switch_band = 0.08
        regime_hysteresis_confirm_bars = 2
        regime_hysteresis_min_hold_bars = 2
        regime_hysteresis_tail_bars = 15
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
    MODULE_VERSION = 'v89_regime_transition_hysteresis_engine'

    def __init__(self) -> None:
        self.runtime_path = Path(PATHS.runtime_dir) / 'regime_service.json'
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)
        self.switch_band = max(0.01, safe_float(getattr(CONFIG, 'regime_hysteresis_switch_band', 0.08), 0.08))
        self.confirm_bars = max(1, int(safe_float(getattr(CONFIG, 'regime_hysteresis_confirm_bars', 2), 2)))
        self.min_hold_bars = max(1, int(safe_float(getattr(CONFIG, 'regime_hysteresis_min_hold_bars', 2), 2)))
        self.tail_bars = max(5, int(safe_float(getattr(CONFIG, 'regime_hysteresis_tail_bars', 15), 15)))

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

    def _raw_row_metrics(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, Any]:
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
        proba_prev = safe_float(prev.get('ai_proba', 0.5 + prev['score_gap'] * 0.1), 0.5)
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
            '__probs': probs,
        }

    def _apply_hysteresis(self, raw_metrics: Mapping[str, Any], prev_state: Mapping[str, Any] | None = None) -> dict[str, Any]:
        probs = raw_metrics.get('__probs', {}) if isinstance(raw_metrics.get('__probs', {}), dict) else {}
        raw_label = str(raw_metrics.get('Regime_Label', '區間盤整'))
        regime_conf = safe_float(raw_metrics.get('Regime_Confidence', 0.5), 0.5)
        transition_label = str(raw_metrics.get('Transition_Label', 'Stable'))
        prev_state = dict(prev_state or {})
        prev_label = str(prev_state.get('Hysteresis_Regime_Label', raw_label) or raw_label)
        prev_stable_bars = int(safe_float(prev_state.get('Hysteresis_Stable_Bars', 0), 0))
        prev_pending_label = str(prev_state.get('Hysteresis_Pending_Label', ''))
        prev_pending_bars = int(safe_float(prev_state.get('Hysteresis_Pending_Bars', 0), 0))
        incumbent_prob = safe_float(probs.get(prev_label, regime_conf), regime_conf)
        challenger_prob = safe_float(probs.get(raw_label, regime_conf), regime_conf)
        switch_margin = challenger_prob - incumbent_prob
        strong_transition = transition_label in {'Range_Breakout_Risk', 'Bull_Emerging', 'Bear_Emerging', 'Bull_Exhausting', 'Bear_Exhausting'}
        min_hold_block = prev_stable_bars < self.min_hold_bars
        same_as_prev = raw_label == prev_label

        hysteresis_label = prev_label
        pending_label = ''
        pending_bars = 0
        switch_armed = False
        regime_changed = False

        if same_as_prev:
            hysteresis_label = raw_label
            stable_bars = prev_stable_bars + 1 if prev_label == raw_label else 1
        else:
            armed_by_margin = switch_margin >= self.switch_band
            armed_by_strong_transition = strong_transition and switch_margin >= (self.switch_band * 0.60)
            urgent_reversal = transition_label in {'Bull_Exhausting', 'Bear_Exhausting'} and switch_margin >= (self.switch_band * 1.10)
            switch_armed = bool(armed_by_margin or armed_by_strong_transition or urgent_reversal)
            if switch_armed and not (min_hold_block and not urgent_reversal):
                pending_label = raw_label
                pending_bars = prev_pending_bars + 1 if prev_pending_label == raw_label else 1
                confirm_target = 1 if urgent_reversal else self.confirm_bars
                if pending_bars >= confirm_target:
                    hysteresis_label = raw_label
                    regime_changed = True
                    stable_bars = 1
                    pending_label = ''
                    pending_bars = 0
                else:
                    hysteresis_label = prev_label
                    stable_bars = prev_stable_bars + 1
            else:
                hysteresis_label = prev_label
                stable_bars = prev_stable_bars + 1

        return {
            'Hysteresis_Regime_Label': hysteresis_label,
            'Hysteresis_Stable_Bars': int(max(stable_bars, 1)),
            'Hysteresis_Switch_Armed': float(bool(switch_armed)),
            'Hysteresis_Switch_Margin': round(float(switch_margin), 6),
            'Hysteresis_Pending_Label': pending_label or 'None',
            'Hysteresis_Pending_Bars': int(max(pending_bars, 0)),
            'Hysteresis_Regime_Changed': float(bool(regime_changed)),
            'Hysteresis_Locked': float(bool(min_hold_block and not regime_changed and hysteresis_label == prev_label and not same_as_prev)),
        }

    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame() if df is None else df.copy()
        out = df.copy()
        metrics_rows: list[dict[str, Any]] = []
        prev_state: dict[str, Any] | None = None
        for i in range(len(out)):
            hist_slice = out.iloc[: i + 1].copy()
            row = out.iloc[i].to_dict()
            raw_metrics = self._raw_row_metrics(row, history_df=hist_slice)
            hyst = self._apply_hysteresis(raw_metrics, prev_state=prev_state)
            merged = {k: v for k, v in raw_metrics.items() if k != '__probs'}
            merged.update(hyst)
            metrics_rows.append(merged)
            prev_state = merged
        metrics_df = pd.DataFrame(metrics_rows, index=out.index)
        for col in metrics_df.columns:
            out[col] = metrics_df[col]
        return out

    def build_regime_row(self, row: Mapping[str, Any], history_df: pd.DataFrame | None = None) -> dict[str, Any]:
        if history_df is None or history_df.empty:
            raw_metrics = self._raw_row_metrics(row, history_df=None)
            raw_metrics.update(self._apply_hysteresis(raw_metrics, prev_state=None))
            raw_metrics.pop('__probs', None)
            return raw_metrics
        tail_df = history_df.tail(self.tail_bars).copy()
        enriched = self.enrich_dataframe(tail_df)
        latest = enriched.iloc[-1].to_dict()
        return {k: v for k, v in latest.items() if k not in set(history_df.columns)}

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'enabled': bool(getattr(CONFIG, 'enable_range_confidence_service', True)),
            'status': 'regime_service_ready',
            'hysteresis': {
                'switch_band': self.switch_band,
                'confirm_bars': self.confirm_bars,
                'min_hold_bars': self.min_hold_bars,
                'tail_bars': self.tail_bars,
            },
            'outputs': [
                'Range_Confidence', 'Trend_Confidence', 'Bull_Emerging_Score', 'Bear_Emerging_Score',
                'Range_Compression_Score', 'Breakout_Readiness', 'Trend_Exhaustion_Score', 'Entry_Readiness',
                'Breakout_Risk_Next3', 'Reversal_Risk_Next3', 'Exit_Hazard_Score', 'Proba_Delta_3d',
                'Trend_Confidence_Delta', 'Range_Confidence_Delta', 'Regime_Label', 'Regime_Confidence',
                'Next_Regime_Prob_Bull', 'Next_Regime_Prob_Bear', 'Next_Regime_Prob_Range', 'Transition_Label',
                'Hysteresis_Regime_Label', 'Hysteresis_Stable_Bars', 'Hysteresis_Switch_Armed',
                'Hysteresis_Switch_Margin', 'Hysteresis_Pending_Label', 'Hysteresis_Pending_Bars',
                'Hysteresis_Regime_Changed', 'Hysteresis_Locked',
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
