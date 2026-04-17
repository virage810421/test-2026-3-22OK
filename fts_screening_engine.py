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
from fts_regime_service import RegimeService
from fts_screening_legacy_compat import (
    _apply_weighted_scores,
    _assign_golden_type,
    _compute_realized_signal_stats,
)


try:
    from fts_entry_exit_param_policy import coerce_entry_exit_params, entry_thresholds, exit_thresholds, risk_caps
except Exception:  # pragma: no cover
    def coerce_entry_exit_params(params): return dict(params or {})
    def entry_thresholds(params=None): return (0.42, 0.55, 0.63, 0.40, 0.48)
    def exit_thresholds(params=None): return (0.45, 0.60, 0.72, 0.88)
    def risk_caps(params=None): return (0.86, 0.78)

try:
    from config import PARAMS  # type: ignore
except Exception:
    PARAMS = {}

try:
    from fts_approved_param_mount import get_effective_params_for_mode
except Exception:  # pragma: no cover
    def get_effective_params_for_mode(mode: str, base_params=None, stage=None):
        return dict(base_params or {})


class ScreeningEngine:
    MODULE_VERSION = 'v88_live_safe_ev_state_machine'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'screening_engine.json'
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)
        self.params = coerce_entry_exit_params(get_effective_params_for_mode('strategy_signal', dict(PARAMS)))
        self.market = MarketDataService()
        self.features = FeatureService()
        self.chips = ChipEnrichmentService()
        self.regime_service = RegimeService()

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
    def _clip01(series: pd.Series | float) -> pd.Series | float:
        if isinstance(series, pd.Series):
            return pd.to_numeric(series, errors='coerce').fillna(0.0).clip(0.0, 1.0)
        try:
            return float(max(0.0, min(1.0, float(series))))
        except Exception:
            return 0.0


    def _build_live_expected_return(self, out: pd.DataFrame, params: dict[str, Any]) -> pd.Series:
        """Build a live-safe expected-return proxy without future-price leakage.

        Forward_Label_Return is kept only for training/backtests.  Realized_EV is
        now deliberately mapped to this live-safe estimate for backward
        compatibility with old gates that still read Realized_EV.
        """
        if out.empty:
            return pd.Series(dtype=float, index=out.index)

        def _num(name: str, default: float = 0.0) -> pd.Series:
            if name in out.columns:
                return pd.to_numeric(out[name], errors='coerce').fillna(default).astype(float)
            return pd.Series([default] * len(out), index=out.index, dtype=float)

        trigger = max(1.0, float(params.get('TRIGGER_SCORE', 2.0)))
        min_samples = max(1, int(params.get('MIN_SIGNAL_SAMPLE_SIZE', 8)))
        score_gap = _num('Score_Gap', 0.0).clip(-trigger * 2.0, trigger * 2.0)
        ai_proba = _num('AI_Proba', 0.5).clip(0.01, 0.99)
        sample_size = _num('歷史訊號樣本數', 0.0).clip(lower=0.0)
        historical_ev_pct = _num('期望值', 0.0)  # compatibility helper reports percent units
        entry_readiness = _num('Entry_Readiness', 0.0).clip(0.0, 1.0)
        preentry_score = _num('PreEntry_Score', 0.0).clip(0.0, 1.0)
        confirm_score = _num('Confirm_Entry_Score', 0.0).clip(0.0, 1.0)
        exit_hazard = _num('Exit_Hazard_Score', 0.0).clip(0.0, 1.0)
        reversal_risk = _num('Reversal_Risk_Next3', 0.0).clip(0.0, 1.0)
        breakout_risk = _num('Breakout_Risk_Next3', 0.0).clip(0.0, 1.0)

        sample_weight = (sample_size / float(min_samples)).clip(0.0, 1.0)
        historical_component = (historical_ev_pct / 100.0) * sample_weight
        score_component = np.tanh(score_gap / trigger) * float(params.get('LIVE_EV_SCORE_EDGE_SCALE', 0.012))
        proba_component = (ai_proba - 0.50) * float(params.get('LIVE_EV_PROBA_EDGE_SCALE', 0.050))
        readiness_component = ((entry_readiness * 0.45 + preentry_score * 0.25 + confirm_score * 0.30) - 0.50) * float(params.get('LIVE_EV_READINESS_SCALE', 0.012))
        risk_penalty = (exit_hazard * 0.50 + reversal_risk * 0.30 + breakout_risk * 0.20) * float(params.get('LIVE_EV_RISK_PENALTY_SCALE', 0.010))

        live_ev = historical_component + score_component + proba_component + readiness_component - risk_penalty
        cap = abs(float(params.get('LIVE_EV_ABS_CAP', 0.20)))
        return pd.to_numeric(live_ev, errors='coerce').fillna(0.0).clip(-cap, cap)

    def _apply_dual_path_state_machine(self, df: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        out = df.copy()
        if out.empty:
            return out

        def _series(name: str, default: float = 0.0) -> pd.Series:
            if name in out.columns:
                return pd.to_numeric(out[name], errors='coerce').fillna(default)
            return pd.Series([default] * len(out), index=out.index, dtype=float)

        trigger = max(1.0, float(params.get('TRIGGER_SCORE', 2.0)))
        legacy_influence = max(0.0, min(1.0, float(params.get('LEGACY_CONFIRM_INFLUENCE', 0.0))))
        watch_th, pilot_th, full_th, _readiness_min, _pilot_confirm_min = entry_thresholds(params)
        pilot_risk_cap, full_risk_cap = risk_caps(params)
        require_alignment = bool(params.get('STATE_MACHINE_CONFIRM_REQUIRES_ALIGNMENT', False))

        weighted_buy = _series('Weighted_Buy_Score', 0.0) if 'Weighted_Buy_Score' in out.columns else _series('Buy_Score', 0.0)
        weighted_sell = _series('Weighted_Sell_Score', 0.0) if 'Weighted_Sell_Score' in out.columns else _series('Sell_Score', 0.0)
        score_gap = _series('Score_Gap', 0.0) if 'Score_Gap' in out.columns else (weighted_buy - weighted_sell)
        raw_ai_proba_legacy = (_series('AI_Proba', 0.5) if 'AI_Proba' in out.columns else (0.5 + score_gap.clip(-2, 2) * 0.10)).clip(0.01, 0.99)
        legacy_ai_influence = max(0.0, min(1.0, float(params.get('LEGACY_AI_PROBA_INFLUENCE', legacy_influence))))
        ai_proba_legacy = (0.5 * (1.0 - legacy_ai_influence) + raw_ai_proba_legacy * legacy_ai_influence).clip(0.01, 0.99)
        entry_readiness = self._clip01(_series('Entry_Readiness', 0.0))
        breakout_risk = self._clip01(_series('Breakout_Risk_Next3', 0.0))
        reversal_risk = self._clip01(_series('Reversal_Risk_Next3', 0.0))
        exit_hazard = self._clip01(_series('Exit_Hazard_Score', 0.0))
        bull_emerge = self._clip01(_series('Bull_Emerging_Score', 0.0))
        bear_emerge = self._clip01(_series('Bear_Emerging_Score', 0.0))
        range_compression = self._clip01(_series('Range_Compression_Score', 0.0))
        breakout_readiness = self._clip01(_series('Breakout_Readiness', 0.0))
        trend_exhaustion = self._clip01(_series('Trend_Exhaustion_Score', 0.0))
        trend_conf = self._clip01(_series('Trend_Confidence', 0.0))
        range_conf = self._clip01(_series('Range_Confidence', 0.0))
        rs_mkt = self._clip01(_series('RS_vs_Market_20_Pctl', 0.5))
        volume_delta = _series('Volume_Z20_Delta', 0.0).clip(-1.0, 1.0)
        foreign_delta = _series('Foreign_Ratio_Delta_3d', 0.0).clip(-1.0, 1.0)
        total_delta = _series('Total_Ratio_Delta_3d', 0.0).clip(-1.0, 1.0)
        score_gap_slope = _series('Score_Gap_Slope_3d', 0.0).clip(-2.0, 2.0) * legacy_influence
        proba_delta = _series('Proba_Delta_3d', 0.0).clip(-0.5, 0.5)
        next_bull = self._clip01(_series('Next_Regime_Prob_Bull', 0.34))
        next_bear = self._clip01(_series('Next_Regime_Prob_Bear', 0.33))
        next_range = self._clip01(_series('Next_Regime_Prob_Range', 0.33))
        transition_label = out['Transition_Label'].astype(str) if 'Transition_Label' in out.columns else pd.Series(['Stable'] * len(out), index=out.index)

        legacy_long_raw = self._clip01((weighted_buy / max(trigger, 1.0)).clip(0.0, 1.5) / 1.5)
        legacy_short_raw = self._clip01((weighted_sell / max(trigger, 1.0)).clip(0.0, 1.5) / 1.5)
        legacy_range_raw = self._clip01(1.0 - (score_gap.abs() / max(trigger, 1.0)).clip(0.0, 1.0))
        legacy_long = legacy_long_raw * legacy_influence
        legacy_short = legacy_short_raw * legacy_influence
        legacy_range = legacy_range_raw * legacy_influence
        structural_long = self._clip01(_series('Structural_Long_Bias', 0.0))
        structural_short = self._clip01(_series('Structural_Short_Bias', 0.0))
        structural_range = self._clip01(_series('Structural_Range_Bias', 0.0))

        long_pre = self._clip01(0.24 * bull_emerge + 0.16 * entry_readiness + 0.12 * ((score_gap_slope.clip(lower=0.0)) / 2.0) + 0.08 * ((volume_delta.clip(lower=0.0)) / 1.0) + 0.08 * ((foreign_delta.clip(lower=0.0) + total_delta.clip(lower=0.0)) / 2.0) + 0.08 * breakout_readiness + 0.08 * rs_mkt.clip(lower=0.0) + 0.16 * structural_long)
        short_pre = self._clip01(0.24 * bear_emerge + 0.16 * entry_readiness + 0.12 * (((-score_gap_slope).clip(lower=0.0)) / 2.0) + 0.08 * (((-volume_delta).clip(lower=0.0)) / 1.0) + 0.08 * (((-foreign_delta).clip(lower=0.0) + (-total_delta).clip(lower=0.0)) / 2.0) + 0.08 * breakout_readiness + 0.08 * (1.0 - rs_mkt) + 0.16 * structural_short)
        range_pre = self._clip01(0.25 * range_compression + 0.18 * range_conf + 0.12 * legacy_range + 0.09 * (1.0 - breakout_risk) + 0.06 * (1.0 - reversal_risk) + 0.07 * self._clip01(_series('Range_Bounce_Quality', 0.0)) + 0.07 * self._clip01(_series('Range_Fade_Quality', 0.0)) + 0.16 * structural_range)

        long_confirm = self._clip01(0.30 * long_pre + 0.22 * trend_conf + 0.18 * ai_proba_legacy + 0.10 * (1.0 - breakout_risk) + 0.10 * (1.0 - reversal_risk) + 0.10 * legacy_long)
        short_confirm = self._clip01(0.30 * short_pre + 0.22 * trend_conf + 0.18 * ai_proba_legacy + 0.10 * (1.0 - breakout_risk) + 0.10 * (1.0 - reversal_risk) + 0.10 * legacy_short)
        range_confirm = self._clip01(0.32 * range_pre + 0.20 * range_conf + 0.15 * ai_proba_legacy + 0.15 * (1.0 - breakout_risk) + 0.08 * (1.0 - exit_hazard) + 0.10 * legacy_range)

        dominant_pre = pd.concat({'LONG': long_pre, 'SHORT': short_pre, 'RANGE': range_pre}, axis=1)
        dominant_confirm = pd.concat({'LONG': long_confirm, 'SHORT': short_confirm, 'RANGE': range_confirm}, axis=1)
        dominant_lane = dominant_pre.idxmax(axis=1)
        dominant_pre_score = dominant_pre.max(axis=1)
        dominant_confirm_score = dominant_confirm.lookup(dominant_confirm.index, dominant_lane) if hasattr(dominant_confirm, 'lookup') else pd.Series([dominant_confirm.loc[idx, lane] for idx, lane in dominant_lane.items()], index=dominant_lane.index)

        align_long = (next_bull >= np.maximum(next_bear, next_range) - 0.03) | transition_label.str.contains('Bull', case=False, na=False) | out.get('Regime', pd.Series(['']*len(out), index=out.index)).astype(str).str.contains('多頭', na=False)
        align_short = (next_bear >= np.maximum(next_bull, next_range) - 0.03) | transition_label.str.contains('Bear', case=False, na=False) | out.get('Regime', pd.Series(['']*len(out), index=out.index)).astype(str).str.contains('空頭', na=False)
        align_range = (next_range >= np.maximum(next_bull, next_bear) - 0.03) | transition_label.str.contains('Range', case=False, na=False) | out.get('Regime', pd.Series(['']*len(out), index=out.index)).astype(str).str.contains('區間', na=False)
        confirm_aligned = pd.Series(False, index=out.index)
        confirm_aligned = confirm_aligned.where(~(dominant_lane == 'LONG'), align_long)
        confirm_aligned = confirm_aligned.where(~(dominant_lane == 'SHORT'), align_short)
        confirm_aligned = confirm_aligned.where(~(dominant_lane == 'RANGE'), align_range)

        max_risk = pd.concat([breakout_risk, reversal_risk, exit_hazard], axis=1).max(axis=1)
        watch_ok = dominant_pre_score >= watch_th
        pilot_ok = (dominant_pre_score >= pilot_th) & (max_risk <= pilot_risk_cap)
        full_ok = (dominant_confirm_score >= full_th) & (max_risk <= full_risk_cap) & (confirm_aligned | (not require_alignment))

        early_state = np.where(pilot_ok, 'PILOT_ENTRY', np.where(watch_ok, 'PREPARE', 'NO_ENTRY'))
        confirm_state = np.where(full_ok, 'FULL_ENTRY', 'WAIT_CONFIRM')
        entry_state = np.where(full_ok, 'FULL_ENTRY', np.where(pilot_ok, 'PILOT_ENTRY', np.where(watch_ok, 'PREPARE', 'NO_ENTRY')))
        entry_path = np.where(full_ok, 'CONFIRMATION', np.where(watch_ok, 'PREEMPTIVE', 'NONE'))

        exit_warn, exit_reduce, exit_defend, exit_hard = exit_thresholds(params)
        exit_state = np.where(exit_hazard >= exit_hard, 'EXIT', np.where(exit_hazard >= exit_defend, 'DEFEND', np.where(exit_hazard >= exit_reduce, 'REDUCE', 'HOLD')))
        exit_path_state = np.where(exit_hazard >= exit_hard, 'EXIT', np.where(exit_hazard >= exit_defend, 'DEFEND', np.where(exit_hazard >= exit_reduce, 'REDUCE', np.where(exit_hazard >= exit_warn, 'WATCH_EXIT', 'HOLD'))))

        pilot_mult = float(params.get('PILOT_ALLOC_MULTIPLIER', 0.33))
        full_mult = float(params.get('FULL_ALLOC_MULTIPLIER', 1.00))
        exit_defend_mult = float(params.get('EXIT_DEFEND_POSITION_MULTIPLIER', 0.60))
        exit_reduce_mult = float(params.get('EXIT_REDUCE_POSITION_MULTIPLIER', 0.35))
        exit_hard_mult = float(params.get('EXIT_HARD_EXIT_POSITION_MULTIPLIER', 0.00))
        stop_tighten_defend = float(params.get('EXIT_STOP_TIGHTEN_DEFEND', 0.80))
        stop_tighten_reduce = float(params.get('EXIT_STOP_TIGHTEN_REDUCE', 0.60))
        stop_tighten_exit = float(params.get('EXIT_STOP_TIGHTEN_EXIT', 0.00))
        synthetic_kelly = float(params.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03))
        raw_kelly = _series('Kelly建議倉位', 0.0) if 'Kelly建議倉位' in out.columns else _series('Kelly_Pos', 0.0)
        effective_kelly = raw_kelly.where(raw_kelly > 0, synthetic_kelly)
        entry_position_mult = np.where(entry_state == 'FULL_ENTRY', full_mult, np.where(entry_state == 'PILOT_ENTRY', pilot_mult, 0.0))
        exit_position_mult = np.where(exit_state == 'EXIT', exit_hard_mult, np.where(exit_state == 'REDUCE', exit_reduce_mult, np.where(exit_state == 'DEFEND', exit_defend_mult, 1.0)))
        target_position_mult = entry_position_mult * exit_position_mult
        state_kelly = effective_kelly * target_position_mult
        stop_tighten_mult = np.where(exit_state == 'EXIT', stop_tighten_exit, np.where(exit_state == 'REDUCE', stop_tighten_reduce, np.where(exit_state == 'DEFEND', stop_tighten_defend, 1.0)))
        can_add_position = ((entry_state == 'FULL_ENTRY') & (exit_state == 'HOLD')).astype(int)
        can_open_fresh = ((entry_state == 'FULL_ENTRY') & (exit_state == 'HOLD')).astype(int)
        exit_action = np.where(exit_state == 'EXIT', 'FLAT_EXIT', np.where(exit_state == 'REDUCE', 'TRIM_POSITION', np.where(exit_state == 'DEFEND', 'TIGHTEN_AND_DEFEND', np.where(exit_path_state == 'WATCH_EXIT', 'WATCH_EXIT', 'HOLD'))))

        lane_to_tag = {'LONG': '多方進場', 'SHORT': '空方進場', 'RANGE': '區間進場'}
        dominant_tag = dominant_lane.map(lane_to_tag).fillna('無')
        out['Golden_Type_Legacy'] = out.get('Golden_Type', '無')
        out['PreEntry_Long_Score'] = long_pre.round(6)
        out['PreEntry_Short_Score'] = short_pre.round(6)
        out['PreEntry_Range_Score'] = range_pre.round(6)
        out['PreEntry_Score'] = dominant_pre_score.round(6)
        out['Confirm_Long_Score'] = long_confirm.round(6)
        out['Confirm_Short_Score'] = short_confirm.round(6)
        out['Confirm_Range_Score'] = range_confirm.round(6)
        out['Confirm_Entry_Score'] = dominant_confirm_score.round(6)
        out['Legacy_Long_Confirm_Pressure'] = legacy_long_raw.round(6)
        out['Legacy_Short_Confirm_Pressure'] = legacy_short_raw.round(6)
        out['Legacy_Range_Confirm_Pressure'] = legacy_range_raw.round(6)
        out['Legacy_Confirm_Influence'] = float(legacy_influence)
        out['Legacy_Score_Alert_Only'] = int(bool(params.get('LEGACY_SCORE_ALERT_ONLY', legacy_influence <= 0.0)))
        out['Watch_Eligible'] = watch_ok.astype(int)
        out['Pilot_Eligible'] = pilot_ok.astype(int)
        out['Full_Eligible'] = full_ok.astype(int)
        out['Early_Path_State'] = pd.Series(early_state, index=out.index)
        out['Confirm_Path_State'] = pd.Series(confirm_state, index=out.index)
        out['Entry_State'] = pd.Series(entry_state, index=out.index)
        out['Entry_Path'] = pd.Series(entry_path, index=out.index)
        out['StateMachine_Direction'] = dominant_lane
        out['Confirm_Transition_Aligned'] = confirm_aligned.astype(int)
        out['Pilot_Position_Multiplier'] = float(pilot_mult)
        out['Full_Position_Multiplier'] = float(full_mult)
        out['StateMachine_Kelly_Pos'] = pd.Series(state_kelly, index=out.index).round(6)
        out['Exit_State'] = pd.Series(exit_state, index=out.index)
        out['Exit_Path_State'] = pd.Series(exit_path_state, index=out.index)
        out['Exit_Position_Multiplier'] = pd.Series(exit_position_mult, index=out.index).round(6)
        out['Target_Position_Multiplier'] = pd.Series(target_position_mult, index=out.index).round(6)
        out['Target_Position'] = pd.Series(state_kelly, index=out.index).round(6)
        out['Stop_Tighten_Multiplier'] = pd.Series(stop_tighten_mult, index=out.index).round(6)
        out['Can_Add_Position'] = pd.Series(can_add_position, index=out.index)
        out['Can_Open_Fresh_Position'] = pd.Series(can_open_fresh, index=out.index)
        out['Exit_Action'] = pd.Series(exit_action, index=out.index)
        out['AI_Proba_Legacy'] = raw_ai_proba_legacy.round(6)
        out['AI_Proba_Legacy_Effective'] = ai_proba_legacy.round(6)
        state_proba = self._clip01(0.58 * dominant_confirm_score + 0.22 * dominant_pre_score + 0.12 * (1.0 - max_risk) + 0.08 * self._clip01(0.5 + proba_delta))
        out['AI_Proba'] = ((ai_proba_legacy * 0.30) + (state_proba * 0.70)).clip(0.01, 0.99).round(6)
        out['Golden_Type'] = dominant_tag.where(pd.Series(entry_state, index=out.index) != 'NO_ENTRY', '無')
        out['Direction'] = dominant_lane
        return out

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
        params = dict(getattr(self, 'params', {}) or {})
        if p:
            params.update(p)
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
        out['RSI'] = self._rsi(out['Close'], int(params.get('RSI_PERIOD', 14)))
        ema12 = out['Close'].ewm(span=int(params.get('MACD_FAST', 12)), adjust=False).mean()
        ema26 = out['Close'].ewm(span=int(params.get('MACD_SLOW', 26)), adjust=False).mean()
        out['MACD'] = ema12 - ema26
        out['MACD_Signal'] = out['MACD'].ewm(span=int(params.get('MACD_SIGNAL', 9)), adjust=False).mean()
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

        def _feature_col(name: str, default: float = 0.0) -> pd.Series:
            if name in out.columns:
                return pd.to_numeric(out[name], errors='coerce').fillna(default).astype(float)
            return pd.Series([default] * len(out), index=out.index, dtype=float)

        foreign_ratio = _feature_col('Foreign_Ratio', 0.0)
        total_ratio = _feature_col('Total_Ratio', 0.0)
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
        out['buy_c7'] = (foreign_ratio > 0).astype(int)
        out['sell_c7'] = (foreign_ratio < 0).astype(int)
        out['buy_c8'] = ((out['ADX14'] >= float(params.get('ADX_TREND_THRESHOLD', 20))) & (out['Close'] > out['MA20'])).astype(int)
        out['sell_c8'] = ((out['ADX14'] >= float(params.get('ADX_TREND_THRESHOLD', 20))) & (out['Close'] < out['MA20'])).astype(int)
        out['buy_c9'] = (total_ratio > 0).astype(int)
        out['sell_c9'] = (total_ratio < 0).astype(int)

        out['Buy_Score'] = out[[f'buy_c{i}' for i in range(2, 10)]].sum(axis=1)
        out['Sell_Score'] = out[[f'sell_c{i}' for i in range(2, 10)]].sum(axis=1)
        out = _apply_weighted_scores(out, params)
        out = _assign_golden_type(out, float(params.get('TRIGGER_SCORE', 2)))
        out['Golden_Type_Legacy'] = out.get('Golden_Type', '無')

        out['Vol_Squeeze'] = (out['BB_Width'] < out['BB_Width'].rolling(20, min_periods=5).mean()).fillna(False).astype(int)
        out['Fake_Breakout'] = ((out['Close'] < out['MA20']) & (out['RSI'] > 55)).fillna(False)
        out['Bear_Trap'] = ((out['Close'] > out['MA20']) & (out['RSI'] < 45)).fillna(False)
        out['Absorption'] = ((out['Close'].pct_change().fillna(0) < 0) & (total_ratio > 0)).astype(int)
        out['MR_Long_Spring'] = ((out['Low'] < out['BB_Lower']) & (out['RSI'] < 35)).fillna(False).astype(int)
        out['MR_Short_Trap'] = ((out['High'] > out['BB_Upper']) & (out['RSI'] > 65)).fillna(False).astype(int)
        out['MR_Long_Accumulation'] = ((out['Close'] > out['MA20']) & (total_ratio > 0)).astype(int)
        out['MR_Short_Distribution'] = ((out['Close'] < out['MA20']) & (total_ratio < 0)).astype(int)
        out = self._infer_regime(out, params)
        out['AI_Proba'] = (0.5 + out['Score_Gap'].clip(-2, 2) * 0.1).clip(0.01, 0.99)
        hold_days = int(params.get('ML_LABEL_HOLD_DAYS', 5))
        # Training/backtest-only forward label.  Do not use this column in live gates.
        out['Forward_Label_Return'] = (out['Close'].shift(-hold_days) / out['Close'] - 1).fillna(0.0)
        out['Forward_Label_Hold_Days'] = hold_days
        out['Forward_Label_Source'] = 'future_close_shift_training_only'
        out['Regime_Raw'] = out.get('Regime', '區間盤整')
        try:
            out = self.regime_service.enrich_dataframe(out)
            if 'Hysteresis_Regime_Label' in out.columns:
                out['Regime'] = out['Hysteresis_Regime_Label'].where(out['Hysteresis_Regime_Label'].astype(str) != '', out['Regime_Raw'])
                out['Regime_Hysteresis_Applied'] = 1
            else:
                out['Regime_Hysteresis_Applied'] = 0
            out['Regime_Source'] = out.get('Regime_Source', 'direction_strength_environment_v2').astype(str) + '|transition_hysteresis_v1'
        except Exception:
            out['Regime_Hysteresis_Applied'] = 0

        try:
            out = self.features.enrich_from_history(out)
        except Exception:
            pass

        stats = _compute_realized_signal_stats(out, params, hold_days=int(params.get('ML_LABEL_HOLD_DAYS', 5)))
        for k, v in stats.items():
            if k == 'Realized_Signal_Returns':
                continue
            out[k] = v
        out = self._apply_dual_path_state_machine(out, params)
        live_ev = self._build_live_expected_return(out, params)
        out['Expected_Return'] = live_ev
        out['Heuristic_EV'] = live_ev
        out['Live_EV'] = live_ev
        # Backward-compatible name: now live-safe, not future-shift leakage.
        out['Realized_EV'] = live_ev
        out['EV_Source'] = 'live_safe_expected_return_v1'
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
        latest['Regime_Hysteresis_Applied'] = int(bool(latest.get('Regime_Hysteresis_Applied', 0)))
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
            'approved_strategy_param_mount': dict(getattr(self, 'params', {}).get('_approved_param_mount', {})) if isinstance(getattr(self, 'params', {}), dict) else {},
            'regime_engine': 'direction_strength_environment_v2 + transition_hysteresis_v1',
            'status': 'screening_engine_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🛰️ screening engine detached: {self.runtime_path}')
        return self.runtime_path, payload
