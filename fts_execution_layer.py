# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from config import PARAMS
from fts_strategy_policy_layer import get_active_strategy, get_strategy_policy
from fts_exception_policy import record_diagnostic
from fts_symbol_contract import get_ticker_symbol, ensure_execution_symbol

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
    PATHS = _Paths()

RUNTIME_PATH = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'execution_layer_status.json'


@dataclass
class GateDecision:
    allowed: bool
    reasons: list[str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PositionPlan:
    allowed: bool
    reason: str
    shares: int
    total_cost: float
    requested_alloc: float
    applied_alloc: float
    risk_amount: float
    stop_pct: float
    take_profit_pct: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(x, default=0.0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception as exc:
        record_diagnostic('execution_layer', 'safe_float_cast', exc, severity='warning', fail_closed=False, context={'value': str(x)[:80]})
        return default


def _safe_int(x, default=0):
    try:
        if pd.isna(x):
            return default
        return int(float(x))
    except Exception as exc:
        record_diagnostic('execution_layer', 'safe_int_cast', exc, severity='warning', fail_closed=False, context={'value': str(x)[:80]})
        return default


def direction_bucket(direction_text: str) -> str:
    s = str(direction_text)
    u = s.upper()
    if 'RANGE' in u or '區間' in s:
        return 'RANGE'
    return 'SHORT' if ('空' in s or 'SHORT' in u or 'SELL' in u) else 'LONG'


def build_entry_metrics(row, params=PARAMS):
    structure = row.get('Structure', row.get('Setup_Tag', 'AI訊號'))
    regime = row.get('Regime', '未知')
    realized_ev = _safe_float(row.get('Realized_EV', 0.0), 0.0)
    strategy_ev = _safe_float(row.get('Strategy_EV_SQL', row.get('Expected_Return', realized_ev)), realized_ev)
    effective_ev = max(realized_ev, strategy_ev)
    sample_size = _safe_int(row.get('Sample_Size', row.get('歷史訊號樣本數', 0)), 0)
    ai_proba = _safe_float(row.get('AI_Proba', 0.5), 0.5)
    weighted_buy = _safe_float(row.get('Weighted_Buy_Score', 0.0), 0.0)
    weighted_sell = _safe_float(row.get('Weighted_Sell_Score', 0.0), 0.0)
    score_gap = _safe_float(row.get('Score_Gap', 0.0), 0.0)
    legacy_influence = _safe_float(params.get('LEGACY_CONFIRM_INFLUENCE', row.get('Legacy_Confirm_Influence', 0.0)), 0.0)
    entry_readiness = _safe_float(row.get('Entry_Readiness', 0.0), 0.0)
    breakout_risk = _safe_float(row.get('Breakout_Risk_Next3', 0.0), 0.0)
    reversal_risk = _safe_float(row.get('Reversal_Risk_Next3', 0.0), 0.0)
    exit_hazard = _safe_float(row.get('Exit_Hazard_Score', 0.0), 0.0)
    transition_label = str(row.get('Transition_Label', ''))
    hysteresis_label = str(row.get('Hysteresis_Regime_Label', row.get('Regime_Label', regime)))
    hysteresis_armed = _safe_float(row.get('Hysteresis_Switch_Armed', 0.0), 0.0)
    hysteresis_locked = _safe_float(row.get('Hysteresis_Locked', 0.0), 0.0)
    entry_state = str(row.get('Entry_State', 'NO_ENTRY')).upper()
    early_state = str(row.get('Early_Path_State', entry_state)).upper()
    confirm_state = str(row.get('Confirm_Path_State', 'WAIT_CONFIRM')).upper()
    entry_path = str(row.get('Entry_Path', 'NONE')).upper()
    preentry_score = _safe_float(row.get('PreEntry_Score', 0.0), 0.0)
    confirm_score = _safe_float(row.get('Confirm_Entry_Score', 0.0), 0.0)

    policy_ready = True
    try:
        dummy_vol = 0.05
        trend_is_with_me = '多頭' in str(regime)
        adx_is_strong = ai_proba >= 0.55
        active_strategy = get_active_strategy(structure, regime=regime)
        dynamic_sl, dynamic_tp, _ = active_strategy.get_exit_rules(params, dummy_vol, trend_is_with_me, adx_is_strong, 0)
        policy = get_strategy_policy(structure, regime=regime)
    except Exception as exc:
        policy_ready = False
        record_diagnostic('execution_layer', 'build_entry_metrics_policy_load', exc, severity='error', fail_closed=True, context={'ticker_symbol': get_ticker_symbol(row), 'structure': structure, 'regime': regime})
        dynamic_sl = float(params.get('SL_MIN_PCT', 0.03))
        dynamic_tp = float(params.get('TP_BASE_PCT', 0.10))
        policy = {'name': 'policy_unavailable_fail_closed', 'playbook': 'fail_closed'}

    rr_ratio = (dynamic_tp / dynamic_sl) if dynamic_sl > 0 else 0.0
    risk_budget_ratio = 0.05
    if sample_size < 8:
        risk_budget_ratio = 0.03
    if realized_ev <= 0 or ai_proba < 0.5:
        risk_budget_ratio = min(risk_budget_ratio, 0.02)
    if legacy_influence > 0 and score_gap <= 0:
        risk_budget_ratio = min(risk_budget_ratio, 0.015)
    if breakout_risk >= 0.75 or reversal_risk >= 0.75 or exit_hazard >= 0.75:
        risk_budget_ratio = min(risk_budget_ratio, 0.0125)
    elif entry_readiness >= 0.60 and realized_ev > 0 and ai_proba >= 0.52:
        risk_budget_ratio = min(max(risk_budget_ratio, 0.035), 0.06)
    if entry_state == 'PILOT_ENTRY':
        risk_budget_ratio = min(risk_budget_ratio, 0.02)
    elif entry_state == 'PREPARE':
        risk_budget_ratio = 0.0

    return {
        '市場狀態': regime,
        '進場陣型': structure,
        '策略名稱': policy.get('name', 'policy_unavailable_fail_closed'),
        '策略劇本': policy.get('playbook', 'fail_closed'),
        'Policy_Ready': bool(policy_ready),
        '期望值': realized_ev,
        '預期停損(%)': round(dynamic_sl * 100, 3),
        '預期停利(%)': round(dynamic_tp * 100, 3),
        '風報比(RR)': round(rr_ratio, 3),
        '風險金額比率': risk_budget_ratio,
        'Weighted_Buy_Score': weighted_buy,
        'Weighted_Sell_Score': weighted_sell,
        'Score_Gap': score_gap,
        'Entry_Readiness': entry_readiness,
        'Breakout_Risk_Next3': breakout_risk,
        'Reversal_Risk_Next3': reversal_risk,
        'Exit_Hazard_Score': exit_hazard,
        'Transition_Label': transition_label,
        'Hysteresis_Regime_Label': hysteresis_label,
        'Hysteresis_Switch_Armed': hysteresis_armed,
        'Hysteresis_Locked': hysteresis_locked,
        'Entry_State': entry_state,
        'Early_Path_State': early_state,
        'Confirm_Path_State': confirm_state,
        'Entry_Path': entry_path,
        'PreEntry_Score': preentry_score,
        'Confirm_Entry_Score': confirm_score,
        'Exit_State': str(row.get('Exit_State', 'HOLD')).upper(),
    }


def _lane_thresholds(strategy_bucket: str, params=PARAMS) -> tuple[float, float, float]:
    lane = str(strategy_bucket or 'LONG').upper()
    if lane == 'SHORT':
        return (
            float(params.get('SHORT_MIN_PROBA', params.get('LONG_MIN_PROBA', 0.52))),
            float(params.get('SHORT_MIN_OOT_EV', 0.0)),
            float(params.get('SHORT_MIN_CONFIDENCE', 0.50)),
        )
    if lane == 'RANGE':
        return (
            float(params.get('RANGE_MIN_PROBA', params.get('LONG_MIN_PROBA', 0.52))),
            float(params.get('RANGE_MIN_OOT_EV', 0.0)),
            float(params.get('RANGE_MIN_CONFIDENCE', 0.50)),
        )
    return (
        float(params.get('LONG_MIN_PROBA', 0.52)),
        float(params.get('LONG_MIN_OOT_EV', 0.0)),
        float(params.get('LONG_MIN_CONFIDENCE', 0.50)),
    )


def signal_gate(row, model_decision=None, params=PARAMS) -> GateDecision:
    reasons: list[str] = []
    diagnostics: list[str] = []
    kelly_pct = _safe_float(row.get('Kelly_Pos', 0.0), 0.0)
    weighted_buy = _safe_float(row.get('Weighted_Buy_Score', 0.0), 0.0)
    weighted_sell = _safe_float(row.get('Weighted_Sell_Score', 0.0), 0.0)
    score_gap = _safe_float(row.get('Score_Gap', weighted_buy - weighted_sell), weighted_buy - weighted_sell)
    health = str(row.get('Health', 'KEEP')).upper()

    if kelly_pct <= 0:
        allow_synth_kelly = bool(params.get('SIGNAL_GATE_ALLOW_SYNTHETIC_KELLY', True))
        if allow_synth_kelly and str(row.get('Entry_State', 'NO_ENTRY')).upper() in {'PILOT_ENTRY', 'FULL_ENTRY'} and _safe_float(params.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03), 0.0) > 0:
            diagnostics.append('diag_synthetic_kelly_substitute')
        elif bool(params.get('SIGNAL_GATE_TREAT_KELLY_ZERO_AS_WARNING', True)):
            diagnostics.append('diag_kelly_zero')
        else:
            reasons.append('kelly_zero')
    if health == 'KILL':
        reasons.append('health_kill')

    raw_direction = direction_bucket(row.get('Direction', ''))
    strategy_bucket = str(row.get('Strategy_Bucket', raw_direction)).upper()
    if strategy_bucket not in {'LONG', 'SHORT', 'RANGE'}:
        strategy_bucket = raw_direction if raw_direction in {'LONG', 'SHORT', 'RANGE'} else 'LONG'

    min_proba, min_ev, min_conf = _lane_thresholds(strategy_bucket, params)
    ai_proba = _safe_float(row.get('AI_Proba', 0.0), 0.0)
    realized_ev = _safe_float(row.get('Realized_EV', 0.0), 0.0)
    strategy_ev = _safe_float(row.get('Strategy_EV_SQL', row.get('Expected_Return', realized_ev)), realized_ev)
    effective_ev = max(realized_ev, strategy_ev)
    signal_conf_raw = row.get('Signal_Confidence', row.get('SignalConfidence', row.get('訊號信心分數(%)', ai_proba)))
    signal_conf = _safe_float(signal_conf_raw, ai_proba)
    if signal_conf > 1.5:
        signal_conf = signal_conf / 100.0
    entry_state = str(row.get('Entry_State', 'NO_ENTRY')).upper()
    early_state = str(row.get('Early_Path_State', entry_state)).upper()
    confirm_state = str(row.get('Confirm_Path_State', 'WAIT_CONFIRM')).upper()
    preentry_score = _safe_float(row.get('PreEntry_Score', 0.0), 0.0)
    confirm_score = _safe_float(row.get('Confirm_Entry_Score', 0.0), 0.0)
    breakout_risk = _safe_float(row.get('Breakout_Risk_Next3', 0.0), 0.0)
    reversal_risk = _safe_float(row.get('Reversal_Risk_Next3', 0.0), 0.0)
    exit_hazard = _safe_float(row.get('Exit_Hazard_Score', 0.0), 0.0)
    pilot_proba = max(0.45, min_proba - float(params.get('PILOT_MIN_PROBA_BUFFER', 0.04)))
    pilot_conf = max(0.40, min_conf - float(params.get('PILOT_MIN_CONF_BUFFER', 0.06)))

    if entry_state == 'NO_ENTRY':
        reasons.append('state_machine_no_entry')
        row['_signal_gate_diagnostics'] = ['diag_state_machine_no_entry']
        return GateDecision(allowed=False, reasons=reasons)
    if entry_state == 'PREPARE':
        reasons.append('state_machine_watch_only')
        row['_signal_gate_diagnostics'] = ['diag_state_machine_watch_only']
        return GateDecision(allowed=False, reasons=reasons)

    # Weighted scores are kept as secondary diagnostics, not as the alpha gate.
    if strategy_bucket == 'SHORT':
        short_gap = weighted_sell - weighted_buy
        if weighted_sell < max(1.0, float(params.get('SHORT_TRIGGER_SCORE', params.get('TRIGGER_SCORE', 2.0)))):
            diagnostics.append('diag_weighted_sell_below_trigger')
        if weighted_buy >= weighted_sell:
            diagnostics.append('diag_buy_pressure_not_cleared')
        if short_gap <= 0:
            diagnostics.append('diag_negative_short_score_gap')
    elif strategy_bucket == 'RANGE':
        dominant_range_score = max(weighted_buy, weighted_sell)
        max_range_gap = float(params.get('RANGE_MAX_SCORE_GAP_ABS', 1.25))
        range_confidence = _safe_float(row.get('Range_Confidence', row.get('Range_Confidence_At_Label', signal_conf)), signal_conf)
        if dominant_range_score < max(1.0, float(params.get('RANGE_TRIGGER_SCORE', 1.0))):
            diagnostics.append('diag_range_score_below_trigger')
        if abs(score_gap) > max_range_gap:
            diagnostics.append('diag_range_score_gap_too_wide')
        if range_confidence < min_conf:
            reasons.append('range_confidence_low')
    else:
        if weighted_buy < max(1.0, float(params.get('TRIGGER_SCORE', 2.0))):
            diagnostics.append('diag_weighted_buy_below_trigger')
        if weighted_sell >= weighted_buy:
            diagnostics.append('diag_sell_pressure_not_cleared')
        if score_gap <= 0:
            diagnostics.append('diag_negative_score_gap')

    active_min_proba = min_proba
    active_min_conf = min_conf
    active_min_ev = min_ev
    if entry_state == 'PILOT_ENTRY':
        active_min_proba = pilot_proba
        active_min_conf = pilot_conf
        active_min_ev = float(params.get('PILOT_MIN_OOT_EV', min_ev))
        if max(breakout_risk, reversal_risk, exit_hazard) > float(params.get('PILOT_MAX_BREAKOUT_RISK', 0.88)):
            reasons.append('pilot_risk_too_high')
    elif max(breakout_risk, reversal_risk, exit_hazard) > float(params.get('FULL_MAX_BREAKOUT_RISK', 0.80)):
        reasons.append('full_entry_risk_too_high')

    if model_decision is not None:
        if not bool(getattr(model_decision, 'approved', False)):
            reasons.extend(list(getattr(model_decision, 'veto_reasons', [])))
        else:
            decision_proba = _safe_float(getattr(model_decision, 'proba', ai_proba), ai_proba)
            decision_ev = max(_safe_float(getattr(model_decision, 'realized_ev', realized_ev), realized_ev), strategy_ev)
            decision_conf = _safe_float(getattr(model_decision, 'signal_confidence', signal_conf), signal_conf)
            if decision_proba < active_min_proba:
                reasons.append(f'model_proba_low:{decision_proba:.3f}<{active_min_proba:.3f}')
            if decision_ev < active_min_ev:
                reasons.append(f'model_ev_low:{decision_ev:.4f}<{active_min_ev:.4f}')
            if decision_conf < active_min_conf:
                reasons.append(f'model_confidence_low:{decision_conf:.3f}<{active_min_conf:.3f}')
    else:
        if ai_proba < active_min_proba:
            reasons.append(f'ai_proba_low:{ai_proba:.3f}<{active_min_proba:.3f}')
        if effective_ev < active_min_ev:
            reasons.append(f'realized_ev_low:{effective_ev:.4f}<{active_min_ev:.4f}')
        if signal_conf < active_min_conf:
            reasons.append(f'signal_confidence_low:{signal_conf:.3f}<{active_min_conf:.3f}')

    # Expose heuristic weaknesses without blocking a model-approved trade.
    diagnostics.extend([f'entry_state={entry_state}', f'early_state={early_state}', f'confirm_state={confirm_state}', f'preentry_score={preentry_score:.3f}', f'confirm_score={confirm_score:.3f}'])
    row['_signal_gate_diagnostics'] = diagnostics
    return GateDecision(allowed=not reasons, reasons=reasons + diagnostics[:0])


def portfolio_gate(row, total_nav, portfolio_state, sector_name='未知產業', params=PARAMS) -> GateDecision:
    reasons: list[str] = []
    if total_nav <= 0:
        reasons.append('total_nav_invalid')
        return GateDecision(allowed=False, reasons=reasons)

    direction = direction_bucket(row.get('Direction', ''))
    entry_state = str(row.get('Entry_State', 'NO_ENTRY')).upper()
    pilot_mult = _safe_float(row.get('Pilot_Position_Multiplier', params.get('PILOT_ALLOC_MULTIPLIER', 0.33)), 0.33)
    full_mult = _safe_float(row.get('Full_Position_Multiplier', params.get('FULL_ALLOC_MULTIPLIER', 1.0)), 1.0)
    requested_alloc = _safe_float(row.get('StateMachine_Kelly_Pos', row.get('Kelly_Pos', 0.0)), 0.0)
    if requested_alloc <= 0:
        base_alloc = _safe_float(row.get('Kelly_Pos', params.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03)), 0.0)
        if entry_state == 'PILOT_ENTRY':
            requested_alloc = max(base_alloc, _safe_float(params.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03), 0.03)) * pilot_mult
        elif entry_state == 'FULL_ENTRY':
            requested_alloc = max(base_alloc, _safe_float(params.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03), 0.03)) * full_mult
    if entry_state not in {'PILOT_ENTRY', 'FULL_ENTRY'}:
        reasons.append('state_machine_not_executable')
        return GateDecision(allowed=False, reasons=reasons)

    max_sector_positions = int(params.get('PORT_MAX_SECTOR_POSITIONS', 2))
    max_sector_alloc = float(params.get('PORT_MAX_SECTOR_ALLOC', 0.35))
    max_total_alloc = float(params.get('PORT_MAX_TOTAL_ALLOC', 0.60))
    max_direction_alloc = float(params.get('PORT_MAX_DIRECTION_ALLOC', 0.45))
    max_single_pos = float(params.get('PORT_MAX_SINGLE_POS', 0.12))
    min_position = float(params.get('PORT_MIN_POSITION', 0.01))

    current_total = float(portfolio_state.get('total_alloc', 0.0))
    current_sector_alloc = float(portfolio_state.get('sector_alloc', {}).get(sector_name, 0.0))
    current_sector_count = int(portfolio_state.get('sector_count', {}).get(sector_name, 0))
    current_direction_alloc = float(portfolio_state.get('direction_alloc', {}).get(direction, 0.0))

    if requested_alloc < min_position:
        reasons.append('position_below_minimum')
    if requested_alloc > max_single_pos:
        reasons.append('position_above_single_limit')
    if current_sector_count >= max_sector_positions:
        reasons.append('sector_position_limit_reached')
    if current_total + requested_alloc > max_total_alloc:
        reasons.append('portfolio_total_alloc_limit')
    if current_sector_alloc + requested_alloc > max_sector_alloc:
        reasons.append('portfolio_sector_alloc_limit')
    if current_direction_alloc + requested_alloc > max_direction_alloc:
        reasons.append('portfolio_direction_alloc_limit')

    return GateDecision(allowed=not reasons, reasons=reasons)


def compute_position_plan(row, curr_price: float, total_nav: float, current_cash: float, entry_metrics: dict[str, Any], params=PARAMS) -> PositionPlan:
    if not bool(entry_metrics.get('Policy_Ready', True)):
        record_diagnostic('execution_layer', 'compute_position_plan_policy_not_ready', severity='error', fail_closed=True, context={'ticker_symbol': get_ticker_symbol(row)})
        return PositionPlan(False, 'policy_not_ready_fail_closed', 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    if curr_price <= 0:
        return PositionPlan(False, 'price_invalid', 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    entry_state = str(row.get('Entry_State', 'NO_ENTRY')).upper()
    pilot_mult = _safe_float(row.get('Pilot_Position_Multiplier', params.get('PILOT_ALLOC_MULTIPLIER', 0.33)), 0.33)
    full_mult = _safe_float(row.get('Full_Position_Multiplier', params.get('FULL_ALLOC_MULTIPLIER', 1.0)), 1.0)
    requested_alloc = _safe_float(row.get('StateMachine_Kelly_Pos', row.get('Kelly_Pos', 0.0)), 0.0)
    if requested_alloc <= 0:
        base_alloc = _safe_float(row.get('Kelly_Pos', params.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03)), 0.0)
        if entry_state == 'PILOT_ENTRY':
            requested_alloc = max(base_alloc, _safe_float(params.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03), 0.03)) * pilot_mult
        elif entry_state == 'FULL_ENTRY':
            requested_alloc = max(base_alloc, _safe_float(params.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03), 0.03)) * full_mult
    if entry_state not in {'PILOT_ENTRY', 'FULL_ENTRY'}:
        return PositionPlan(False, 'state_machine_not_executable', 0, 0.0, requested_alloc, 0.0, 0.0, 0.0, 0.0)
    stop_pct = max(_safe_float(entry_metrics.get('預期停損(%)', 0.0), 0.0) / 100.0, 1e-6)
    tp_pct = max(_safe_float(entry_metrics.get('預期停利(%)', 0.0), 0.0) / 100.0, 0.0)
    risk_budget_ratio = _safe_float(entry_metrics.get('風險金額比率', 0.0), 0.0)

    qty_by_cap = int((total_nav * requested_alloc) / curr_price)
    risk_budget_cash = max(total_nav * risk_budget_ratio, 0.0)
    qty_by_risk = int(risk_budget_cash / max(curr_price * stop_pct, 1e-6))
    shares = min(q for q in [qty_by_cap, qty_by_risk] if q > 0) if any(q > 0 for q in [qty_by_cap, qty_by_risk]) else 0
    if shares >= 1000:
        shares = int(shares // 1000) * 1000
    total_cost = curr_price * shares * (1 + float(params.get('FEE_RATE', 0.001425)) * float(params.get('FEE_DISCOUNT', 1.0)))

    if shares < 1:
        return PositionPlan(False, 'shares_below_minimum', 0, 0.0, requested_alloc, 0.0, 0.0, stop_pct, tp_pct)
    if total_cost > current_cash and not bool(params.get('IGNORE_CASH_LIMIT', False)):
        return PositionPlan(False, 'cash_insufficient', 0, total_cost, requested_alloc, 0.0, 0.0, stop_pct, tp_pct)

    applied_alloc = total_cost / total_nav if total_nav > 0 else 0.0
    risk_amount = total_cost * risk_budget_ratio
    plan = PositionPlan(True, 'ok', shares, total_cost, requested_alloc, applied_alloc, risk_amount, stop_pct, tp_pct)
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_PATH.write_text(json.dumps(plan.as_dict(), ensure_ascii=False, indent=2), encoding='utf-8')
    return plan


def apply_signal_gate(row, model_decision=None, params=PARAMS):
    direction = direction_bucket(row.get('Direction', ''))
    strategy_bucket = str(row.get('Strategy_Bucket', direction)).upper()
    if strategy_bucket not in {'LONG','SHORT','RANGE'}:
        strategy_bucket = 'SHORT' if direction == 'SHORT' else 'LONG'
    gate = signal_gate(row, model_decision=model_decision, params=params)
    payload = gate.as_dict()
    payload.update({
        'direction_bucket': direction,
        'strategy_bucket': strategy_bucket,
        'range_confidence': _safe_float(row.get('Range_Confidence', row.get('Range_Confidence_At_Label', 0.0)), 0.0),
        'approved_for_long': bool(gate.allowed and strategy_bucket == 'LONG'),
        'approved_for_short': bool(gate.allowed and strategy_bucket == 'SHORT'),
        'approved_for_range': bool(gate.allowed and strategy_bucket == 'RANGE'),
        'entry_state': str(row.get('Entry_State', 'NO_ENTRY')).upper(),
        'preentry_score': _safe_float(row.get('PreEntry_Score', 0.0), 0.0),
        'confirm_entry_score': _safe_float(row.get('Confirm_Entry_Score', 0.0), 0.0),
    })
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload
