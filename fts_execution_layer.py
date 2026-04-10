# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from config import PARAMS
from fts_strategy_policy_layer import get_active_strategy, get_strategy_policy

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
    except Exception:
        return default


def _safe_int(x, default=0):
    try:
        if pd.isna(x):
            return default
        return int(float(x))
    except Exception:
        return default


def direction_bucket(direction_text: str) -> str:
    s = str(direction_text)
    return 'SHORT' if ('空' in s or 'Short' in s or 'SELL' in s.upper()) else 'LONG'


def build_entry_metrics(row, params=PARAMS):
    structure = row.get('Structure', row.get('Setup_Tag', 'AI訊號'))
    regime = row.get('Regime', '未知')
    realized_ev = _safe_float(row.get('Realized_EV', 0.0), 0.0)
    sample_size = _safe_int(row.get('Sample_Size', row.get('歷史訊號樣本數', 0)), 0)
    ai_proba = _safe_float(row.get('AI_Proba', 0.5), 0.5)
    weighted_buy = _safe_float(row.get('Weighted_Buy_Score', 0.0), 0.0)
    weighted_sell = _safe_float(row.get('Weighted_Sell_Score', 0.0), 0.0)
    score_gap = _safe_float(row.get('Score_Gap', 0.0), 0.0)

    try:
        dummy_vol = 0.05
        trend_is_with_me = '多頭' in str(regime)
        adx_is_strong = ai_proba >= 0.55
        active_strategy = get_active_strategy(structure, regime=regime)
        dynamic_sl, dynamic_tp, _ = active_strategy.get_exit_rules(params, dummy_vol, trend_is_with_me, adx_is_strong, 0)
        policy = get_strategy_policy(structure, regime=regime)
    except Exception:
        dynamic_sl = float(params.get('SL_MIN_PCT', 0.03))
        dynamic_tp = float(params.get('TP_BASE_PCT', 0.10))
        policy = {'name': 'fallback'}

    rr_ratio = (dynamic_tp / dynamic_sl) if dynamic_sl > 0 else 0.0
    risk_budget_ratio = 0.05
    if sample_size < 8:
        risk_budget_ratio = 0.03
    if realized_ev <= 0 or ai_proba < 0.5:
        risk_budget_ratio = min(risk_budget_ratio, 0.02)
    if score_gap <= 0:
        risk_budget_ratio = min(risk_budget_ratio, 0.015)

    return {
        '市場狀態': regime,
        '進場陣型': structure,
        '策略名稱': policy.get('name', 'fallback'),
        '策略劇本': policy.get('playbook', 'fallback'),
        '期望值': realized_ev,
        '預期停損(%)': round(dynamic_sl * 100, 3),
        '預期停利(%)': round(dynamic_tp * 100, 3),
        '風報比(RR)': round(rr_ratio, 3),
        '風險金額比率': risk_budget_ratio,
        'Weighted_Buy_Score': weighted_buy,
        'Weighted_Sell_Score': weighted_sell,
        'Score_Gap': score_gap,
    }


def signal_gate(row, model_decision=None, params=PARAMS) -> GateDecision:
    reasons: list[str] = []
    kelly_pct = _safe_float(row.get('Kelly_Pos', 0.0), 0.0)
    weighted_buy = _safe_float(row.get('Weighted_Buy_Score', 0.0), 0.0)
    weighted_sell = _safe_float(row.get('Weighted_Sell_Score', 0.0), 0.0)
    score_gap = _safe_float(row.get('Score_Gap', 0.0), 0.0)
    health = str(row.get('Health', 'KEEP')).upper()

    if kelly_pct <= 0:
        reasons.append('kelly_zero')
    if health == 'KILL':
        reasons.append('health_kill')
    if weighted_buy < max(2.0, float(params.get('TRIGGER_SCORE', 2))):
        reasons.append('weighted_buy_below_trigger')
    if weighted_sell >= weighted_buy:
        reasons.append('sell_pressure_not_cleared')
    if score_gap <= 0:
        reasons.append('negative_score_gap')

    if model_decision is not None and not bool(getattr(model_decision, 'approved', False)):
        reasons.extend(list(getattr(model_decision, 'veto_reasons', [])))
    elif model_decision is None:
        ai_proba = _safe_float(row.get('AI_Proba', 0.0), 0.0)
        realized_ev = _safe_float(row.get('Realized_EV', 0.0), 0.0)
        if ai_proba < 0.50:
            reasons.append('ai_proba_low')
        if realized_ev <= 0:
            reasons.append('realized_ev_non_positive')

    return GateDecision(allowed=not reasons, reasons=reasons)


def portfolio_gate(row, total_nav, portfolio_state, sector_name='未知產業', params=PARAMS) -> GateDecision:
    reasons: list[str] = []
    if total_nav <= 0:
        reasons.append('total_nav_invalid')
        return GateDecision(allowed=False, reasons=reasons)

    direction = direction_bucket(row.get('Direction', ''))
    requested_alloc = _safe_float(row.get('Kelly_Pos', 0.0), 0.0)

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
    if curr_price <= 0:
        return PositionPlan(False, 'price_invalid', 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    requested_alloc = _safe_float(row.get('Kelly_Pos', 0.0), 0.0)
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
