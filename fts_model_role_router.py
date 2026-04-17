# -*- coding: utf-8 -*-
from __future__ import annotations

"""Model role router: entry alpha / risk-failure / sizing / execution.

The goal is to keep model outputs separated instead of mixing everything into
one composite gate.  The execution layer can consume these role decisions without
re-deciding alpha logic.
"""

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
    PATHS = _Paths()

try:
    from config import PARAMS  # type: ignore
except Exception:  # pragma: no cover
    PARAMS = {}

RUNTIME_PATH = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'model_role_router.json'


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == '':
            return default
        return float(value)
    except Exception:
        return default


@dataclass
class RoleDecision:
    role: str
    approved: bool
    score: float
    threshold: float
    action: str
    reasons: list[str]
    inputs: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ModelRoleBundle:
    entry_alpha: RoleDecision
    risk_failure: RoleDecision
    sizing: RoleDecision
    execution: RoleDecision

    @property
    def approved(self) -> bool:
        return bool(self.entry_alpha.approved and self.risk_failure.approved and self.sizing.approved and self.execution.approved)

    def veto_reasons(self) -> list[str]:
        out: list[str] = []
        for role in [self.entry_alpha, self.risk_failure, self.sizing, self.execution]:
            if not role.approved:
                out.extend([f'{role.role}:{r}' for r in role.reasons])
        return out

    def as_dict(self) -> dict[str, Any]:
        payload = {
            'approved': self.approved,
            'veto_reasons': self.veto_reasons(),
            'entry_alpha': self.entry_alpha.as_dict(),
            'risk_failure': self.risk_failure.as_dict(),
            'sizing': self.sizing.as_dict(),
            'execution': self.execution.as_dict(),
        }
        return payload


def build_model_role_bundle(row: Any, *, proba: float, expected_return: float, signal_confidence: float, base_approved: bool = True) -> ModelRoleBundle:
    get = row.get if hasattr(row, 'get') else lambda k, d=None: d
    entry_state = str(get('Entry_State', 'NO_ENTRY')).upper()
    entry_readiness = _safe_float(get('Entry_Readiness', 0.0), 0.0)
    preentry = _safe_float(get('PreEntry_Score', 0.0), 0.0)
    confirm = _safe_float(get('Confirm_Entry_Score', 0.0), 0.0)
    breakout_risk = _safe_float(get('Breakout_Risk_Next3', 0.0), 0.0)
    reversal_risk = _safe_float(get('Reversal_Risk_Next3', 0.0), 0.0)
    exit_hazard = _safe_float(get('Exit_Hazard_Score', 0.0), 0.0)
    liquidity_score = _safe_float(get('Liquidity_Score', get('liquidity_score', 0.0)), 0.0)
    adv20 = _safe_float(get('ADV20', get('ADV20_Proxy', 0.0)), 0.0)
    turnover = _safe_float(get('Turnover_Ratio', get('Turnover_Proxy', 0.0)), 0.0)
    kelly = _safe_float(get('Kelly_Pos', get('StateMachine_Kelly_Pos', 0.0)), 0.0)

    entry_threshold = float(PARAMS.get('ROLE_ENTRY_ALPHA_MIN_SCORE', 0.52))
    entry_score = float(0.45 * proba + 0.25 * max(signal_confidence, 0.0) + 0.20 * max(entry_readiness, 0.0) + 0.10 * max(confirm, preentry, 0.0))
    entry_reasons: list[str] = []
    if not base_approved:
        entry_reasons.append('base_model_not_approved')
    if entry_state not in {'PILOT_ENTRY', 'FULL_ENTRY'}:
        entry_reasons.append('entry_state_not_executable')
    if expected_return < float(PARAMS.get('ROLE_ENTRY_MIN_EV', PARAMS.get('MODEL_LAYER_MIN_EXPECTED_RETURN', -0.0015))):
        entry_reasons.append('expected_return_below_role_floor')
    if entry_score < entry_threshold:
        entry_reasons.append('entry_alpha_score_below_floor')
    entry = RoleDecision('entry_alpha', not entry_reasons, round(entry_score, 6), entry_threshold, 'ALLOW_ENTRY_ALPHA' if not entry_reasons else 'BLOCK_ENTRY_ALPHA', entry_reasons, {'proba': proba, 'expected_return': expected_return, 'signal_confidence': signal_confidence, 'entry_state': entry_state, 'entry_readiness': entry_readiness, 'confirm_score': confirm, 'preentry_score': preentry})

    risk_threshold = float(PARAMS.get('ROLE_RISK_FAILURE_MAX_SCORE', 0.78))
    risk_score = max(breakout_risk, reversal_risk, exit_hazard)
    risk_reasons: list[str] = []
    if risk_score > risk_threshold:
        risk_reasons.append('risk_failure_score_above_ceiling')
    if exit_hazard > float(PARAMS.get('ROLE_EXIT_HAZARD_HARD_BLOCK', 0.88)):
        risk_reasons.append('exit_hazard_hard_block')
    risk = RoleDecision('risk_failure', not risk_reasons, round(float(risk_score), 6), risk_threshold, 'ALLOW_RISK' if not risk_reasons else 'BLOCK_RISK', risk_reasons, {'breakout_risk': breakout_risk, 'reversal_risk': reversal_risk, 'exit_hazard': exit_hazard})

    sizing_threshold = float(PARAMS.get('ROLE_SIZING_MIN_ALLOC', PARAMS.get('PORT_MIN_POSITION', 0.01)))
    size_score = max(kelly, _safe_float(get('StateMachine_Kelly_Pos', 0.0), 0.0))
    sizing_reasons: list[str] = []
    if size_score <= 0:
        sizing_reasons.append('sizing_model_zero_allocation')
    elif size_score < sizing_threshold and entry_state == 'FULL_ENTRY':
        sizing_reasons.append('full_entry_sizing_below_minimum')
    sizing_action = 'PILOT_SIZE' if entry_state == 'PILOT_ENTRY' else 'FULL_SIZE' if entry_state == 'FULL_ENTRY' else 'NO_SIZE'
    sizing = RoleDecision('sizing', not sizing_reasons, round(float(size_score), 6), sizing_threshold, sizing_action if not sizing_reasons else 'BLOCK_SIZE', sizing_reasons, {'kelly_pos': kelly, 'state_machine_kelly': get('StateMachine_Kelly_Pos', 0.0), 'entry_state': entry_state})

    liq_floor = float(PARAMS.get('EXECUTION_MIN_LIQUIDITY_SCORE', 0.20))
    adv_floor = float(PARAMS.get('EXECUTION_MIN_ADV20', 5_000_000))
    execution_score = liquidity_score if liquidity_score > 0 else min(1.0, adv20 / max(adv_floor, 1.0)) if adv20 > 0 else 0.5
    execution_reasons: list[str] = []
    if liquidity_score > 0 and liquidity_score < liq_floor:
        execution_reasons.append('liquidity_score_below_execution_floor')
    if adv20 > 0 and adv20 < adv_floor:
        execution_reasons.append('adv20_below_execution_floor')
    if turnover > 0 and turnover < float(PARAMS.get('EXECUTION_TURNOVER_HARD_FLOOR', 0.15)):
        execution_reasons.append('turnover_below_execution_floor')
    execution = RoleDecision('execution', not execution_reasons, round(float(execution_score), 6), liq_floor, 'TWAP3_OR_LIMIT_EXECUTION' if not execution_reasons else 'BLOCK_EXECUTION', execution_reasons, {'liquidity_score': liquidity_score, 'adv20': adv20, 'turnover_ratio': turnover})

    bundle = ModelRoleBundle(entry, risk, sizing, execution)
    try:
        RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
        RUNTIME_PATH.write_text(json.dumps({'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), **bundle.as_dict()}, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        pass
    return bundle


if __name__ == '__main__':
    sample = {'Entry_State': 'PILOT_ENTRY', 'Kelly_Pos': 0.02, 'Liquidity_Score': 0.5}
    print(json.dumps(build_model_role_bundle(sample, proba=0.58, expected_return=0.01, signal_confidence=0.6).as_dict(), ensure_ascii=False, indent=2))
