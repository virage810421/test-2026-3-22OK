# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_utils import safe_float

try:
    from config import PARAMS
except Exception:
    PARAMS = {}


def _infer_direction(row: dict[str, Any]) -> str:
    text = ' '.join(str(row.get(k, '')) for k in ['Direction', 'Golden_Type', 'Structure', 'Regime']).upper()
    if 'SHORT' in text or '空' in text:
        return 'SHORT'
    if 'RANGE' in text or '區間' in text:
        return 'RANGE'
    return 'LONG'


def evaluate_signal_gate(row: dict[str, Any], trigger_score: float = 2.0) -> dict[str, Any]:
    kelly_pct = safe_float(row.get('Kelly_Pos', 0.0), 0.0)
    weighted_buy = safe_float(row.get('Weighted_Buy_Score', 0.0), 0.0)
    weighted_sell = safe_float(row.get('Weighted_Sell_Score', 0.0), 0.0)
    score_gap = safe_float(row.get('Score_Gap', 0.0), 0.0)
    ai_proba = safe_float(row.get('AI_Proba', 0.0), 0.0)
    expected_return = safe_float(row.get('Expected_Return', row.get('Heuristic_EV', row.get('Live_EV', row.get('Realized_EV', 0.0)))), 0.0)
    ev_source = str(row.get('EV_Source', 'unknown'))
    ev_sample_size = int(safe_float(row.get('歷史訊號樣本數', row.get('Sample_Size', 0)), 0.0))
    health = str(row.get('Health', 'KEEP')).upper()
    direction = _infer_direction(row)
    fallback_build = bool(row.get('FallbackBuild', False))
    desk_usable = bool(row.get('DeskUsable', False if bool(PARAMS.get('SIGNAL_PATH_FAIL_CLOSED', True)) else True))
    execution_eligible = bool(row.get('ExecutionEligible', False if bool(PARAMS.get('SIGNAL_PATH_FAIL_CLOSED', True)) else True))
    entry_readiness = safe_float(row.get('Entry_Readiness', 0.0), 0.0)
    breakout_risk = safe_float(row.get('Breakout_Risk_Next3', 0.0), 0.0)
    reversal_risk = safe_float(row.get('Reversal_Risk_Next3', 0.0), 0.0)
    exit_hazard = safe_float(row.get('Exit_Hazard_Score', 0.0), 0.0)
    transition_label = str(row.get('Transition_Label', ''))
    hysteresis_label = str(row.get('Hysteresis_Regime_Label', row.get('Regime_Label', row.get('Regime', ''))))
    hysteresis_armed = safe_float(row.get('Hysteresis_Switch_Armed', 0.0), 0.0)
    hysteresis_locked = safe_float(row.get('Hysteresis_Locked', 0.0), 0.0)
    entry_state = str(row.get('Entry_State', 'NO_ENTRY')).upper()
    early_state = str(row.get('Early_Path_State', entry_state)).upper()
    confirm_state = str(row.get('Confirm_Path_State', 'WAIT_CONFIRM')).upper()
    entry_path = str(row.get('Entry_Path', 'NONE')).upper()
    preentry_score = safe_float(row.get('PreEntry_Score', 0.0), 0.0)
    confirm_score = safe_float(row.get('Confirm_Entry_Score', 0.0), 0.0)
    legacy_long_pressure = safe_float(row.get('Legacy_Long_Confirm_Pressure', 0.0), 0.0)
    legacy_short_pressure = safe_float(row.get('Legacy_Short_Confirm_Pressure', 0.0), 0.0)

    blockers: list[str] = []
    warnings: list[str] = []
    diagnostics: list[str] = []

    if kelly_pct <= 0:
        blockers.append('kelly_zero')
    if health in {'KILL', 'BLOCKED'}:
        blockers.append('health_blocked')
    if health in {'REVIEW_REQUIRED', 'FALLBACK_BUILD'}:
        blockers.append('manual_review_required')
    if fallback_build:
        blockers.append('fallback_build_unusable')
    if not desk_usable:
        blockers.append('decision_desk_unusable')
    if not execution_eligible:
        blockers.append('execution_not_eligible')
    live_min_ev = float(PARAMS.get('LIVE_MIN_EXPECTED_RETURN', -0.0015))
    ev_min_sample = int(PARAMS.get('LIVE_EV_MIN_SAMPLE_FOR_HARD_BLOCK', PARAMS.get('MIN_SIGNAL_SAMPLE_SIZE', 8)))
    if expected_return < live_min_ev and ev_sample_size >= ev_min_sample:
        blockers.append(f'expected_return_below_threshold:{expected_return:.4f}<{live_min_ev:.4f}')
    elif expected_return < live_min_ev:
        warnings.append(f'expected_return_soft_negative:{expected_return:.4f}')
    pilot_proba_floor = max(0.45, float(PARAMS.get('LONG_MIN_PROBA', 0.52)) - float(PARAMS.get('PILOT_MIN_PROBA_BUFFER', 0.04)))
    if entry_state == 'NO_ENTRY':
        blockers.append('state_machine_no_entry')
    elif entry_state == 'PREPARE':
        blockers.append('state_machine_watch_only')
    elif entry_state == 'PILOT_ENTRY':
        if ai_proba < pilot_proba_floor:
            blockers.append('pilot_ai_probability_below_threshold')
    elif ai_proba < 0.50:
        blockers.append('ai_probability_below_threshold')
    if entry_readiness < 0.10:
        warnings.append('entry_readiness_low')
    if breakout_risk >= 0.80:
        warnings.append('breakout_risk_high')
    if reversal_risk >= 0.80 or exit_hazard >= 0.80:
        warnings.append('reversal_or_exit_hazard_high')
    if transition_label and transition_label != 'Stable':
        diagnostics.append(f'transition={transition_label}')
    if hysteresis_label:
        diagnostics.append(f'hysteresis_regime={hysteresis_label}')
    if hysteresis_armed >= 0.5:
        warnings.append('hysteresis_switch_armed')
    if hysteresis_locked >= 0.5:
        diagnostics.append('hysteresis_locked')

    # 舊 c2-c9 / weighted score 現在只保留做確認診斷，不再主導提前布局。
    diagnostics.extend([
        f'entry_state={entry_state}',
        f'early_state={early_state}',
        f'confirm_state={confirm_state}',
        f'entry_path={entry_path}',
        f'preentry_score={preentry_score:.3f}',
        f'confirm_score={confirm_score:.3f}',
        f'expected_return={expected_return:.4f}',
        f'ev_source={ev_source}',
        f'ev_sample_size={ev_sample_size}',
        f'score_gap={score_gap:.3f}',
    ])
    if direction == 'LONG':
        diagnostics.append(f'legacy_long_confirm_pressure={legacy_long_pressure:.3f}')
        if weighted_sell > weighted_buy:
            warnings.append('legacy_long_confirmation_weaker_than_sell_pressure')
    elif direction == 'SHORT':
        diagnostics.append(f'legacy_short_confirm_pressure={legacy_short_pressure:.3f}')
        if weighted_buy > weighted_sell:
            warnings.append('legacy_short_confirmation_weaker_than_buy_pressure')
    else:
        diagnostics.append(f'range_score_gap={score_gap:.3f}')
        if abs(score_gap) > max(trigger_score, 2.0) and ai_proba < 0.55:
            warnings.append('range_setup_score_tilted_to_trend')

    passed = len(blockers) == 0
    note_parts = []
    if blockers:
        note_parts.append('BLOCK:' + ','.join(blockers))
    if warnings:
        note_parts.append('WARN:' + ','.join(warnings))
    if diagnostics:
        note_parts.append('DIAG:' + ','.join(diagnostics))

    return {
        'passed': passed,
        'direction': direction,
        'blockers': blockers,
        'warnings': warnings,
        'diagnostics': diagnostics,
        'note': ' | '.join(note_parts) if note_parts else '通過訊號閘門',
        'core_driver': 'model_ev_kelly_first',
        'heuristic_role': 'diagnostic_only',
    }


def passes_signal_gate(row: dict[str, Any], trigger_score: float = 2.0) -> tuple[bool, str]:
    result = evaluate_signal_gate(row, trigger_score=trigger_score)
    return bool(result['passed']), str(result['note'])
