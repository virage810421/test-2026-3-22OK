# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_utils import safe_float
from fts_execution_layer import signal_gate as _canonical_signal_gate

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
    direction = _infer_direction(row)
    canonical = _canonical_signal_gate(row, model_decision=None, params=PARAMS)
    blockers = list(getattr(canonical, 'reasons', []) or [])
    warnings: list[str] = []
    diagnostics = list(row.get('_signal_gate_diagnostics', []) or [])

    expected_return = max(
        safe_float(row.get('Expected_Return', row.get('Heuristic_EV', row.get('Live_EV', row.get('Realized_EV', 0.0)))), 0.0),
        safe_float(row.get('Strategy_EV_SQL', row.get('Realized_EV', 0.0)), 0.0),
    )
    ev_source = str(row.get('EV_Source', 'unknown'))
    ev_sample_size = int(safe_float(row.get('歷史訊號樣本數', row.get('Sample_Size', 0)), 0.0))
    health = str(row.get('Health', 'KEEP')).upper()
    fallback_build = bool(row.get('FallbackBuild', False))
    desk_usable = bool(row.get('DeskUsable', False if bool(PARAMS.get('SIGNAL_PATH_FAIL_CLOSED', True)) else True))
    execution_eligible = bool(row.get('ExecutionEligible', False if bool(PARAMS.get('SIGNAL_PATH_FAIL_CLOSED', True)) else True))
    transition_label = str(row.get('Transition_Label', ''))
    hysteresis_label = str(row.get('Hysteresis_Regime_Label', row.get('Regime_Label', row.get('Regime', ''))))
    hysteresis_armed = safe_float(row.get('Hysteresis_Switch_Armed', 0.0), 0.0)
    hysteresis_locked = safe_float(row.get('Hysteresis_Locked', 0.0), 0.0)
    entry_readiness = safe_float(row.get('Entry_Readiness', 0.0), 0.0)
    breakout_risk = safe_float(row.get('Breakout_Risk_Next3', 0.0), 0.0)
    reversal_risk = safe_float(row.get('Reversal_Risk_Next3', 0.0), 0.0)
    exit_hazard = safe_float(row.get('Exit_Hazard_Score', 0.0), 0.0)
    weighted_buy = safe_float(row.get('Weighted_Buy_Score', 0.0), 0.0)
    weighted_sell = safe_float(row.get('Weighted_Sell_Score', 0.0), 0.0)
    score_gap = safe_float(row.get('Score_Gap', 0.0), 0.0)

    if health in {'BLOCKED'} and 'health_blocked' not in blockers:
        blockers.append('health_blocked')
    if health in {'REVIEW_REQUIRED', 'FALLBACK_BUILD'} and 'manual_review_required' not in blockers:
        blockers.append('manual_review_required')
    if fallback_build and 'fallback_build_unusable' not in blockers:
        blockers.append('fallback_build_unusable')
    if not desk_usable and 'decision_desk_unusable' not in blockers:
        blockers.append('decision_desk_unusable')
    entry_state = str(row.get('Entry_State', 'NO_ENTRY')).upper()
    action = str(row.get('Action', 'HOLD')).upper()
    provisional_executable = action in {'BUY', 'SHORT'} and entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'} and bool(PARAMS.get('FALLBACK_DECISION_ALLOW_PAPER_EXECUTION', True))
    if not execution_eligible and 'execution_not_eligible' not in blockers:
        if provisional_executable:
            warnings.append('execution_not_eligible_yet')
        else:
            blockers.append('execution_not_eligible')

    live_min_ev = float(PARAMS.get('LIVE_MIN_EXPECTED_RETURN', -0.0015))
    ev_min_sample = int(PARAMS.get('LIVE_EV_MIN_SAMPLE_FOR_HARD_BLOCK', PARAMS.get('MIN_SIGNAL_SAMPLE_SIZE', 8)))
    if expected_return < live_min_ev and ev_sample_size >= ev_min_sample:
        blocker = f'expected_return_below_threshold:{expected_return:.4f}<{live_min_ev:.4f}'
        if blocker not in blockers:
            blockers.append(blocker)
    elif expected_return < live_min_ev:
        warnings.append(f'expected_return_soft_negative:{expected_return:.4f}')

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

    diagnostics.extend([
        f'expected_return={expected_return:.4f}',
        f'ev_source={ev_source}',
        f'ev_sample_size={ev_sample_size}',
        f'score_gap={score_gap:.3f}',
    ])
    if direction == 'LONG' and weighted_sell > weighted_buy:
        warnings.append('legacy_long_confirmation_weaker_than_sell_pressure')
    elif direction == 'SHORT' and weighted_buy > weighted_sell:
        warnings.append('legacy_short_confirmation_weaker_than_buy_pressure')
    elif direction == 'RANGE' and abs(score_gap) > max(trigger_score, 2.0):
        warnings.append('range_setup_score_tilted_to_trend')

    # de-duplicate while preserving order
    blockers = list(dict.fromkeys([str(x) for x in blockers if str(x)]))
    warnings = list(dict.fromkeys([str(x) for x in warnings if str(x)]))
    diagnostics = list(dict.fromkeys([str(x) for x in diagnostics if str(x)]))
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
        'core_driver': 'fts_execution_layer.signal_gate',
        'heuristic_role': 'diagnostic_only',
    }


def passes_signal_gate(row: dict[str, Any], trigger_score: float = 2.0) -> tuple[bool, str]:
    result = evaluate_signal_gate(row, trigger_score=trigger_score)
    return bool(result['passed']), str(result['note'])
