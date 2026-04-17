# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json, normalize_key

_STATUS_ALIASES = {
    'NEW': 'NEW',
    'CREATED': 'NEW',
    'ACK': 'SUBMITTED',
    'SUBMITTED': 'SUBMITTED',
    'PENDING_SUBMIT': 'PENDING_SUBMIT',
    'PENDINGSUBMIT': 'PENDING_SUBMIT',
    'ACCEPTED': 'SUBMITTED',
    'WORKING': 'SUBMITTED',
    'PARTIAL': 'PARTIALLY_FILLED',
    'PARTIALLY_FILLED': 'PARTIALLY_FILLED',
    'PARTIALLYFILLED': 'PARTIALLY_FILLED',
    'FILL': 'FILLED',
    'FILLED': 'FILLED',
    'DONE': 'FILLED',
    'CANCELLED': 'CANCELLED',
    'CANCELED': 'CANCELLED',
    'PENDING_CANCEL': 'PENDING_CANCEL',
    'PENDINGCANCEL': 'PENDING_CANCEL',
    'REJECTED': 'REJECTED',
    'ERROR': 'REJECTED',
    'REPLACED': 'REPLACED',
    'REPLACE_PENDING': 'REPLACE_PENDING',
    'REPLACEPENDING': 'REPLACE_PENDING',
    'REPAIR_REVIEW': 'REPAIR_REVIEW',
    'REPAIR_PENDING': 'REPAIR_PENDING',
    'REPAIRED': 'REPAIRED',
}

_STATUS_RANK = {
    'UNKNOWN': 0,
    'NEW': 1,
    'PENDING_SUBMIT': 2,
    'SUBMITTED': 3,
    'REPLACE_PENDING': 4,
    'PARTIALLY_FILLED': 5,
    'PENDING_CANCEL': 6,
    'REPLACED': 7,
    'CANCELLED': 8,
    'REJECTED': 8,
    'FILLED': 9,
    'REPAIR_PENDING': 10,
    'REPAIR_REVIEW': 11,
    'REPAIRED': 12,
}

_TERMINAL_STATES = {'FILLED', 'CANCELLED', 'REJECTED', 'REPAIRED'}

_LEGAL_TRANSITIONS = {
    'UNKNOWN': {'NEW', 'PENDING_SUBMIT', 'SUBMITTED', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED'},
    'NEW': {'PENDING_SUBMIT', 'SUBMITTED', 'REJECTED', 'CANCELLED', 'PARTIALLY_FILLED', 'FILLED', 'REPLACE_PENDING'},
    'PENDING_SUBMIT': {'SUBMITTED', 'REJECTED', 'CANCELLED', 'PARTIALLY_FILLED', 'FILLED', 'REPLACE_PENDING'},
    'SUBMITTED': {'PARTIALLY_FILLED', 'FILLED', 'PENDING_CANCEL', 'CANCELLED', 'REJECTED', 'REPLACE_PENDING', 'REPLACED'},
    'REPLACE_PENDING': {'REPLACED', 'SUBMITTED', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED'},
    'REPLACED': {'SUBMITTED', 'PARTIALLY_FILLED', 'FILLED', 'PENDING_CANCEL', 'CANCELLED', 'REJECTED'},
    'PARTIALLY_FILLED': {'PARTIALLY_FILLED', 'FILLED', 'PENDING_CANCEL', 'CANCELLED', 'REJECTED', 'REPLACE_PENDING', 'REPLACED'},
    'PENDING_CANCEL': {'CANCELLED', 'PARTIALLY_FILLED', 'FILLED', 'REJECTED'},
    'CANCELLED': {'REPAIR_PENDING', 'REPAIR_REVIEW', 'REPAIRED'},
    'REJECTED': {'REPAIR_PENDING', 'REPAIR_REVIEW', 'REPAIRED'},
    'FILLED': {'REPAIR_PENDING', 'REPAIR_REVIEW', 'REPAIRED'},
    'REPAIR_PENDING': {'REPAIR_REVIEW', 'REPAIRED'},
    'REPAIR_REVIEW': {'REPAIRED'},
    'REPAIRED': set(),
}


def _normalize_status(value: Any) -> str:
    key = normalize_key(value)
    return _STATUS_ALIASES.get(key, key or 'UNKNOWN')


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _parse_ts(event: dict[str, Any]) -> str:
    for key in ('event_time', 'callback_time', 'updated_at', 'timestamp', 'recorded_at', 'broker_ts', 'ts'):
        value = event.get(key)
        if value:
            return str(value)
    return now_str()


def _ts_sort_value(ts: str) -> float:
    text = str(ts or '').strip()
    if not text:
        return 0.0
    for candidate in (text, text.replace('Z', '+00:00'), text.replace(' ', 'T')):
        try:
            return datetime.fromisoformat(candidate).timestamp()
        except Exception:
            continue
    return 0.0


class DirectionalExecutionStateMachine:
    def __init__(self):
        self.path = PATHS.state_dir / 'directional_execution_state_machine.json'

    def _empty_state(self) -> dict[str, Any]:
        return {
            'lanes': {'LONG': {}, 'SHORT': {}, 'RANGE': {}, 'UNKNOWN': {}},
            'history': [],
            'meta': {
                'module_version': 'v20260416_order_lifecycle_hardening',
                'legal_transitions_enabled': True,
                'duplicate_dedupe_enabled': True,
                'terminal_state_lock_enabled': True,
                'partial_fill_accumulation_enabled': True,
                'cancel_replace_chain_enabled': True,
                'out_of_order_guard_enabled': True,
            },
        }

    def _load(self) -> dict[str, Any]:
        state = load_json(self.path, default=self._empty_state()) or self._empty_state()
        state.setdefault('lanes', {'LONG': {}, 'SHORT': {}, 'RANGE': {}, 'UNKNOWN': {}})
        for lane in ('LONG', 'SHORT', 'RANGE', 'UNKNOWN'):
            state['lanes'].setdefault(lane, {})
        state.setdefault('history', [])
        state.setdefault('meta', self._empty_state()['meta'])
        return state

    def _key(self, event: dict[str, Any]) -> str:
        return str(event.get('client_order_id') or event.get('broker_order_id') or event.get('order_id') or '').strip()

    def _lane(self, event: dict[str, Any]) -> str:
        return normalize_key(event.get('direction_bucket') or event.get('approved_pool_type') or event.get('lane') or 'UNKNOWN') or 'UNKNOWN'

    def _event_fingerprint(self, event: dict[str, Any], status: str) -> str:
        parts = [
            str(event.get('event_type') or ''),
            str(status),
            str(event.get('filled_qty') or event.get('cum_filled_qty') or event.get('quantity_filled') or ''),
            str(event.get('remaining_qty') or event.get('leaves_qty') or ''),
            str(event.get('fill_id') or ''),
            _parse_ts(event),
        ]
        return '|'.join(parts)

    def _extract_event_seq(self, event: dict[str, Any]) -> tuple[int | None, float]:
        seq = None
        for key in ('event_seq', 'callback_seq', 'sequence', 'seq'):
            try:
                if event.get(key) not in (None, ''):
                    seq = int(float(event.get(key)))
                    break
            except Exception:
                continue
        ts = _ts_sort_value(_parse_ts(event))
        return seq, ts

    def _filled_qty(self, event: dict[str, Any], current: dict[str, Any]) -> float:
        cumulative = None
        for key in ('cum_filled_qty', 'filled_qty', 'quantity_filled', 'executed_qty', 'cum_qty'):
            if event.get(key) not in (None, ''):
                cumulative = _safe_float(event.get(key), 0.0)
                break
        last_fill_qty = None
        for key in ('last_fill_qty', 'fill_qty', 'last_qty'):
            if event.get(key) not in (None, ''):
                last_fill_qty = _safe_float(event.get(key), 0.0)
                break
        current_cum = _safe_float(current.get('cum_filled_qty'), 0.0)
        if cumulative is not None:
            return max(current_cum, cumulative)
        if last_fill_qty is not None:
            return max(current_cum, current_cum + max(0.0, last_fill_qty))
        if _normalize_status(event.get('status')) == 'FILLED':
            requested = _safe_float(event.get('qty') or event.get('quantity') or event.get('target_qty') or current.get('qty'), current_cum)
            return max(current_cum, requested)
        return current_cum

    def _is_duplicate(self, current: dict[str, Any], fingerprint: str) -> bool:
        seen = list(current.get('recent_event_fingerprints', []) or [])
        return fingerprint in seen

    def _remember_fingerprint(self, current: dict[str, Any], fingerprint: str) -> list[str]:
        seen = list(current.get('recent_event_fingerprints', []) or [])
        seen.append(fingerprint)
        return seen[-25:]

    def transition(self, event: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        state = self._load()
        key = self._key(event)
        if not key:
            write_json(self.path, state)
            payload = {'generated_at': now_str(), 'status': 'ignored_missing_order_key', 'event': event}
            return str(self.path), payload

        lane = self._lane(event)
        state['lanes'].setdefault(lane, {})
        current = dict(state['lanes'][lane].get(key, {}))
        current_status = _normalize_status(current.get('status'))
        incoming_status = _normalize_status(event.get('status'))
        next_status = incoming_status or current_status or 'UNKNOWN'
        event_seq, event_ts = self._extract_event_seq(event)
        last_seq = current.get('last_event_seq')
        last_ts = _safe_float(current.get('last_event_ts_sort'), 0.0)
        fingerprint = self._event_fingerprint(event, next_status)

        if self._is_duplicate(current, fingerprint):
            payload = {
                'generated_at': now_str(),
                'status': 'duplicate_event_ignored',
                'order_id': key,
                'lane': lane,
                'current_status': current_status or 'UNKNOWN',
            }
            return str(self.path), payload

        if current_status in _TERMINAL_STATES and next_status not in _LEGAL_TRANSITIONS.get(current_status, set()) and next_status != current_status:
            payload = {
                'generated_at': now_str(),
                'status': 'terminal_state_locked',
                'order_id': key,
                'lane': lane,
                'current_status': current_status,
                'incoming_status': next_status,
            }
            return str(self.path), payload

        out_of_order = False
        if event_seq is not None and last_seq is not None and event_seq < last_seq:
            out_of_order = True
        elif event_seq is None and last_ts > 0 and event_ts > 0 and event_ts + 1e-9 < last_ts:
            out_of_order = True

        proposed_cum = self._filled_qty(event, current)
        current_cum = _safe_float(current.get('cum_filled_qty'), 0.0)
        if out_of_order and proposed_cum <= current_cum and _STATUS_RANK.get(next_status, 0) <= _STATUS_RANK.get(current_status or 'UNKNOWN', 0):
            payload = {
                'generated_at': now_str(),
                'status': 'out_of_order_event_ignored',
                'order_id': key,
                'lane': lane,
                'current_status': current_status or 'UNKNOWN',
                'incoming_status': next_status,
            }
            return str(self.path), payload

        legal_next = _LEGAL_TRANSITIONS.get(current_status or 'UNKNOWN', _LEGAL_TRANSITIONS['UNKNOWN'])
        illegal_transition = bool(current_status and next_status != current_status and next_status not in legal_next)
        if illegal_transition and not (next_status == 'PARTIALLY_FILLED' and current_status in {'SUBMITTED', 'REPLACE_PENDING'}):
            payload = {
                'generated_at': now_str(),
                'status': 'illegal_transition_ignored',
                'order_id': key,
                'lane': lane,
                'current_status': current_status,
                'incoming_status': next_status,
            }
            return str(self.path), payload

        replace_parent = str(event.get('replace_of') or event.get('replaces_order_id') or event.get('parent_order_id') or current.get('replace_parent_order_id') or '').strip()
        replace_child = str(event.get('replacement_order_id') or event.get('new_order_id') or '').strip()

        entry = {
            'order_id': key,
            'lane': lane,
            'status': next_status,
            'symbol': event.get('symbol', current.get('symbol', '')),
            'strategy_bucket': event.get('strategy_bucket', current.get('strategy_bucket', '')),
            'approved_pool_type': event.get('approved_pool_type', current.get('approved_pool_type', '')),
            'model_scope': event.get('model_scope', current.get('model_scope', '')),
            'range_confidence': event.get('range_confidence', current.get('range_confidence', 0.0)),
            'broker_order_id': str(event.get('broker_order_id') or current.get('broker_order_id') or ''),
            'client_order_id': str(event.get('client_order_id') or current.get('client_order_id') or ''),
            'qty': _safe_float(event.get('qty') or event.get('quantity') or event.get('target_qty') or current.get('qty'), _safe_float(current.get('qty'), 0.0)),
            'cum_filled_qty': proposed_cum,
            'last_fill_qty': _safe_float(event.get('last_fill_qty') or event.get('fill_qty') or event.get('last_qty'), _safe_float(current.get('last_fill_qty'), 0.0)),
            'remaining_qty': max(0.0, _safe_float(event.get('remaining_qty') or event.get('leaves_qty'), max(0.0, _safe_float(current.get('qty'), 0.0) - proposed_cum))),
            'replace_parent_order_id': replace_parent,
            'replacement_order_id': replace_child or current.get('replacement_order_id', ''),
            'updated_at': now_str(),
            'last_event_type': str(event.get('event_type', current.get('last_event_type', ''))),
            'last_event_time': _parse_ts(event),
            'last_event_seq': event_seq if event_seq is not None else current.get('last_event_seq'),
            'last_event_ts_sort': max(last_ts, event_ts),
            'recent_event_fingerprints': self._remember_fingerprint(current, fingerprint),
            'out_of_order_event_count': int(current.get('out_of_order_event_count', 0) or 0) + (1 if out_of_order else 0),
            'duplicate_event_count': int(current.get('duplicate_event_count', 0) or 0),
            'transition_count': int(current.get('transition_count', 0) or 0) + 1,
            'terminal_locked': next_status in _TERMINAL_STATES,
        }
        state['lanes'][lane][key] = {**current, **entry}
        state['history'].append({
            'at': now_str(),
            'order_id': key,
            'lane': lane,
            'status': next_status,
            'event_type': event.get('event_type', ''),
            'out_of_order': out_of_order,
            'cum_filled_qty': proposed_cum,
            'replace_parent_order_id': replace_parent,
            'replacement_order_id': replace_child,
        })
        state['history'] = state['history'][-2000:]
        write_json(self.path, state)
        payload = {
            'generated_at': now_str(),
            'status': 'state_transition_recorded',
            'order_id': key,
            'lane': lane,
            'previous_status': current_status or 'UNKNOWN',
            'current_status': next_status,
            'cum_filled_qty': proposed_cum,
            'out_of_order': out_of_order,
            'lane_order_count': len(state['lanes'][lane]),
        }
        return str(self.path), payload

    def force_repair(self, lane: str, order_id: str, target_status: str, reason: str = '', step: str = '') -> tuple[str, dict[str, Any]]:
        state = self._load()
        lane = normalize_key(lane) or 'UNKNOWN'
        state['lanes'].setdefault(lane, {})
        current = dict(state['lanes'][lane].get(order_id, {'order_id': order_id, 'lane': lane}))
        current['status'] = _normalize_status(target_status) or current.get('status', 'UNKNOWN')
        current['repair_reason'] = reason
        current['repair_step'] = step
        current['updated_at'] = now_str()
        current['terminal_locked'] = current['status'] in _TERMINAL_STATES
        current['transition_count'] = int(current.get('transition_count', 0) or 0) + 1
        state['lanes'][lane][order_id] = current
        state['history'].append({'at': now_str(), 'order_id': order_id, 'lane': lane, 'status': current['status'], 'event_type': 'REPAIR_FORCE', 'repair_step': step, 'reason': reason})
        state['history'] = state['history'][-2000:]
        write_json(self.path, state)
        payload = {'generated_at': now_str(), 'status': 'repair_forced', 'lane': lane, 'order_id': order_id, 'target_status': current['status'], 'repair_step': step}
        return str(self.path), payload
