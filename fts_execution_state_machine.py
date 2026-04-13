# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json, normalize_key

STATUS_FLOW = {
    'NEW': 'NEW',
    'ACK': 'SUBMITTED',
    'SUBMITTED': 'SUBMITTED',
    'ACCEPTED': 'SUBMITTED',
    'PARTIAL': 'PARTIALLY_FILLED',
    'PARTIALLY_FILLED': 'PARTIALLY_FILLED',
    'FILLED': 'FILLED',
    'DONE': 'FILLED',
    'CANCELLED': 'CANCELLED',
    'CANCELED': 'CANCELLED',
    'REJECTED': 'REJECTED',
    'ERROR': 'REJECTED',
    'REPAIR_REVIEW': 'REPAIR_REVIEW',
    'REPAIR_PENDING': 'REPAIR_PENDING',
    'REPAIRED': 'REPAIRED',
}


class DirectionalExecutionStateMachine:
    def __init__(self):
        self.path = PATHS.state_dir / 'directional_execution_state_machine.json'

    def _load(self) -> dict[str, Any]:
        return load_json(self.path, default={'lanes': {'LONG': {}, 'SHORT': {}, 'RANGE': {}, 'UNKNOWN': {}}, 'history': []}) or {'lanes': {'LONG': {}, 'SHORT': {}, 'RANGE': {}, 'UNKNOWN': {}}, 'history': []}

    def _key(self, event: dict[str, Any]) -> str:
        return str(event.get('client_order_id') or event.get('broker_order_id') or event.get('order_id') or '').strip()

    def _lane(self, event: dict[str, Any]) -> str:
        return normalize_key(event.get('direction_bucket') or event.get('approved_pool_type') or event.get('lane') or 'UNKNOWN') or 'UNKNOWN'

    def transition(self, event: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        state = self._load()
        key = self._key(event)
        if not key:
            write_json(self.path, state)
            payload = {'generated_at': now_str(), 'status': 'ignored_missing_order_key', 'event': event}
            return str(self.path), payload
        lane = self._lane(event)
        state['lanes'].setdefault(lane, {})
        current = state['lanes'][lane].get(key, {})
        incoming = normalize_key(event.get('status'))
        status = STATUS_FLOW.get(incoming, incoming or current.get('status', 'UNKNOWN'))
        entry = {
            'order_id': key,
            'lane': lane,
            'status': status,
            'symbol': event.get('symbol', ''),
            'strategy_bucket': event.get('strategy_bucket', ''),
            'approved_pool_type': event.get('approved_pool_type', ''),
            'model_scope': event.get('model_scope', ''),
            'range_confidence': event.get('range_confidence', 0.0),
            'updated_at': now_str(),
        }
        state['lanes'][lane][key] = {**current, **entry}
        state['history'].append({'at': now_str(), 'order_id': key, 'lane': lane, 'status': status, 'event_type': event.get('event_type', '')})
        write_json(self.path, state)
        payload = {'generated_at': now_str(), 'status': 'state_transition_recorded', 'order_id': key, 'lane': lane, 'current_status': status, 'lane_order_count': len(state['lanes'][lane])}
        return str(self.path), payload

    def force_repair(self, lane: str, order_id: str, target_status: str, reason: str = '', step: str = '') -> tuple[str, dict[str, Any]]:
        state = self._load()
        lane = normalize_key(lane) or 'UNKNOWN'
        state['lanes'].setdefault(lane, {})
        current = state['lanes'][lane].get(order_id, {'order_id': order_id, 'lane': lane})
        current['status'] = STATUS_FLOW.get(normalize_key(target_status), normalize_key(target_status) or current.get('status', 'UNKNOWN'))
        current['repair_reason'] = reason
        current['repair_step'] = step
        current['updated_at'] = now_str()
        state['lanes'][lane][order_id] = current
        state['history'].append({'at': now_str(), 'order_id': order_id, 'lane': lane, 'status': current['status'], 'event_type': 'REPAIR_FORCE', 'repair_step': step, 'reason': reason})
        write_json(self.path, state)
        payload = {'generated_at': now_str(), 'status': 'repair_forced', 'lane': lane, 'order_id': order_id, 'target_status': current['status'], 'repair_step': step}
        return str(self.path), payload
