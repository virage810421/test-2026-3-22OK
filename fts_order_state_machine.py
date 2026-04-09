# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, write_json, normalize_key

_ALLOWED = {
    'NEW': {'PENDING_SUBMIT', 'REJECTED', 'CANCELLED'},
    'PENDING_SUBMIT': {'SUBMITTED', 'REJECTED', 'CANCELLED'},
    'SUBMITTED': {'PARTIALLY_FILLED', 'FILLED', 'CANCEL_PENDING', 'CANCELLED', 'REJECTED'},
    'PARTIALLY_FILLED': {'PARTIALLY_FILLED', 'FILLED', 'CANCEL_PENDING', 'CANCELLED'},
    'CANCEL_PENDING': {'CANCELLED', 'PARTIALLY_FILLED', 'FILLED'},
    'FILLED': set(),
    'CANCELLED': set(),
    'REJECTED': set(),
}


class OrderStateMachine:
    def transition(self, current: str, target: str) -> dict[str, Any]:
        current = normalize_key(current) or 'NEW'
        target = normalize_key(target)
        ok = target in _ALLOWED.get(current, set()) or current == target
        return {
            'from': current,
            'to': target,
            'allowed': ok,
            'reason': 'ok' if ok else 'illegal_transition',
        }

    def build_definition(self) -> tuple[str, dict[str, Any]]:
        payload = {
            'generated_at': now_str(),
            'status': 'order_state_machine_defined',
            'states': sorted(_ALLOWED.keys()),
            'allowed_transitions': {k: sorted(v) for k, v in _ALLOWED.items()},
        }
        path = PATHS.runtime_dir / 'order_state_machine_definition.json'
        write_json(path, payload)
        return str(path), payload
