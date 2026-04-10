# -*- coding: utf-8 -*-
"""Level-3 safe runtime loader for execution / broker / risk integration."""
from __future__ import annotations

import importlib
from typing import Any

_SERVICE_MAP = {
    'DecisionExecutionBridge': ('fts_decision_execution_bridge', 'DecisionExecutionBridge'),
    'LiveReadinessGate': ('fts_live_readiness_gate', 'LiveReadinessGate'),
    'OrderStateMachine': ('fts_order_state_machine', 'OrderStateMachine'),
    'PositionStateService': ('fts_position_state_service', 'PositionStateService'),
    'ReconciliationEngine': ('fts_reconciliation_engine', 'ReconciliationEngine'),
    'KillSwitchManager': ('fts_kill_switch', 'KillSwitchManager'),
    'RecoveryEngine': ('fts_recovery_engine', 'RecoveryEngine'),
}


def _load_class(module_name: str, attr_name: str):
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def get_level3_classes() -> dict[str, Any]:
    loaded = {}
    for public_name, (module_name, attr_name) in _SERVICE_MAP.items():
        try:
            loaded[public_name] = _load_class(module_name, attr_name)
        except Exception:
            loaded[public_name] = None
    return loaded


def build_level3_services() -> tuple[dict[str, Any], dict[str, Any]]:
    classes = get_level3_classes()
    services: dict[str, Any] = {}
    meta: dict[str, Any] = {'services': {}, 'status': 'level3_partial_ready'}
    ok_count = 0
    for name, cls in classes.items():
        if cls is None:
            meta['services'][name] = {'loaded': False}
            continue
        try:
            services[name] = cls()
            meta['services'][name] = {'loaded': True}
            ok_count += 1
        except Exception as e:
            meta['services'][name] = {'loaded': False, 'error': repr(e)}
    if ok_count == len(classes):
        meta['status'] = 'level3_ready'
    elif ok_count == 0:
        meta['status'] = 'level3_unavailable'
    return services, meta
