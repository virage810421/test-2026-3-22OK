# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, write_json


class RecoveryValidationBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'recovery_validation.json'

    def build(self, retry_queue_summary: dict[str, Any], recovery_plan: dict[str, Any] | None = None) -> tuple[Any, dict[str, Any]]:
        state = load_json(PATHS.state_dir / 'engine_state.json', {}) or {}
        recovery_plan = recovery_plan or {}
        checks = []
        checks.append({'check': 'state_file_exists', 'value': bool(state), 'status': 'ok' if state else 'fail'})
        checks.append({'check': 'state_has_cash', 'value': state.get('cash', None), 'status': 'ok' if state and 'cash' in state else 'fail'})
        checks.append({'check': 'state_has_positions', 'value': len(state.get('positions', [])) if state else 0, 'status': 'ok' if state and 'positions' in state else 'fail'})
        checks.append({'check': 'state_has_open_orders', 'value': len(state.get('open_orders', [])) if state else 0, 'status': 'ok' if state and 'open_orders' in state else 'warn'})
        retry_total = int(retry_queue_summary.get('total', 0) or 0)
        checks.append({'check': 'retry_queue_total', 'value': retry_total, 'status': 'ok' if retry_total == 0 else 'warn'})
        if recovery_plan:
            checks.append({'check': 'recovery_plan_ready', 'value': recovery_plan.get('ready_to_recover', False), 'status': 'ok' if recovery_plan.get('ready_to_recover', False) else 'fail'})
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'checks': checks,
            'all_green': all(c['status'] == 'ok' for c in checks if c['check'] != 'state_has_open_orders'),
            'status': 'validation_ready' if all(c['status'] in {'ok', 'warn'} for c in checks) else 'validation_blocked',
        }
        write_json(self.path, payload)
        log(f'♻️ 已輸出 recovery validation：{self.path}')
        return self.path, payload
