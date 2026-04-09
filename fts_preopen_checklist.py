# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json


class PreOpenChecklistBuilder:
    def build(self) -> tuple[str, dict[str, Any]]:
        governance = load_json(PATHS.runtime_dir / 'trainer_promotion_decision.json', {}) or {}
        safety = load_json(PATHS.runtime_dir / 'live_safety_gate.json', {}) or {}
        recon = load_json(PATHS.runtime_dir / 'reconciliation_engine.json', {}) or {}
        recovery = load_json(PATHS.runtime_dir / 'recovery_validation.json', {}) or {}
        training = load_json(PATHS.runtime_dir / 'training_orchestrator.json', {}) or {}
        execution = load_json(PATHS.runtime_dir / 'decision_execution_bridge.json', {}) or {}
        incident = load_json(PATHS.runtime_dir / 'intraday_incident_guard.json', {}) or {}
        kill_state = load_json(PATHS.runtime_dir / 'kill_switch_state.json', {}) or {}

        recon_green = bool(recon.get('all_green', recon.get('summary', {}).get('all_green', False)))
        recovery_ready = bool(recovery.get('ready_for_resume', recovery.get('all_green', False)))
        execution_ready = str(execution.get('status', '')).startswith(('execution_payload_ready', 'partial_execution_ready'))
        checklist = [
            {'item': '模型 promotion / governance', 'ok': governance.get('go_for_shadow', governance.get('go_for_promote', False)), 'detail': governance.get('status', 'missing')},
            {'item': 'training orchestrator 已建置', 'ok': bool(training), 'detail': training.get('status', 'missing')},
            {'item': 'execution payload 已產出', 'ok': execution_ready, 'detail': execution.get('status', 'missing')},
            {'item': 'live safety 通過', 'ok': safety.get('paper_live_safe', False), 'detail': safety.get('status', 'missing')},
            {'item': '前次對帳全綠', 'ok': recon_green, 'detail': recon.get('status', 'missing')},
            {'item': 'recovery validation 通過', 'ok': recovery_ready, 'detail': recovery.get('status', 'missing')},
            {'item': 'incident guard 無封鎖', 'ok': incident.get('status', '') != 'incident_guard_block', 'detail': incident.get('status', 'missing')},
            {'item': 'kill switch 未觸發', 'ok': not bool(kill_state.get('system', {}).get('armed')), 'detail': kill_state.get('system', {}).get('reason', '')},
        ]
        green_count = sum(1 for x in checklist if x['ok'])
        payload = {
            'generated_at': now_str(),
            'status': 'preopen_green' if green_count == len(checklist) else 'preopen_partial',
            'all_green': green_count == len(checklist),
            'green_count': green_count,
            'total_count': len(checklist),
            'readiness_pct': int(round(green_count / max(len(checklist), 1) * 100, 0)),
            'items': checklist,
            'next_blockers': [x['item'] for x in checklist if not x['ok']],
        }
        path = PATHS.runtime_dir / 'preopen_checklist.json'
        write_json(path, payload)
        return str(path), payload
