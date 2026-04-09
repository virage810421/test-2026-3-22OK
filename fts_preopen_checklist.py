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
        checklist = [
            {'item': '模型 promotion 是否放行', 'ok': governance.get('promote_candidate', False)},
            {'item': 'live safety 是否通過', 'ok': safety.get('paper_live_safe', False)},
            {'item': '前次對帳是否全綠', 'ok': recon.get('all_green', False)},
            {'item': 'recovery validation 是否通過', 'ok': recovery.get('ready_for_resume', False)},
        ]
        payload = {
            'generated_at': now_str(),
            'status': 'preopen_checklist_built',
            'all_green': all(x['ok'] for x in checklist),
            'items': checklist,
        }
        path = PATHS.runtime_dir / 'preopen_checklist.json'
        write_json(path, payload)
        return str(path), payload
