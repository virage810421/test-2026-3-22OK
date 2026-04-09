# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class StartupRepairPlanner:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'startup_repair_plan.json'

    def build(self, recovery_report: dict[str, Any]):
        actions = []
        checks = recovery_report.get('checks', {}) or {}
        summary = recovery_report.get('summary', {}) or {}

        if not checks.get('state_file_exists', False):
            actions.append({'priority': 'medium', 'action': 'rebuild_empty_state', 'message': '建立乾淨的初始 state 骨架'})

        retry_total = int(checks.get('retry_queue_total', 0) or 0)
        if retry_total > 0:
            actions.append({'priority': 'high', 'action': 'review_retry_queue', 'message': f'檢查 retry queue，共 {retry_total} 筆'})

        if summary.get('corporate_action_suspect_count', 0) > 0:
            actions.append({'priority': 'high', 'action': 'apply_corporate_action_position_rebuild', 'message': '疑似除權息/減資/分割，重建持倉與成本'})

        if summary.get('position_mismatch_count', 0) > 0:
            actions.append({'priority': 'high', 'action': 'rebuild_positions_from_fills', 'message': '依成交紀錄重建持倉快照'})

        if not recovery_report.get('cash_check', {}).get('matched', True):
            actions.append({'priority': 'high', 'action': 'replay_cash_ledger', 'message': '重播現金帳與手續費稅額'})

        for name in recovery_report.get('repair_actions', []) or []:
            if not any(a['action'] == name for a in actions):
                actions.append({'priority': 'medium', 'action': name, 'message': f'建議執行 {name}'})

        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'planned_actions': actions,
            'action_count': len(actions),
            'status': 'planner_ready_for_manual_orchestrated_repair',
        }
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f'🛠️ 已輸出 startup repair plan：{self.path}')
        return self.path, payload
