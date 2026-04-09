# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class StartupRepairPlanner:
    def __init__(self):
        self.path = PATHS.runtime_dir / "startup_repair_plan.json"

    def build(self, recovery_report: dict):
        actions = []

        if not recovery_report.get("checks", {}).get("state_file_exists", False):
            actions.append({
                "priority": "medium",
                "action": "rebuild_empty_state",
                "message": "建立乾淨的初始 state 骨架"
            })

        retry_total = recovery_report.get("checks", {}).get("retry_queue_total", 0)
        if retry_total > 0:
            actions.append({
                "priority": "high",
                "action": "review_retry_queue",
                "message": f"檢查 retry queue，共 {retry_total} 筆"
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "planned_actions": actions,
            "action_count": len(actions),
            "status": "planner_only_not_auto_repair"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛠️ 已輸出 startup repair plan：{self.path}")
        return self.path, payload
