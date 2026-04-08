# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class RecoveryConsistencySuite:
    def __init__(self):
        self.path = PATHS.runtime_dir / "recovery_consistency_report.json"

    def build(self, retry_queue_summary: dict):
        state_path = PATHS.state_dir / "engine_state.json"
        has_state = state_path.exists()
        retry_total = retry_queue_summary.get("total", 0)

        failures = []
        warnings = []

        if not has_state:
            warnings.append({
                "type": "missing_state_file",
                "message": "尚未找到 state/engine_state.json"
            })

        if retry_total > 0:
            warnings.append({
                "type": "pending_retry_queue",
                "message": f"retry queue 目前仍有 {retry_total} 筆待處理/已記錄項目"
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "checks": {
                "state_file_exists": has_state,
                "retry_queue_total": retry_total,
            },
            "failures": failures,
            "warnings": warnings,
            "all_passed": len(failures) == 0,
            "status": "consistency_suite_defined"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧩 已輸出 recovery consistency report：{self.path}")
        return self.path, payload
