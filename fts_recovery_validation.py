# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class RecoveryValidationBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "recovery_validation.json"

    def build(self, retry_queue_summary: dict):
        state_path = PATHS.state_dir / "engine_state.json"
        state_exists = state_path.exists()
        retry_total = retry_queue_summary.get("total", 0)

        checks = [
            {"check": "state_file_exists", "value": state_exists, "status": "ok" if state_exists else "warn"},
            {"check": "retry_queue_total", "value": retry_total, "status": "ok" if retry_total == 0 else "warn"},
        ]

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "checks": checks,
            "all_green": all(c["status"] == "ok" for c in checks),
            "status": "validation_skeleton"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"♻️ 已輸出 recovery validation：{self.path}")
        return self.path, payload
