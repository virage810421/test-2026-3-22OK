# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ReconciliationEngineSkeleton:
    def __init__(self):
        self.path = PATHS.runtime_dir / "reconciliation_engine.json"

    def build(self, execution_result: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "checks": {
                "submitted_vs_filled_consistent": execution_result.get("submitted", 0) >= execution_result.get("filled", 0),
                "submitted_vs_partial_consistent": execution_result.get("submitted", 0) >= execution_result.get("partially_filled", 0),
                "submitted_vs_rejected_consistent": execution_result.get("submitted", 0) >= execution_result.get("rejected", 0),
            },
            "execution_result": execution_result,
            "status": "reconciliation_skeleton"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧮 已輸出 reconciliation engine：{self.path}")
        return self.path, payload
