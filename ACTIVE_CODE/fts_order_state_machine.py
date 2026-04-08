# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class OrderStateMachineRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "order_state_machine.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "states": [
                "NEW",
                "PENDING_SUBMIT",
                "SUBMITTED",
                "PARTIALLY_FILLED",
                "FILLED",
                "CANCELLED",
                "REJECTED",
            ],
            "allowed_transitions": {
                "NEW": ["PENDING_SUBMIT", "REJECTED"],
                "PENDING_SUBMIT": ["SUBMITTED", "REJECTED", "CANCELLED"],
                "SUBMITTED": ["PARTIALLY_FILLED", "FILLED", "CANCELLED", "REJECTED"],
                "PARTIALLY_FILLED": ["FILLED", "CANCELLED"],
                "FILLED": [],
                "CANCELLED": [],
                "REJECTED": [],
            },
            "status": "registry_defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📜 已輸出 order state machine：{self.path}")
        return self.path, payload
