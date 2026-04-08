# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LiveAdapterStubBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "live_adapter_stub.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "stub_methods": [
                "connect",
                "place_order",
                "cancel_order",
                "get_order_status",
                "get_positions",
                "get_cash",
                "disconnect",
            ],
            "safety_rules": [
                "default_disabled",
                "requires_live_approval_workflow",
                "requires_operator_confirmation",
                "requires_callback_monitoring",
            ],
            "status": "stub_defined_not_enabled"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧱 已輸出 live adapter stub：{self.path}")
        return self.path, payload
