# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ExecutionCallbackFlowBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "execution_callback_flow.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "flow": [
                "submit_order_payload",
                "receive_broker_callback",
                "normalize_callback_status",
                "validate_callback_fields",
                "update_order_state_machine",
                "reconciliation_engine_check",
                "persist_state_and_report",
            ],
            "integration_points": {
                "callback_norm": "broker_callback_normalization.json",
                "state_machine": "order_state_machine.json",
                "reconciliation_engine": "reconciliation_engine.json",
            },
            "status": "callback_flow_defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔁 已輸出 execution callback flow：{self.path}")
        return self.path, payload
