# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LiveApprovalWorkflowBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "live_approval_workflow.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "workflow": [
                {"step": 1, "name": "launch_gate_pass"},
                {"step": 2, "name": "live_safety_gate_pass"},
                {"step": 3, "name": "broker_approval_gate_pass"},
                {"step": 4, "name": "submission_contract_gate_pass"},
                {"step": 5, "name": "operator_confirmation_reserved"},
                {"step": 6, "name": "live_adapter_submission_reserved"},
                {"step": 7, "name": "callback_monitoring_reserved"},
            ],
            "status": "workflow_defined_not_live_enabled"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"✅ 已輸出 live approval workflow：{self.path}")
        return self.path, payload
