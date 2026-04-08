# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class InterfaceAuditBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "interface_audit.json"

    def build(self):
        audit = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "aligned": {
                "etl_to_main_control": True,
                "ai_to_main_control": True,
                "decision_to_execution": True,
                "launch_gate_to_execution": True,
                "live_safety_to_execution": True,
                "broker_approval_to_execution": True,
                "ops_dashboard_to_runtime": True,
            },
            "partial_or_not_fully_governed": {
                "research_to_decision_quality_contract": "partial",
                "model_selection_to_decision_policy": "partial",
                "live_broker_submission_contract": "reserved_only",
            },
            "not_aligned_yet": [
                "真券商實接提交契約尚未正式落地",
                "research 輸出品質雖已有 gate，但還缺更完整的版本化治理",
                "model 選用規則已初步治理，但尚未形成完整回退/升版策略"
            ]
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(audit, f, ensure_ascii=False, indent=2)
        log(f"🧩 已輸出 interface audit：{self.path}")
        return self.path, audit
