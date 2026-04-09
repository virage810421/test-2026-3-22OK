# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ValidationSuiteBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "validation_suite_report.json"

    def build(self, launch_gate: dict, model_gate: dict, live_safety_gate: dict, broker_approval_gate: dict, submission_gate: dict):
        checks = {
            "launch_gate": bool(launch_gate.get("go_for_execution", False)),
            "model_gate": bool(model_gate.get("go_for_model_linkage", False)),
            "live_safety_gate": bool(live_safety_gate.get("paper_live_safe", False)),
            "broker_approval_gate": bool(broker_approval_gate.get("go_for_broker_submission", False)),
            "submission_gate": bool(submission_gate.get("go_for_submission_contract", False)),
        }
        failed_checks = [k for k, v in checks.items() if not v]

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "checks": checks,
            "failed_checks": failed_checks,
            "all_passed": len(failed_checks) == 0,
            "summary": {
                "total_checks": len(checks),
                "passed_checks": sum(1 for v in checks.values() if v),
                "failed_checks": len(failed_checks),
            }
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧪 已輸出 validation suite report：{self.path}")
        return self.path, payload
