# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class GateSummaryBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "gate_summary.json"

    def build(self, research_gate, model_gate, launch_gate, live_safety_gate, broker_approval_gate, submission_gate):
        summary = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "research_gate": research_gate.get("go_for_decision_linkage", False),
            "model_gate": model_gate.get("go_for_model_linkage", False),
            "launch_gate": launch_gate.get("go_for_execution", False),
            "live_safety_gate": live_safety_gate.get("paper_live_safe", False),
            "broker_approval_gate": broker_approval_gate.get("go_for_broker_submission", False),
            "submission_gate": submission_gate.get("go_for_submission_contract", False),
        }
        summary["all_green"] = all(v for k, v in summary.items() if isinstance(v, bool))

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        log(f"🚥 已輸出 gate summary：{self.path}")
        return self.path, summary
