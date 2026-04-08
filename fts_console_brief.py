# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ConsoleBriefBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "console_brief.json"

    def build(self, ai_exec, compat_info, readiness, research_gate, model_gate, launch_gate, live_safety, broker_approval, submission_gate, execution_result):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "brief": {
                "ai_stage_enabled": ai_exec.get("ai_stage_enabled", False),
                "ai_dry_run": ai_exec.get("dry_run", True),
                "ai_executed_count": len(ai_exec.get("executed", [])),
                "decision_rows": compat_info.get("row_count", 0),
                "signal_count": readiness.get("total_signals", 0),
                "research_gate": research_gate.get("go_for_decision_linkage", False),
                "model_gate": model_gate.get("go_for_model_linkage", False),
                "launch_gate": launch_gate.get("go_for_execution", False),
                "live_safety_gate": live_safety.get("paper_live_safe", False),
                "broker_approval_gate": broker_approval.get("go_for_broker_submission", False),
                "submission_gate": submission_gate.get("go_for_submission_contract", False),
                "submitted": execution_result.get("submitted", 0),
                "filled": execution_result.get("filled", 0),
                "partially_filled": execution_result.get("partially_filled", 0),
                "rejected": execution_result.get("rejected", 0),
                "cancelled": execution_result.get("cancelled", 0),
            },
            "status": "console_brief_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🪖 已輸出 console brief：{self.path}")
        return self.path, payload

    def render_lines(self, payload: dict):
        b = payload["brief"]
        return [
            "================= 戰情室摘要 =================",
            f"AI階段 | enabled={b['ai_stage_enabled']} | dry_run={b['ai_dry_run']} | executed={b['ai_executed_count']}",
            f"Decision | rows={b['decision_rows']} | signals={b['signal_count']}",
            f"Gates | research={b['research_gate']} | model={b['model_gate']} | launch={b['launch_gate']}",
            f"Gates | live={b['live_safety_gate']} | approval={b['broker_approval_gate']} | submission={b['submission_gate']}",
            f"Execution | submitted={b['submitted']} | filled={b['filled']} | partial={b['partially_filled']} | rejected={b['rejected']} | cancelled={b['cancelled']}",
            "================================================"
        ]
