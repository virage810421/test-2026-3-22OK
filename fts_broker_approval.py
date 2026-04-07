# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class BrokerApprovalGate:
    """
    v33:
    在未來接真券商前，先做雙重確認邏輯。
    預設 PAPER 一律可通過；
    若不是 PAPER，則要求更嚴格的 approval 條件。
    """
    def __init__(self):
        self.path = PATHS.runtime_dir / "broker_approval_gate.json"

    def evaluate(self, launch_gate: dict, live_safety_gate: dict):
        mode = getattr(CONFIG, "mode", "PAPER")
        broker_type = getattr(CONFIG, "broker_type", "paper")

        failures = []
        warnings = []

        if not launch_gate.get("go_for_execution", False):
            failures.append({
                "type": "launch_gate_blocked",
                "message": "launch gate 未通過"
            })

        if not live_safety_gate.get("paper_live_safe", False):
            failures.append({
                "type": "live_safety_blocked",
                "message": "live safety gate 未通過"
            })

        requires_explicit_approval = not (str(mode).upper() == "PAPER" and str(broker_type).lower() == "paper")

        if requires_explicit_approval:
            warnings.append({
                "type": "explicit_live_approval_required",
                "message": f"mode={mode}, broker_type={broker_type}，未來接真券商時必須加入人工審批/簽核"
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "mode": mode,
            "broker_type": broker_type,
            "requires_explicit_approval": requires_explicit_approval,
            "go_for_broker_submission": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(
            f"🧾 Broker Approval Gate | go_for_broker_submission={payload['go_for_broker_submission']} | "
            f"requires_explicit_approval={payload['requires_explicit_approval']} | "
            f"failures={len(failures)} | warnings={len(warnings)}"
        )
        return self.path, payload
