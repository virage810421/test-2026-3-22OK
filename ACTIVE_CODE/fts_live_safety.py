# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LiveSafetyGate:
    def __init__(self):
        self.path = PATHS.runtime_dir / "live_safety_gate.json"

    def evaluate(self, readiness: dict, launch_gate: dict):
        failures = []
        warnings = []

        mode = getattr(CONFIG, "mode", "PAPER")
        broker_type = getattr(CONFIG, "broker_type", "paper")

        if mode.upper() != "PAPER":
            warnings.append({
                "type": "non_paper_mode",
                "message": f"目前 mode={mode}，已不是純 paper 模式"
            })

        if broker_type.lower() != "paper":
            warnings.append({
                "type": "non_paper_broker",
                "message": f"目前 broker_type={broker_type}，請確認不是誤接真券商"
            })

        if not launch_gate.get("go_for_execution", False):
            failures.append({
                "type": "launch_gate_blocked",
                "message": "發車前驗證閘門未通過"
            })

        if readiness.get("total_signals", 0) == 0:
            warnings.append({
                "type": "zero_signal",
                "message": "本輪有效訊號為 0"
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "mode": mode,
            "broker_type": broker_type,
            "paper_live_safe": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(
            f"🛡️ Live Safety Gate | paper_live_safe={payload['paper_live_safe']} | "
            f"failures={len(failures)} | warnings={len(warnings)}"
        )
        return self.path, payload
