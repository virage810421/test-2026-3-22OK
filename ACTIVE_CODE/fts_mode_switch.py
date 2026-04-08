# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ModeSwitchPolicy:
    def __init__(self):
        self.path = PATHS.runtime_dir / "mode_switch_policy.json"

    def build(self):
        mode = getattr(CONFIG, "mode", "PAPER")
        broker_type = getattr(CONFIG, "broker_type", "paper")
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "current_mode": mode,
            "current_broker_type": broker_type,
            "policy": {
                "paper_allowed_directly": True,
                "live_requires_approval": True,
                "live_requires_submission_gate": True,
                "live_requires_launch_gate": True,
                "live_requires_live_safety_gate": True,
                "notes": "v37 先把切換規則工程化，尚未真正開放 live 自動送單"
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔀 已輸出 mode switch policy：{self.path}")
        return self.path, payload
