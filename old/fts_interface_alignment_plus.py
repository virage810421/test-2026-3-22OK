# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class InterfaceAlignmentPlus:
    def __init__(self):
        self.path = PATHS.runtime_dir / "interface_alignment_plus.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "aligned": {
                "research_to_decision_contract": True,
                "decision_to_execution_contract": True,
                "execution_to_report_contract": True,
                "execution_to_state_contract": True,
                "risk_to_submission_contract": True,
            },
            "partial": {
                "live_broker_runtime_binding": "partial",
                "research_renderer_binding": "partial",
                "model_runtime_selection_feedback": "partial",
            },
            "not_done_yet": [
                "真券商 live adapter 細節尚未完全實接",
                "研究圖表舊模組仍是部分橋接",
                "模型運行後的回饋閉環還可再加深"
            ],
            "status": "alignment_plus_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧩 已輸出 interface alignment plus：{self.path}")
        return self.path, payload
