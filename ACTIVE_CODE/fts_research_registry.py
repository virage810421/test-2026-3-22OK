# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchSelectionRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_selection_registry.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "research_selection_layer": {
                "role": "把 ETL / AI / 基本面 / 技術面 / 風險偏好 整合成 decision 輸出",
                "current_status": "registered_but_not_fully_governed",
                "importance": {
                    "what_it_affects": [
                        "選股品質",
                        "進出場品質",
                        "報酬分佈",
                        "策略穩定性"
                    ],
                    "what_it_does_not_replace": [
                        "風控",
                        "實盤保護",
                        "恢復機制",
                        "驗證閘門"
                    ]
                },
                "notes": "研究/選股層很重要，但它主要決定 alpha 品質；系統安全層則決定你會不會先因工程事故出事。"
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔎 已輸出 research selection registry：{self.path}")
        return self.path, payload
