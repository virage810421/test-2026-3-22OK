# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class BridgeReplacementPlanBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "bridge_replacement_plan.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "waves": [
                {
                    "wave": 1,
                    "focus": "明確 contract 與治理層",
                    "targets": [
                        "monthly_revenue_simple.py",
                        "ml_data_generator.py",
                        "ml_trainer.py"
                    ]
                },
                {
                    "wave": 2,
                    "focus": "逐步吸收可重複邏輯",
                    "targets": [
                        "yahoo_csv_to_sql.py",
                        "model_governance.py"
                    ]
                },
                {
                    "wave": 3,
                    "focus": "保留少數高價值專業引擎",
                    "targets": [
                        "advanced_chart.py",
                        "daily_chip_etl.py"
                    ]
                }
            ],
            "principle": "不是全部刪掉，而是把該吸收的吸收、該保留的保留成正規子模組",
            "status": "replacement_plan_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛠️ 已輸出 bridge replacement plan：{self.path}")
        return self.path, payload
