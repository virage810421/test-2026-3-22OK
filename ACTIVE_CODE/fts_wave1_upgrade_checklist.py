# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class Wave1UpgradeChecklistBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "wave1_upgrade_checklist.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "checklist": {
                "daily_chip_etl.py": [
                    "補 quality report 輸出",
                    "補 batch stats 輸出",
                    "補 sql sync summary 輸出",
                    "補 error classification 輸出",
                    "補欄位完整率統計"
                ],
                "monthly_revenue_simple.py": [
                    "補 publish window guard 輸出",
                    "補 csv/sql consistency 輸出",
                    "補 fallback summary 輸出",
                    "補 coverage ratio 統計"
                ],
                "ml_data_generator.py": [
                    "補 feature summary 輸出",
                    "補 missing value report",
                    "補 output versioning",
                    "補 data quality score"
                ]
            },
            "status": "checklist_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"✅ 已輸出 wave1 upgrade checklist：{self.path}")
        return self.path, payload
