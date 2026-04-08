# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class Wave1CoreUpgradeBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "wave1_core_upgrade.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "wave": 1,
            "targets": [
                {
                    "file": "daily_chip_etl.py",
                    "upgrade_items": [
                        "etl_quality_report",
                        "batch_stats",
                        "field_completeness",
                        "error_classification",
                        "sql_sync_summary"
                    ],
                    "target_level": 95
                },
                {
                    "file": "monthly_revenue_simple.py",
                    "upgrade_items": [
                        "publish_window_guard",
                        "csv_sql_consistency",
                        "source_fallback_summary",
                        "field_coverage_report"
                    ],
                    "target_level": 95
                },
                {
                    "file": "ml_data_generator.py",
                    "upgrade_items": [
                        "feature_summary",
                        "missing_value_report",
                        "output_versioning",
                        "data_quality_report"
                    ],
                    "target_level": 95
                }
            ],
            "status": "wave1_upgrade_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🌊 已輸出 wave1 core upgrade：{self.path}")
        return self.path, payload
