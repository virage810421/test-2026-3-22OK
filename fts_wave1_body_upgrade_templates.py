# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class Wave1BodyUpgradeTemplatesBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "wave1_body_upgrade_templates.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "templates": {
                "daily_chip_etl.py": {
                    "new_outputs": [
                        "chip_etl_quality_report.json",
                        "chip_etl_batch_stats.json",
                        "chip_sql_sync_summary.json",
                        "chip_error_classification.json"
                    ],
                    "new_fields": [
                        "source_name",
                        "fetch_started_at",
                        "fetch_finished_at",
                        "rows_downloaded",
                        "rows_written_to_sql",
                        "missing_field_count",
                        "error_count"
                    ]
                },
                "monthly_revenue_simple.py": {
                    "new_outputs": [
                        "monthly_revenue_quality_report.json",
                        "monthly_revenue_csv_sql_consistency.json",
                        "monthly_revenue_publish_window_guard.json",
                        "monthly_revenue_fallback_summary.json"
                    ],
                    "new_fields": [
                        "publish_window_ok",
                        "source_primary",
                        "source_fallback_used",
                        "csv_row_count",
                        "sql_row_count",
                        "coverage_ratio",
                        "error_count"
                    ]
                },
                "ml_data_generator.py": {
                    "new_outputs": [
                        "ml_data_feature_summary.json",
                        "ml_data_missing_value_report.json",
                        "ml_data_output_version.json",
                        "ml_data_quality_report.json"
                    ],
                    "new_fields": [
                        "feature_count",
                        "label_count",
                        "missing_value_ratio",
                        "generated_row_count",
                        "output_version",
                        "quality_score"
                    ]
                }
            },
            "status": "body_templates_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧱 已輸出 wave1 body upgrade templates：{self.path}")
        return self.path, payload
