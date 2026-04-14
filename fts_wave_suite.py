# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 5 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_wave_suite.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_wave_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class Wave1ContractPackBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "wave1_contract_pack.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "contracts": {
                "daily_chip_etl_contract": {
                    "inputs": ["watchlist/full-market scope", "local csv cache", "sql connection", "finmind/api source"],
                    "outputs": ["chip csv", "sql sync summary", "etl quality summary"]
                },
                "monthly_revenue_contract": {
                    "inputs": ["mops/openapi source", "publish window config", "local csv/sql state"],
                    "outputs": ["monthly revenue csv", "sql sync result", "coverage summary"]
                },
                "ml_data_generator_contract": {
                    "inputs": ["etl outputs", "feature config", "labeling config"],
                    "outputs": ["ml_training_data.csv", "feature summary", "missing value report"]
                }
            },
            "status": "contract_pack_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📦 已輸出 wave1 contract pack：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_wave_suite.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_wave_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class Wave1IOBindingsBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "wave1_io_bindings.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "bindings": [
                {
                    "module": "daily_chip_etl.py",
                    "bind_to": [
                        "etl_quality governance",
                        "batch stats governance",
                        "sql sync summary",
                        "daily ops summary"
                    ]
                },
                {
                    "module": "monthly_revenue_simple.py",
                    "bind_to": [
                        "publish window guard",
                        "csv/sql consistency",
                        "fallback summary",
                        "daily ops summary"
                    ]
                },
                {
                    "module": "ml_data_generator.py",
                    "bind_to": [
                        "ai quality report",
                        "training prod readiness",
                        "trainer promotion policy",
                        "model registry"
                    ]
                }
            ],
            "status": "io_bindings_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔗 已輸出 wave1 io bindings：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_wave_suite.py
# ==============================================================================
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
