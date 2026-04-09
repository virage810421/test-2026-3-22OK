# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ETLQualityReportBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "etl_quality_report.json"

    def build(self):
        candidate_files = [
            PATHS.base_dir / "daily_chip_etl.py",
            PATHS.base_dir / "monthly_revenue_simple.py",
            PATHS.base_dir / "yahoo_csv_to_sql.py",
            PATHS.base_dir / "daily_decision_desk.csv",
            PATHS.base_dir / "data" / "ml_training_data.csv",
        ]

        existing = [p for p in candidate_files if p.exists()]
        missing = [str(p) for p in candidate_files if not p.exists()]

        quality = {
            "expected_file_count": len(candidate_files),
            "existing_file_count": len(existing),
            "missing_file_count": len(missing),
            "coverage_ratio": round(len(existing) / len(candidate_files), 4) if candidate_files else 0,
            "missing_files": missing,
        }

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "etl_quality": quality,
            "checks": {
                "file_coverage_ok": quality["coverage_ratio"] >= 0.6,
                "all_expected_files_present": quality["missing_file_count"] == 0,
            },
            "status": "quality_skeleton"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧪 已輸出 etl quality report：{self.path}")
        return self.path, payload
