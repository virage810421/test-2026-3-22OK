# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 5 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_etl_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ETLAIVisibilityBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "etl_ai_visibility.json"

    def build(self, ai_exec: dict):
        expected_inputs = [
            str(PATHS.base_dir / "daily_chip_etl.py"),
            str(PATHS.base_dir / "monthly_revenue_simple.py"),
            str(PATHS.base_dir / "yahoo_csv_to_sql.py"),
            str(PATHS.base_dir / "ml_data_generator.py"),
            str(PATHS.base_dir / "ml_trainer.py"),
            str(PATHS.base_dir / "model_governance.py"),
        ]
        expected_outputs = [
            str(PATHS.base_dir / "data" / "ml_training_data.csv"),
            str(PATHS.base_dir / "models"),
            str(PATHS.base_dir / "daily_decision_desk.csv"),
        ]
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "expected_inputs": [
                {"path": p, "exists": Path(p).exists()} for p in expected_inputs
            ],
            "expected_outputs": [
                {"path": p, "exists": Path(p).exists()} for p in expected_outputs
            ],
            "ai_exec_summary": {
                "enabled": ai_exec.get("ai_stage_enabled", False),
                "dry_run": ai_exec.get("dry_run", True),
                "executed_count": len(ai_exec.get("executed", [])),
                "skipped_count": len(ai_exec.get("skipped", [])),
                "failed_count": len(ai_exec.get("failed", [])),
            },
            "status": "visibility_only"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛰️ 已輸出 etl/ai visibility：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_etl_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ETLBatchStatsBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "etl_batch_stats.json"

    def build(self, upstream_exec: dict):
        executed = upstream_exec.get("executed", [])
        failed = upstream_exec.get("failed", [])
        skipped = upstream_exec.get("skipped", [])

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "stats": {
                "executed_count": len(executed),
                "failed_count": len(failed),
                "skipped_count": len(skipped),
                "success_ratio": round(len(executed) / max(1, (len(executed) + len(failed))), 4),
            },
            "executed_preview": executed[:10],
            "failed_preview": failed[:10],
            "skipped_preview": skipped[:10],
            "status": "batch_stats_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"📦 已輸出 etl batch stats：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_etl_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ETLDataQualityPlusBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "etl_data_quality_plus.json"

    def build(self):
        tracked = [
            PATHS.base_dir / "daily_chip_etl.py",
            PATHS.base_dir / "monthly_revenue_simple.py",
            PATHS.base_dir / "yahoo_csv_to_sql.py",
            PATHS.base_dir / "latest_monthly_revenue_master.csv",
            PATHS.base_dir / "market_financials_backup.csv",
            PATHS.base_dir / "data" / "ml_training_data.csv",
        ]
        rows = [{"path": str(p), "exists": p.exists(), "is_file": p.is_file()} for p in tracked]
        existing = sum(1 for r in rows if r["exists"])
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "tracked_assets": rows,
            "summary": {
                "tracked_count": len(rows),
                "existing_count": existing,
                "coverage_ratio": round(existing / len(rows), 4) if rows else 0,
            },
            "status": "plus_quality_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧪 已輸出 etl data quality plus：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_etl_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ETLFieldCompletenessBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "etl_field_completeness.json"

    def build(self, compat_info: dict):
        row_count = compat_info.get("row_count", 0)
        rows_with_ticker = compat_info.get("rows_with_ticker", 0)
        rows_with_action = compat_info.get("rows_with_action", 0)
        rows_with_price = compat_info.get("rows_with_price", 0)

        def ratio(x):
            return round(x / row_count, 4) if row_count else 0

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "row_count": row_count,
            "field_completeness": {
                "ticker_ratio": ratio(rows_with_ticker),
                "action_ratio": ratio(rows_with_action),
                "price_ratio": ratio(rows_with_price),
            },
            "status": "decision-side completeness proxy"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧬 已輸出 etl field completeness：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_etl_quality_suite.py
# ==============================================================================
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
