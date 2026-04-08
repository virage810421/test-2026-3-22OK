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
