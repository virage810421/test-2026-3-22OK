# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyInventoryBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_inventory.json"

    def build(self):
        keep_as_legacy_bridge = [
            "daily_chip_etl.py",
            "monthly_revenue_simple.py",
            "yahoo_csv_to_sql.py",
            "advanced_chart.py",
            "ml_data_generator.py",
            "ml_trainer.py",
            "model_governance.py",
        ]
        likely_redundant_or_review = [
            "formal_trading_system_v40.py",
            "formal_trading_system_v41.py",
            "formal_trading_system_v42.py",
            "formal_trading_system_v43.py",
            "formal_trading_system_v44.py",
            "formal_trading_system_v45.py",
            "formal_trading_system_v46.py",
            "formal_trading_system_v47.py",
            "formal_trading_system_v48.py",
            "formal_trading_system_v49.py",
            "formal_trading_system_v50.py",
            "formal_trading_system_v51.py",
            "formal_trading_system_v52.py",
        ]
        rows = []
        for name in keep_as_legacy_bridge + likely_redundant_or_review:
            p = PATHS.base_dir / name
            rows.append({
                "file": name,
                "exists": p.exists(),
                "category": "legacy_bridge_keep" if name in keep_as_legacy_bridge else "older_mainline_review",
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "legacy_bridge_keep_count": len(keep_as_legacy_bridge),
                "older_mainline_review_count": len(likely_redundant_or_review),
            },
            "rows": rows,
            "status": "inventory_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🗃️ 已輸出 legacy inventory：{self.path}")
        return self.path, payload
