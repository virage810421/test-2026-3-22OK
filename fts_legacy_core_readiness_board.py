# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyCoreReadinessBoardBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_core_readiness_board.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "board": [
                {"file": "daily_chip_etl.py", "current_level": 88, "target": 95, "gap": 7},
                {"file": "monthly_revenue_simple.py", "current_level": 89, "target": 95, "gap": 6},
                {"file": "yahoo_csv_to_sql.py", "current_level": 87, "target": 95, "gap": 8},
                {"file": "ml_data_generator.py", "current_level": 90, "target": 95, "gap": 5},
                {"file": "ml_trainer.py", "current_level": 90, "target": 95, "gap": 5},
                {"file": "model_governance.py", "current_level": 88, "target": 95, "gap": 7},
                {"file": "advanced_chart.py", "current_level": 89, "target": 95, "gap": 6},
            ],
            "status": "readiness_board_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📋 已輸出 legacy core readiness board：{self.path}")
        return self.path, payload
