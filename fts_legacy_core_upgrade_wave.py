# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyCoreUpgradeWaveBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_core_upgrade_wave.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "waves": [
                {
                    "wave": 1,
                    "focus": "最影響上游與AI輸入品質的核心",
                    "targets": ["daily_chip_etl.py", "monthly_revenue_simple.py", "ml_data_generator.py"]
                },
                {
                    "wave": 2,
                    "focus": "訓練與治理核心",
                    "targets": ["ml_trainer.py", "model_governance.py"]
                },
                {
                    "wave": 3,
                    "focus": "fundamentals與chart專業引擎",
                    "targets": ["yahoo_csv_to_sql.py", "advanced_chart.py"]
                }
            ],
            "status": "upgrade_wave_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🌊 已輸出 legacy core upgrade wave：{self.path}")
        return self.path, payload
