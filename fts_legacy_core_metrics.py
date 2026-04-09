# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyCoreMetricsBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_core_metrics.json"

    def _score(self, required_files):
        hits = sum(1 for f in required_files if (PATHS.base_dir / f).exists())
        return int(round(90 + (hits / max(len(required_files), 1)) * 10))

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "metrics": {
                "daily_chip_etl.py": {
                    "current_score": self._score(["daily_chip_etl.py", "daily_chip_data_backup.csv"]),
                    "target_score": 98,
                    "priority": "high"
                },
                "monthly_revenue_simple.py": {
                    "current_score": self._score(["monthly_revenue_simple.py", "monthly_revenue_simple.csv", "latest_monthly_revenue_simple.csv"]),
                    "target_score": 98,
                    "priority": "high"
                },
                "ml_data_generator.py": {
                    "current_score": self._score(["ml_data_generator.py", "ml_trainer.py", "model_governance.py"]),
                    "target_score": 98,
                    "priority": "high"
                },
                "master_pipeline.py": {
                    "current_score": self._score(["master_pipeline.py", "launcher.py", "live_paper_trading.py"]),
                    "target_score": 99,
                    "priority": "high"
                }
            },
            "status": "metrics_ready_v62"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📏 已輸出 legacy core metrics：{self.path}")
        return self.path, payload
