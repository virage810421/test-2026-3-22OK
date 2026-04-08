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
