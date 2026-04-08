# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class SingleCoreMigrationBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "single_core_migration.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "phases": [
                {
                    "phase": 1,
                    "name": "治理收口",
                    "done": True,
                    "items": [
                        "interface alignment",
                        "decision consistency",
                        "etl quality",
                        "ai quality",
                        "callback normalization",
                        "state/recovery/report governance"
                    ]
                },
                {
                    "phase": 2,
                    "name": "legacy bridge 明確化",
                    "done": True,
                    "items": [
                        "盤點哪些仍直接使用舊 code",
                        "盤點哪些可先封存",
                        "盤點哪些新骨架已可承接"
                    ]
                },
                {
                    "phase": 3,
                    "name": "逐步單核化",
                    "done": False,
                    "items": [
                        "把 ETL 實抓邏輯逐段搬進新主線",
                        "把 chart rendering 契約化後再替換",
                        "把 ml trainer / data generator 逐步包進新 pipeline"
                    ]
                },
                {
                    "phase": 4,
                    "name": "live-ready 最終收口",
                    "done": False,
                    "items": [
                        "broker adapter 真實實作",
                        "callback loop 實接",
                        "對帳引擎深化",
                        "實盤保護/回退機制"
                    ]
                }
            ],
            "status": "migration_plan_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧭 已輸出 single core migration：{self.path}")
        return self.path, payload
