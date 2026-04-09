# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class StageIOMapBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "stage_io_map.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "stages": {
                "etl": {
                    "inputs": ["TWSE/TPEX/OpenAPI/MOPS/Yahoo/FinMind", "local csv cache", "SQL"],
                    "outputs": ["local csv", "SQL tables", "feature-ready source data"]
                },
                "ai_training": {
                    "inputs": ["data/ml_training_data.csv", "feature generator", "trainer"],
                    "outputs": ["models/", "model governance metadata"]
                },
                "decision": {
                    "inputs": ["models", "research outputs", "normalized features"],
                    "outputs": ["daily_decision_desk.csv"]
                },
                "execution": {
                    "inputs": ["daily_decision_desk.csv", "risk filters", "submission contract"],
                    "outputs": ["orders", "fills", "state", "reports"]
                }
            },
            "status": "io_map_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🗺️ 已輸出 stage io map：{self.path}")
        return self.path, payload
