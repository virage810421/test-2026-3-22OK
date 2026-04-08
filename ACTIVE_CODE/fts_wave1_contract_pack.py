# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class Wave1ContractPackBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "wave1_contract_pack.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "contracts": {
                "daily_chip_etl_contract": {
                    "inputs": ["watchlist/full-market scope", "local csv cache", "sql connection", "finmind/api source"],
                    "outputs": ["chip csv", "sql sync summary", "etl quality summary"]
                },
                "monthly_revenue_contract": {
                    "inputs": ["mops/openapi source", "publish window config", "local csv/sql state"],
                    "outputs": ["monthly revenue csv", "sql sync result", "coverage summary"]
                },
                "ml_data_generator_contract": {
                    "inputs": ["etl outputs", "feature config", "labeling config"],
                    "outputs": ["ml_training_data.csv", "feature summary", "missing value report"]
                }
            },
            "status": "contract_pack_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📦 已輸出 wave1 contract pack：{self.path}")
        return self.path, payload
