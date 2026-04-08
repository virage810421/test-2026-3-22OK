# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class SubmoduleContractBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "submodule_contracts.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "contracts": {
                "etl_submodule_contract": {
                    "inputs": ["config", "local caches", "sql connection"],
                    "outputs": ["csv artifacts", "sql sync result", "summary stats"]
                },
                "chart_submodule_contract": {
                    "inputs": ["ticker", "dataframe", "signals", "render config"],
                    "outputs": ["chart artifact", "chart metadata"]
                },
                "training_data_submodule_contract": {
                    "inputs": ["etl outputs", "feature config"],
                    "outputs": ["ml_training_data.csv", "feature summary"]
                },
                "trainer_submodule_contract": {
                    "inputs": ["training data", "model config"],
                    "outputs": ["model artifacts", "training summary", "validation metrics"]
                },
                "model_governance_contract": {
                    "inputs": ["model artifacts", "evaluation metrics"],
                    "outputs": ["selected model", "registry update", "deployment recommendation"]
                }
            },
            "status": "contracts_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📐 已輸出 submodule contracts：{self.path}")
        return self.path, payload
