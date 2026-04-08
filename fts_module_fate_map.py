# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ModuleFateMapBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "module_fate_map.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "modules": [
                {
                    "file": "daily_chip_etl.py",
                    "current_state": "legacy_bridge_engine",
                    "target_state": "逐步吸入新主線 or 保留為獨立ETL子模組",
                    "recommended_direction": "先保留，再逐段拆進新ETL orchestrator",
                },
                {
                    "file": "monthly_revenue_simple.py",
                    "current_state": "legacy_bridge_engine",
                    "target_state": "逐步吸入新主線 or 保留為月營收專屬子模組",
                    "recommended_direction": "保留，後續做 module contract 化",
                },
                {
                    "file": "yahoo_csv_to_sql.py",
                    "current_state": "legacy_bridge_engine",
                    "target_state": "被 fundamentals ETL 主線逐步取代",
                    "recommended_direction": "中期傾向被吸收，不建議永久橋接",
                },
                {
                    "file": "advanced_chart.py",
                    "current_state": "legacy_bridge_renderer",
                    "target_state": "保留為 chart engine 或被新 chart layer 契約化接管",
                    "recommended_direction": "偏向保留成專業 rendering 子模組",
                },
                {
                    "file": "ml_data_generator.py",
                    "current_state": "legacy_bridge_training_data_engine",
                    "target_state": "被 AI pipeline 包裝，不一定要刪",
                    "recommended_direction": "適合保留成 data generation 子模組",
                },
                {
                    "file": "ml_trainer.py",
                    "current_state": "legacy_bridge_training_engine",
                    "target_state": "被 AI manager/pipeline 接管入口，但核心訓練可保留",
                    "recommended_direction": "適合變成新主線底下的 trainer backend",
                },
                {
                    "file": "model_governance.py",
                    "current_state": "legacy_bridge_governance_engine",
                    "target_state": "逐步併入 model registry/gate 主線",
                    "recommended_direction": "中期傾向吸收進新治理主線",
                }
            ],
            "summary": {
                "not_all_forever_bridge": True,
                "best_end_state": "新主控 + 少量穩定子模組，而不是新主控 + 一堆永久 legacy bridge"
            },
            "status": "fate_map_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧬 已輸出 module fate map：{self.path}")
        return self.path, payload
