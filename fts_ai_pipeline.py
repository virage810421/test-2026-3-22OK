# -*- coding: utf-8 -*-
import json
from pathlib import Path
from datetime import datetime
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class AIPipelineRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "ai_pipeline_registry.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "ai_pipeline": {
                "feature_data_script": "ml_data_generator.py",
                "trainer_script": "ml_trainer.py",
                "model_governance_script": "model_governance.py",
                "expected_outputs": [
                    "data/ml_training_data.csv",
                    "models/",
                    "daily_decision_desk.csv"
                ],
                "status": "registered_not_fully_managed",
                "notes": "v29 開始把 AI 訓練正式掛進主控視角，但預設仍為安全模式，不自動執行。"
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧠 已輸出 AI pipeline registry：{self.path}")
        return self.path, payload

class AIPipelineInspector:
    def inspect(self):
        checks = {
            "feature_data_script_exists": (PATHS.base_dir / "ml_data_generator.py").exists(),
            "trainer_script_exists": (PATHS.base_dir / "ml_trainer.py").exists(),
            "governance_script_exists": (PATHS.base_dir / "model_governance.py").exists(),
            "training_data_exists": (PATHS.base_dir / "data" / "ml_training_data.csv").exists(),
            "models_dir_exists": (PATHS.base_dir / "models").exists(),
        }
        summary = {
            "generated_at": now_str(),
            "checks": checks,
            "all_core_scripts_present": checks["feature_data_script_exists"] and checks["trainer_script_exists"],
            "training_assets_present": checks["training_data_exists"] and checks["models_dir_exists"],
        }
        out = PATHS.runtime_dir / "ai_pipeline_status.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        log(f"🧪 已輸出 AI pipeline status：{out}")
        return out, summary

class AIDecisionBridge:
    def build_summary(self):
        """
        先不直接訓練模型，只描述 AI 訓練在整個主架構中的位置。
        """
        summary = {
            "generated_at": now_str(),
            "bridge": {
                "upstream": [
                    "daily_chip_etl.py",
                    "monthly_revenue_simple.py",
                    "yahoo_csv_to_sql.py"
                ],
                "training": [
                    "ml_data_generator.py",
                    "ml_trainer.py",
                    "model_governance.py"
                ],
                "downstream": [
                    "daily_decision_desk.csv",
                    "formal_trading_system_v29.py"
                ],
                "status": "ai_connected_at_architecture_level"
            }
        }
        out = PATHS.runtime_dir / "ai_decision_bridge.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        log(f"🌉 已輸出 AI decision bridge：{out}")
        return out, summary
