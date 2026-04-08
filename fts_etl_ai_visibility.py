# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ETLAIVisibilityBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "etl_ai_visibility.json"

    def build(self, ai_exec: dict):
        expected_inputs = [
            str(PATHS.base_dir / "daily_chip_etl.py"),
            str(PATHS.base_dir / "monthly_revenue_simple.py"),
            str(PATHS.base_dir / "yahoo_csv_to_sql.py"),
            str(PATHS.base_dir / "ml_data_generator.py"),
            str(PATHS.base_dir / "ml_trainer.py"),
            str(PATHS.base_dir / "model_governance.py"),
        ]
        expected_outputs = [
            str(PATHS.base_dir / "data" / "ml_training_data.csv"),
            str(PATHS.base_dir / "models"),
            str(PATHS.base_dir / "daily_decision_desk.csv"),
        ]
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "expected_inputs": [
                {"path": p, "exists": Path(p).exists()} for p in expected_inputs
            ],
            "expected_outputs": [
                {"path": p, "exists": Path(p).exists()} for p in expected_outputs
            ],
            "ai_exec_summary": {
                "enabled": ai_exec.get("ai_stage_enabled", False),
                "dry_run": ai_exec.get("dry_run", True),
                "executed_count": len(ai_exec.get("executed", [])),
                "skipped_count": len(ai_exec.get("skipped", [])),
                "failed_count": len(ai_exec.get("failed", [])),
            },
            "status": "visibility_only"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛰️ 已輸出 etl/ai visibility：{self.path}")
        return self.path, payload
