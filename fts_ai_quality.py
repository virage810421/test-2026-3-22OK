# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class AIQualityReportBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "ai_quality_report.json"

    def build(self, ai_exec: dict):
        expected_inputs = [
            PATHS.base_dir / "ml_data_generator.py",
            PATHS.base_dir / "ml_trainer.py",
            PATHS.base_dir / "model_governance.py",
            PATHS.base_dir / "data" / "ml_training_data.csv",
        ]
        expected_outputs = [
            PATHS.base_dir / "models",
            PATHS.base_dir / "daily_decision_desk.csv",
        ]
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "inputs": [{"path": str(p), "exists": p.exists()} for p in expected_inputs],
            "outputs": [{"path": str(p), "exists": p.exists()} for p in expected_outputs],
            "ai_exec_summary": {
                "enabled": ai_exec.get("ai_stage_enabled", False),
                "dry_run": ai_exec.get("dry_run", True),
                "executed_count": len(ai_exec.get("executed", [])),
                "skipped_count": len(ai_exec.get("skipped", [])),
                "failed_count": len(ai_exec.get("failed", [])),
            },
            "status": "quality_skeleton"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧠 已輸出 ai quality report：{self.path}")
        return self.path, payload
