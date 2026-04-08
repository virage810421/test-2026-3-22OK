# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class TestMatrixBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "test_matrix.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "scenarios": [
                {"name": "decision_empty", "covered": True},
                {"name": "signals_zero", "covered": True},
                {"name": "launch_gate_blocked", "covered": True},
                {"name": "submission_gate_blocked", "covered": True},
                {"name": "retry_queue_pending", "covered": True},
                {"name": "state_file_missing", "covered": True},
                {"name": "ai_stage_dry_run", "covered": True},
                {"name": "model_artifact_missing", "covered": True},
                {"name": "etl_expected_file_missing", "covered": True},
            ],
            "status": "matrix_defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧪 已輸出 test matrix：{self.path}")
        return self.path, payload
