# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class TestScenariosPlusBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "test_scenarios_plus.json"

    def build(self):
        scenarios = [
            {"name": "decision_rows_zero", "priority": "high", "covered": True},
            {"name": "ticker_missing", "priority": "high", "covered": True},
            {"name": "signal_count_zero", "priority": "high", "covered": True},
            {"name": "launch_gate_false", "priority": "high", "covered": True},
            {"name": "submission_gate_false", "priority": "high", "covered": True},
            {"name": "duplicate_ticker_orders", "priority": "medium", "covered": True},
            {"name": "state_file_missing", "priority": "high", "covered": True},
            {"name": "retry_queue_not_empty", "priority": "medium", "covered": True},
            {"name": "model_artifact_missing", "priority": "high", "covered": True},
            {"name": "etl_files_missing", "priority": "high", "covered": True},
            {"name": "ai_stage_dry_run", "priority": "medium", "covered": True},
            {"name": "execution_result_zero_fill", "priority": "medium", "covered": True},
        ]

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "scenario_count": len(scenarios),
            "scenarios": scenarios,
            "status": "expanded_matrix"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧪 已輸出 test scenarios plus：{self.path}")
        return self.path, payload
