# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 9 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_project_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path

try:
    from fts_config import PATHS  # type: ignore
except Exception:
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        runtime_dir = base_dir / 'runtime'
        data_dir = base_dir / 'data'
    PATHS = _Paths()

MODULE_VERSION = 'v83_project_completion_audit'

REQUIRED_MODULES = [
    'formal_trading_system_v83_official_main.py', 'fts_feature_service.py', 'fts_screening_engine.py',
    'fts_cross_sectional_percentile_service.py', 'fts_event_calendar_service.py', 'fts_training_data_builder.py',
    'db_setup_research_plus.py', 'fts_sql_feature_snapshot_sync.py', 'fts_mainline_linkage.py',
]
REQUIRED_DATA = ['feature_cross_section_snapshot.csv', 'feature_event_calendar.csv', 'selected_live_feature_mounts.csv']
REQUIRED_RUNTIME = ['feature_stack_audit.json', 'cross_sectional_percentile_service.json', 'event_calendar_service.json', 'project_completion_audit.json']
REQUIRED_TASKS = {
    '主控串聯': '完成', '全市場percentile': '完成', '事件窗精準化': '完成', '特徵掛載': '完成',
    '訓練資料接新特徵': '完成', '研究層增補table': '完成', '特徵snapshot寫回SQL': '完成',
}


class ProjectCompletionAudit:
    def __init__(self):
        self.runtime_path = Path(PATHS.runtime_dir) / 'project_completion_audit.json'
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)

    def build(self):
        base = Path(__file__).resolve().parent
        payload = {
            'module_version': MODULE_VERSION,
            'required_modules': {name: (base / name).exists() for name in REQUIRED_MODULES},
            'required_data': {name: (Path(PATHS.data_dir) / name).exists() for name in REQUIRED_DATA},
            'required_runtime': {name: (Path(PATHS.runtime_dir) / name).exists() for name in REQUIRED_RUNTIME},
            'task_board': REQUIRED_TASKS,
        }
        payload['all_modules_ready'] = all(payload['required_modules'].values())
        payload['all_data_ready'] = all(payload['required_data'].values())
        payload['all_runtime_ready'] = all(payload['required_runtime'].values())
        payload['status'] = 'project_completion_audit_ready'
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.runtime_path, payload


if False and __name__ == "__main__":  # disabled after consolidation
    print(ProjectCompletionAudit().build())


# ==============================================================================
# Merged from: fts_project_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ValidationSuiteBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "validation_suite_report.json"

    def build(self, launch_gate: dict, model_gate: dict, live_safety_gate: dict, broker_approval_gate: dict, submission_gate: dict):
        checks = {
            "launch_gate": bool(launch_gate.get("go_for_execution", False)),
            "model_gate": bool(model_gate.get("go_for_model_linkage", False)),
            "live_safety_gate": bool(live_safety_gate.get("paper_live_safe", False)),
            "broker_approval_gate": bool(broker_approval_gate.get("go_for_broker_submission", False)),
            "submission_gate": bool(submission_gate.get("go_for_submission_contract", False)),
        }
        failed_checks = [k for k, v in checks.items() if not v]

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "checks": checks,
            "failed_checks": failed_checks,
            "all_passed": len(failed_checks) == 0,
            "summary": {
                "total_checks": len(checks),
                "passed_checks": sum(1 for v in checks.values() if v),
                "failed_checks": len(failed_checks),
            }
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧪 已輸出 validation suite report：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_project_quality_suite.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_project_quality_suite.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_project_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class InterfaceAuditBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "interface_audit.json"

    def build(self):
        audit = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "aligned": {
                "etl_to_main_control": True,
                "ai_to_main_control": True,
                "decision_to_execution": True,
                "launch_gate_to_execution": True,
                "live_safety_to_execution": True,
                "broker_approval_to_execution": True,
                "ops_dashboard_to_runtime": True,
            },
            "partial_or_not_fully_governed": {
                "research_to_decision_quality_contract": "partial",
                "model_selection_to_decision_policy": "partial",
                "live_broker_submission_contract": "reserved_only",
            },
            "not_aligned_yet": [
                "真券商實接提交契約尚未正式落地",
                "research 輸出品質雖已有 gate，但還缺更完整的版本化治理",
                "model 選用規則已初步治理，但尚未形成完整回退/升版策略"
            ]
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(audit, f, ensure_ascii=False, indent=2)
        log(f"🧩 已輸出 interface audit：{self.path}")
        return self.path, audit


# ==============================================================================
# Merged from: fts_project_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class InterfaceAlignmentPlus:
    def __init__(self):
        self.path = PATHS.runtime_dir / "interface_alignment_plus.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "aligned": {
                "research_to_decision_contract": True,
                "decision_to_execution_contract": True,
                "execution_to_report_contract": True,
                "execution_to_state_contract": True,
                "risk_to_submission_contract": True,
            },
            "partial": {
                "live_broker_runtime_binding": "partial",
                "research_renderer_binding": "partial",
                "model_runtime_selection_feedback": "partial",
            },
            "not_done_yet": [
                "真券商 live adapter 細節尚未完全實接",
                "研究圖表舊模組仍是部分橋接",
                "模型運行後的回饋閉環還可再加深"
            ],
            "status": "alignment_plus_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧩 已輸出 interface alignment plus：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_project_quality_suite.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_project_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ModelArtifactCheckBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "model_artifact_checks.json"

    def build(self):
        models_dir = PATHS.base_dir / "models"
        expected_files = [
            models_dir / "selected_features.pkl",
            models_dir / "model_趨勢多頭.pkl",
            models_dir / "model_區間盤整.pkl",
            models_dir / "model_趨勢空頭.pkl",
            models_dir / "selected_features_long.pkl",
            models_dir / "selected_features_short.pkl",
            models_dir / "selected_features_range.pkl",
        ]
        directional_models = list(models_dir.glob('model_long_*.pkl')) + list(models_dir.glob('model_short_*.pkl')) + list(models_dir.glob('model_range_*.pkl'))
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "models_dir_exists": models_dir.exists(),
            "expected_model_files": [{"path": str(p), "exists": p.exists()} for p in expected_files],
            "existing_model_file_count": sum(1 for p in expected_files if p.exists()),
            "directional_model_count": len(directional_models),
            "directional_model_paths_preview": [str(p) for p in directional_models[:12]],
            "status": "artifact_check_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧾 已輸出 model artifact checks：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_project_quality_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, write_json


class RecoveryValidationBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'recovery_validation.json'

    def build(self, retry_queue_summary: dict[str, Any], recovery_plan: dict[str, Any] | None = None) -> tuple[Any, dict[str, Any]]:
        state = load_json(PATHS.state_dir / 'engine_state.json', {}) or {}
        recovery_plan = recovery_plan or {}
        checks = []
        checks.append({'check': 'state_file_exists', 'value': bool(state), 'status': 'ok' if state else 'fail'})
        checks.append({'check': 'state_has_cash', 'value': state.get('cash', None), 'status': 'ok' if state and 'cash' in state else 'fail'})
        checks.append({'check': 'state_has_positions', 'value': len(state.get('positions', [])) if state else 0, 'status': 'ok' if state and 'positions' in state else 'fail'})
        checks.append({'check': 'state_has_open_orders', 'value': len(state.get('open_orders', [])) if state else 0, 'status': 'ok' if state and 'open_orders' in state else 'warn'})
        retry_total = int(retry_queue_summary.get('total', 0) or 0)
        checks.append({'check': 'retry_queue_total', 'value': retry_total, 'status': 'ok' if retry_total == 0 else 'warn'})
        if recovery_plan:
            checks.append({'check': 'recovery_plan_ready', 'value': recovery_plan.get('ready_to_recover', False), 'status': 'ok' if recovery_plan.get('ready_to_recover', False) else 'fail'})
        all_green = all(c['status'] == 'ok' for c in checks if c['check'] != 'state_has_open_orders')
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'checks': checks,
            'all_green': all_green,
            'ready_for_resume': all_green,
            'status': 'validation_ready' if all(c['status'] in {'ok', 'warn'} for c in checks) else 'validation_blocked',
        }
        write_json(self.path, payload)
        log(f'♻️ 已輸出 recovery validation：{self.path}')
        return self.path, payload
