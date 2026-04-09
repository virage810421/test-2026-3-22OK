# -*- coding: utf-8 -*-
from __future__ import annotations

import json

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class FileClassificationBuilder:
    def __init__(self):
        self.json_path = PATHS.runtime_dir / "file_classification.json"
        self.md_path = PATHS.runtime_dir / "FILE_CLASSIFICATION.md"

    def build(self):
        must_keep = [
            "formal_trading_system_v83_official_main.py",
            "formal_trading_system_v82_three_stage_upgrade.py",
            "fts_fundamentals_etl_mainline.py",
            "fts_training_governance_mainline.py",
            "fts_trainer_backend.py",
            "fts_phase1_upgrade.py",
            "fts_phase2_mock_broker_stage.py",
            "fts_phase3_real_cutover_stage.py",
            "fts_broker_factory.py",
            "fts_broker_paper.py",
            "fts_broker_real_stub.py",
            "fts_decision_execution_bridge.py",
            "fts_live_safety.py",
            "fts_reconciliation_engine.py",
            "fts_preopen_checklist.py",
            "fts_intraday_incident_guard.py",
            "fts_eod_closebook.py",
            "fts_callback_event_schema.py",
            "fts_callback_event_store.py",
            "fts_order_state_machine.py",
            "fts_broker_requirements_contract.py",
            "fts_real_broker_adapter_blueprint.py",
            "fts_live_release_gate.py",
            "fts_live_cutover_plan.py",
            "fts_training_orchestrator.py",
            "fts_training_prod_readiness.py",
            "fts_trainer_promotion_policy.py",
            "ml_data_generator.py",
            "daily_chip_etl.py",
            "monthly_revenue_simple.py",
            "advanced_chart.py",
        ]
        mergeable = [
            "fts_bridge_replacement_plan.py",
            "fts_legacy_bridge_map.py",
            "fts_module_fate_map.py",
            "fts_unused_candidates.py",
            "fts_upgrade_plan.py",
            "fts_target95_plan.py",
            "fts_target95_push.py",
            "fts_target95_scorecard.py",
            "fts_upgrade_truth_report.py",
            "fts_completion_gap_report.py",
            "fts_progress.py",
            "fts_progress_full_report.py",
            "fts_gate_summary.py",
            "fts_console_brief.py",
            "fts_report.py",
            "fts_dashboard.py",
        ]
        hold = [
            "launcher.py",
            "risk_gateway.py",
            "execution_engine.py",
            "paper_broker.py",
            "portfolio_risk.py",
            "system_guard.py",
            "live_paper_trading.py",
            "master_pipeline.py",
        ]
        deletable_after_smoketest = [
            "formal_trading_system_v79.py",
            "formal_trading_system_v80_prebroker_sealed.py",
            "formal_trading_system_v81_mainline_merged.py",
            "fts_live_adapter_stub.py",
        ]
        absorbed_wrappers = [
            ("yahoo_csv_to_sql.py", "已被 fts_fundamentals_etl_mainline.py 吸收；先保留相容入口"),
            ("model_governance.py", "已被 fts_training_governance_mainline.py 吸收；先保留治理函式"),
            ("ml_trainer.py", "已被 fts_trainer_backend.py 吸收；先保留舊執行入口"),
        ]

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "must_keep": [{"file": x, "exists": (PATHS.base_dir / x).exists(), "reason": "主線 or 核心 contract / 執行 / 風控 / ETL"} for x in must_keep],
            "mergeable": [{"file": x, "exists": (PATHS.base_dir / x).exists(), "reason": "偏治理/報表/規劃，可以再收斂"} for x in mergeable],
            "hold_do_not_touch_yet": [{"file": x, "exists": (PATHS.base_dir / x).exists(), "reason": "仍可能被 legacy 主線或資料流依賴"} for x in hold],
            "deletable_after_smoketest": [{"file": x, "exists": (PATHS.base_dir / x).exists(), "reason": "v83 冒煙測試通過後可封存或刪除"} for x in deletable_after_smoketest],
            "absorbed_keep_wrapper": [{"file": x, "exists": (PATHS.base_dir / x).exists(), "reason": reason} for x, reason in absorbed_wrappers],
            "status": "classification_ready_v83",
        }
        self.json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        md = [
            f"# File Classification ({now_str()})",
            "",
            "## 一定要留",
        ]
        for row in payload["must_keep"]:
            md.append(f"- {row['file']}：{row['reason']}")
        md.append("")
        md.append("## 可合併")
        for row in payload["mergeable"]:
            md.append(f"- {row['file']}：{row['reason']}")
        md.append("")
        md.append("## 先別動")
        for row in payload["hold_do_not_touch_yet"]:
            md.append(f"- {row['file']}：{row['reason']}")
        md.append("")
        md.append("## 已合併進主線，但先保留相容入口")
        for row in payload["absorbed_keep_wrapper"]:
            md.append(f"- {row['file']}：{row['reason']}")
        md.append("")
        md.append("## 可刪除（先跑完 v83 smoke test）")
        for row in payload["deletable_after_smoketest"]:
            md.append(f"- {row['file']}：{row['reason']}")
        self.md_path.write_text("\n".join(md), encoding="utf-8")
        log(f"🗂️ 已輸出 file classification：{self.json_path}")
        return self.json_path, payload
