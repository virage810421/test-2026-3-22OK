# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import traceback

from fts_config import PATHS, CONFIG
from fts_file_classification import FileClassificationBuilder
from fts_fundamentals_etl_mainline import FundamentalsETLMainline
from fts_training_governance_mainline import TrainingGovernanceMainline
from fts_phase1_upgrade import Phase1Upgrade
from fts_phase2_mock_broker_stage import Phase2MockBrokerStage
from fts_phase3_real_cutover_stage import Phase3RealCutoverStage
from fts_utils import now_str, log


class FormalTradingSystemV83OfficialMain:
    MODULE_VERSION = "v83_official_main"

    def __init__(self):
        self.path = PATHS.runtime_dir / "formal_trading_system_v83_official_main.json"

    def run(self):
        log("=" * 72)
        log("🚀 啟動 正式交易主控版_v83_official_main")
        log("🧭 收編內容：fundamentals ETL + training governance + 三階段交易升級")
        log("=" * 72)

        fundamentals_path, fundamentals = FundamentalsETLMainline().build_summary()
        training_path, training = TrainingGovernanceMainline().build_summary(execute_backend=False)
        phase1_path, phase1 = Phase1Upgrade().run()
        phase2_path, phase2 = Phase2MockBrokerStage().run()
        phase3_path, phase3 = Phase3RealCutoverStage().run()
        classification_path, classification = FileClassificationBuilder().build()

        completed = []
        if fundamentals.get("merge_status") == "merged_into_mainline":
            completed.append("Fundamentals ETL：已吸收 yahoo_csv_to_sql.py")
        if training.get("status") == "training_governance_mainline_ready":
            completed.append("Training Governance：已吸收 ml_trainer.py + model_governance.py")
        if phase1.get("status") == "phase1_ready":
            completed.append("Phase1：pre-live 補滿")
        if phase2.get("status") == "phase2_ready":
            completed.append("Phase2：假真券商補滿")
        if phase3.get("status") == "phase3_contract_ready_account_pending":
            completed.append("Phase3：真券商 contract ready，待開戶")

        payload = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "system_name": CONFIG.system_name,
            "mainline_outputs": {
                "fundamentals_etl_mainline": str(fundamentals_path),
                "training_governance_mainline": str(training_path),
                "phase1": str(phase1_path),
                "phase2": str(phase2_path),
                "phase3": str(phase3_path),
                "file_classification": str(classification_path),
            },
            "completed_upgrades": completed,
            "phase_completion": {
                "phase1": {
                    "status": phase1.get("status"),
                    "complete_for_scope": phase1.get("status") == "phase1_ready",
                    "scope_note": "AI / promotion / payload / safety / preflight 已補齊到 pre-live 等級",
                },
                "phase2": {
                    "status": phase2.get("status"),
                    "complete_for_scope": phase2.get("status") == "phase2_ready",
                    "scope_note": "mock broker / callback / reconciliation / preopen / EOD 已補齊到假真券商等級",
                },
                "phase3": {
                    "status": phase3.get("status"),
                    "complete_for_scope": False,
                    "scope_note": "真券商接口規格與 cutover skeleton 已完成；開戶前無法宣告完整升級",
                },
            },
            "status": "v83_official_main_ready",
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"✅ v83 正式交易主控版完成：{self.path}")
        return self.path, payload


def main() -> int:
    try:
        FormalTradingSystemV83OfficialMain().run()
        return 0
    except Exception as exc:
        err = {
            "generated_at": now_str(),
            "module_version": FormalTradingSystemV83OfficialMain.MODULE_VERSION,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
        (PATHS.runtime_dir / "formal_trading_system_v83_official_main_error.json").write_text(
            json.dumps(err, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log(f"❌ v83 正式交易主控版失敗：{exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
