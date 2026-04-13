# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyBridgeMapBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "service_detachment_map.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "categories": {
                "still_directly_used_legacy_code": [
                    {
                        "file": "daily_chip_etl.py",
                        "role": "ETL實抓 / CSV-SQL補寫 / 上游資料來源",
                        "status": "legacy_engine_still_used"
                    },
                    {
                        "file": "monthly_revenue_simple.py",
                        "role": "月營收 ETL / 上游資料來源",
                        "status": "legacy_engine_still_used"
                    },
                    {
                        "file": "yahoo_csv_to_sql.py",
                        "role": "財報/基本面 ETL 上游來源",
                        "status": "legacy_engine_still_used"
                    },
                    {
                        "file": "advanced_chart.py",
                        "role": "圖表輸出 / chart rendering",
                        "status": "legacy_renderer_still_used"
                    },
                    {
                        "file": "ml_data_generator.py",
                        "role": "訓練資料生成",
                        "status": "legacy_training_data_engine_still_used"
                    },
                    {
                        "file": "ml_trainer.py",
                        "role": "AI訓練主跑",
                        "status": "legacy_training_engine_still_used"
                    },
                    {
                        "file": "model_governance.py",
                        "role": "模型治理/選模輔助",
                        "status": "legacy_governance_engine_still_used"
                    }
                ],
                "new_mainline_governance_orchestrator": [
                    {
                        "file": "formal_trading_system_v55.py",
                        "role": "新主控 / 總司令部 / 治理整合",
                        "status": "current_main_entry"
                    }
                ],
                "new_skeletons_preparing_to_replace_legacy": [
                    {
                        "file": "fts_etl_quality.py / fts_etl_data_quality_plus.py",
                        "role": "ETL品質治理",
                        "status": "governance_ready_not_full_replacement"
                    },
                    {
                        "file": "fts_research_quality_stats.py / fts_research_versioning.py",
                        "role": "research治理與版本化",
                        "status": "governance_ready_not_full_replacement"
                    },
                    {
                        "file": "fts_live_adapter_stub.py / fts_broker_adapter_contract.py",
                        "role": "future broker replacement path",
                        "status": "stub_ready_not_live_replacement"
                    }
                ]
            },
            "summary": {
                "current_architecture": "new_orchestrator_plus_legacy_engines",
                "fully_single_core_yet": False,
                "migration_strategy": "先治理收口，再逐步替換 legacy engines"
            },
            "status": "bridge_map_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🗺️ 已輸出 legacy bridge map：{self.path}")
        return self.path, payload
