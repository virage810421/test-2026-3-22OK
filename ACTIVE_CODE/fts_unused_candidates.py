# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class UnusedCandidateBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "unused_candidates.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "safe_to_archive_first": [
                "formal_trading_system_v40.py",
                "formal_trading_system_v41.py",
                "formal_trading_system_v42.py",
                "formal_trading_system_v43.py",
                "formal_trading_system_v44.py",
                "formal_trading_system_v45.py",
                "formal_trading_system_v46.py",
                "formal_trading_system_v47.py",
                "formal_trading_system_v48.py",
                "formal_trading_system_v49.py",
                "formal_trading_system_v50.py",
                "formal_trading_system_v51.py",
                "formal_trading_system_v52.py",
            ],
            "do_not_delete_yet": [
                "formal_trading_system_v53.py",
                "daily_chip_etl.py",
                "monthly_revenue_simple.py",
                "yahoo_csv_to_sql.py",
                "advanced_chart.py",
                "ml_data_generator.py",
                "ml_trainer.py",
                "model_governance.py",
            ],
            "notes": [
                "舊版主控檔大多可先搬到 archive/ 或 backup/，不建議直接永久刪除",
                "ETL / chart / AI 訓練腳本目前多半仍是被新主控橋接或依賴的來源",
            ],
            "status": "candidate_list_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧹 已輸出 unused candidates：{self.path}")
        return self.path, payload
