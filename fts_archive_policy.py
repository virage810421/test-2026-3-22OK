# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ArchivePolicyBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "archive_policy.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "keep_now": [
                "formal_trading_system_v54.py",
                "formal_trading_system_v55.py",
                "daily_chip_etl.py",
                "monthly_revenue_simple.py",
                "yahoo_csv_to_sql.py",
                "advanced_chart.py",
                "ml_data_generator.py",
                "ml_trainer.py",
                "model_governance.py",
            ],
            "archive_first": [
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
                "formal_trading_system_v53.py",
            ],
            "policy": [
                "先 archive，再觀察 1~2 個版本週期",
                "不要先永久刪除 legacy engines",
                "只刪已完全被替換且無 bridge 依賴的檔案"
            ],
            "status": "archive_policy_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📦 已輸出 archive policy：{self.path}")
        return self.path, payload
