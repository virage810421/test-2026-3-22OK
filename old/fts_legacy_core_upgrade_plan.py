# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class LegacyCoreUpgradePlanBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "legacy_core_upgrade_plan.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "strategy": "parallel_upgrade",
            "principle": "邊升新主控，邊把舊核心本體升到95+，避免外層新、內核舊",
            "upgrade_targets": [
                {
                    "file": "daily_chip_etl.py",
                    "target": "95+",
                    "focus": ["ETL品質報表", "批次統計", "欄位完整率", "錯誤分類", "同步摘要"]
                },
                {
                    "file": "monthly_revenue_simple.py",
                    "target": "95+",
                    "focus": ["發布時窗治理", "CSV/SQL一致性", "來源失敗容錯", "欄位覆蓋率"]
                },
                {
                    "file": "yahoo_csv_to_sql.py",
                    "target": "95+",
                    "focus": ["fundamentals契約化", "欄位品質", "資料年月/年月日正規化", "上游失敗摘要"]
                },
                {
                    "file": "ml_data_generator.py",
                    "target": "95+",
                    "focus": ["特徵摘要", "缺值統計", "輸出版本化", "資料品質報表"]
                },
                {
                    "file": "ml_trainer.py",
                    "target": "95+",
                    "focus": ["訓練摘要", "artifact完整性", "validation輸出", "晉升前檢查"]
                },
                {
                    "file": "model_governance.py",
                    "target": "95+",
                    "focus": ["版本選模", "promotion policy", "rollback policy", "registry一致性"]
                },
                {
                    "file": "advanced_chart.py",
                    "target": "95+",
                    "focus": ["chart artifact metadata", "render contract", "輸出治理", "錯誤摘要"]
                }
            ],
            "status": "parallel_plan_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛠️ 已輸出 legacy core upgrade plan：{self.path}")
        return self.path, payload
