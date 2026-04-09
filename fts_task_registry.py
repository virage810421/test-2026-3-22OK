# -*- coding: utf-8 -*-
import json
from dataclasses import dataclass, asdict
from typing import List
from fts_config import PATHS
from fts_utils import now_str, log

@dataclass
class RegisteredTask:
    stage: str
    name: str
    script: str
    required: bool = False
    enabled: bool = True
    notes: str = ""

class TaskRegistry:
    def __init__(self):
        self.tasks: List[RegisteredTask] = [
            RegisteredTask("etl", "daily_chip_etl", "daily_chip_etl.py", required=True, notes="法人籌碼/日更資料"),
            RegisteredTask("etl", "monthly_revenue", "monthly_revenue_simple.py", required=False, notes="月營收"),
            RegisteredTask("etl", "fundamentals_import", "yahoo_csv_to_sql.py", required=False, notes="財報/基本面"),
            RegisteredTask("ai", "ml_data_generator", "ml_data_generator.py", required=False, notes="特徵資料集"),
            RegisteredTask("ai", "ml_trainer", "ml_trainer.py", required=False, notes="模型訓練"),
            RegisteredTask("ai", "model_governance", "model_governance.py", required=False, notes="模型治理"),
            RegisteredTask("decision", "decision_builder_csv", "daily_decision_desk.csv", required=True, notes="決策輸出檔"),
        ]
        self.path = PATHS.runtime_dir / "task_registry.json"

    def write(self):
        payload = {
            "generated_at": now_str(),
            "tasks": [asdict(t) for t in self.tasks],
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧩 已輸出 task registry：{self.path}")
        return self.path

    def summary(self):
        return [asdict(t) for t in self.tasks]
