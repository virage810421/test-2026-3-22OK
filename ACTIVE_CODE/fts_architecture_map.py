# -*- coding: utf-8 -*-
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

@dataclass
class ArchitectureMap:
    etl_entry: str = "daily_chip_etl.py / monthly_revenue_simple.py / yahoo_csv_to_sql.py"
    ai_entry: str = "ml_data_generator.py / ml_trainer.py / model_governance.py"
    research_entry: str = "你的 research / scoring / decision builder 主程式"
    decision_file: str = "daily_decision_desk.csv"
    execution_entry: str = "formal_trading_system_v19.py"
    state_store: str = "state/engine_state.json"
    audit_trail: str = "runtime/audit_events.jsonl"
    runtime_heartbeat: str = "runtime/heartbeat.json"
    notes: str = "v19 重點是把新主控明確掛回你的原始上游架構，而不是取代它。"

class ArchitectureMapWriter:
    def __init__(self):
        self.path = PATHS.runtime_dir / "architecture_map.json"

    def write(self):
        amap = ArchitectureMap()
        payload = asdict(amap)
        payload["generated_at"] = now_str()
        payload["system_name"] = CONFIG.system_name
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🗺️ 已輸出 architecture map：{self.path}")
        return self.path
