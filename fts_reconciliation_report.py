# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ReconciliationSummaryBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "reconciliation_summary.json"

    def build(self, execution_result: dict, accepted_count: int, rejected_count: int):
        summary = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "accepted_count": accepted_count,
            "risk_rejected_count": rejected_count,
            "submitted": execution_result.get("submitted", 0),
            "filled": execution_result.get("filled", 0),
            "partially_filled": execution_result.get("partially_filled", 0),
            "broker_rejected": execution_result.get("rejected", 0),
            "cancelled": execution_result.get("cancelled", 0),
            "auto_exit_signals": execution_result.get("auto_exit_signals", 0),
            "fills_count": execution_result.get("fills_count", 0),
            "status": "summary_only"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        log(f"🧮 已輸出 reconciliation summary：{self.path}")
        return self.path, summary
