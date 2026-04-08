# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class StageTraceWriter:
    def __init__(self):
        self.path = PATHS.runtime_dir / "stage_trace.json"

    def build(self, ai_exec: dict, compat_info: dict, readiness: dict, accepted_count: int, rejected_count: int, execution_result: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "trace": [
                {
                    "stage": "ai_training",
                    "enabled": ai_exec.get("ai_stage_enabled", False),
                    "dry_run": ai_exec.get("dry_run", True),
                    "executed_count": len(ai_exec.get("executed", [])),
                    "skipped_count": len(ai_exec.get("skipped", [])),
                    "failed_count": len(ai_exec.get("failed", [])),
                },
                {
                    "stage": "decision_normalize",
                    "row_count": compat_info.get("row_count", 0),
                    "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                    "rows_with_action": compat_info.get("rows_with_action", 0),
                    "rows_with_price": compat_info.get("rows_with_price", 0),
                },
                {
                    "stage": "signal_readiness",
                    "execution_ready": readiness.get("execution_ready", False),
                    "total_signals": readiness.get("total_signals", 0),
                },
                {
                    "stage": "risk_filter",
                    "accepted_count": accepted_count,
                    "rejected_count": rejected_count,
                },
                {
                    "stage": "execution",
                    "submitted": execution_result.get("submitted", 0),
                    "filled": execution_result.get("filled", 0),
                    "partially_filled": execution_result.get("partially_filled", 0),
                    "rejected": execution_result.get("rejected", 0),
                    "cancelled": execution_result.get("cancelled", 0),
                    "auto_exit_signals": execution_result.get("auto_exit_signals", 0),
                }
            ]
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧭 已輸出 stage trace：{self.path}")
        return self.path, payload
