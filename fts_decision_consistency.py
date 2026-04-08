# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class DecisionConsistencyBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "decision_consistency.json"

    def build(self, compat_info: dict, readiness: dict):
        row_count = compat_info.get("row_count", 0)
        signal_count = readiness.get("total_signals", 0)
        rows_with_ticker = compat_info.get("rows_with_ticker", 0)
        rows_with_action = compat_info.get("rows_with_action", 0)
        rows_with_price = compat_info.get("rows_with_price", 0)

        issues = []
        if row_count > 0 and signal_count == 0:
            issues.append("decision_rows_exist_but_no_signals")
        if row_count > 0 and rows_with_ticker == 0:
            issues.append("rows_missing_ticker")
        if row_count > 0 and rows_with_action == 0:
            issues.append("rows_missing_action")
        if row_count > 0 and rows_with_price == 0:
            issues.append("rows_missing_price")

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "row_count": row_count,
                "signal_count": signal_count,
                "rows_with_ticker": rows_with_ticker,
                "rows_with_action": rows_with_action,
                "rows_with_price": rows_with_price,
                "issue_count": len(issues),
            },
            "issues": issues,
            "all_green": len(issues) == 0,
            "status": "consistency_check_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧷 已輸出 decision consistency：{self.path}")
        return self.path, payload
