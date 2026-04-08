# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchQualityGate:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_quality_gate.json"

    def evaluate(self, compat_info: dict, readiness: dict):
        failures = []
        warnings = []

        row_count = compat_info.get("row_count", 0)
        rows_with_ticker = compat_info.get("rows_with_ticker", 0)
        rows_with_action = compat_info.get("rows_with_action", 0)
        rows_with_price = compat_info.get("rows_with_price", 0)
        total_signals = readiness.get("total_signals", 0)

        if row_count == 0:
            failures.append({
                "type": "empty_research_output",
                "message": "研究/選股輸出在 normalize 後為空"
            })

        if rows_with_ticker == 0:
            failures.append({
                "type": "ticker_missing",
                "message": "研究/選股輸出缺少可用 ticker"
            })

        if rows_with_action == 0:
            failures.append({
                "type": "action_missing",
                "message": "研究/選股輸出缺少可用 action"
            })

        if rows_with_price == 0:
            failures.append({
                "type": "price_missing",
                "message": "研究/選股輸出缺少可用 reference price"
            })

        if row_count > 0 and total_signals == 0:
            warnings.append({
                "type": "zero_signal_after_research",
                "message": "研究/選股有輸出，但轉成有效訊號後為 0"
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "go_for_decision_linkage": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
            "summary": {
                "row_count": row_count,
                "rows_with_ticker": rows_with_ticker,
                "rows_with_action": rows_with_action,
                "rows_with_price": rows_with_price,
                "total_signals": total_signals,
                "failure_count": len(failures),
                "warning_count": len(warnings),
            }
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(
            f"🔎 Research Quality Gate | go_for_decision_linkage={payload['go_for_decision_linkage']} | "
            f"failures={len(failures)} | warnings={len(warnings)}"
        )
        return self.path, payload
