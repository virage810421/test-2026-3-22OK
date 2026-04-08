# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartQualityGate:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_quality_gate.json"

    def evaluate(self, chart_bridge_summary: dict):
        failures = []
        warnings = []

        decision_rows = chart_bridge_summary.get("bridge_summary", {}).get("decision_rows", 0)
        rows_with_ticker = chart_bridge_summary.get("bridge_summary", {}).get("rows_with_ticker", 0)
        signal_count = chart_bridge_summary.get("bridge_summary", {}).get("signal_count", 0)

        if decision_rows == 0:
            failures.append({
                "type": "empty_decision_context",
                "message": "decision rows 為 0，無法建立有效 chart context"
            })

        if rows_with_ticker == 0:
            failures.append({
                "type": "ticker_missing_for_chart",
                "message": "缺少 ticker，chart 無法對準標的"
            })

        if decision_rows > 0 and signal_count == 0:
            warnings.append({
                "type": "no_signal_but_has_decision_rows",
                "message": "有 decision rows，但 signal count = 0"
            })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "go_for_chart_linkage": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
            "summary": {
                "decision_rows": decision_rows,
                "rows_with_ticker": rows_with_ticker,
                "signal_count": signal_count,
                "failure_count": len(failures),
                "warning_count": len(warnings),
            }
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🖼️ 已輸出 chart quality gate：{self.path}")
        return self.path, payload
