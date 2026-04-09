# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartArtifactSummaryBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_artifact_summary.json"

    def build(self, chart_bridge_summary: dict, chart_quality_gate: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "bridge_mode": chart_bridge_summary.get("bridge_summary", {}).get("bridge_mode", ""),
                "decision_rows": chart_bridge_summary.get("bridge_summary", {}).get("decision_rows", 0),
                "rows_with_ticker": chart_bridge_summary.get("bridge_summary", {}).get("rows_with_ticker", 0),
                "signal_count": chart_bridge_summary.get("bridge_summary", {}).get("signal_count", 0),
                "go_for_chart_linkage": chart_quality_gate.get("go_for_chart_linkage", False),
                "failure_count": len(chart_quality_gate.get("failures", [])),
                "warning_count": len(chart_quality_gate.get("warnings", [])),
            },
            "status": "summary_only"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧾 已輸出 chart artifact summary：{self.path}")
        return self.path, payload
