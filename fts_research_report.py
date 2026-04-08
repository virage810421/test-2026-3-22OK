# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchDecisionReportBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_decision_report.json"

    def build(self, compat_info: dict, readiness: dict, research_gate: dict):
        report = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "research_rows": compat_info.get("row_count", 0),
                "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                "rows_with_action": compat_info.get("rows_with_action", 0),
                "rows_with_price": compat_info.get("rows_with_price", 0),
                "signal_count": readiness.get("total_signals", 0),
                "go_for_decision_linkage": research_gate.get("go_for_decision_linkage", False),
                "failure_count": len(research_gate.get("failures", [])),
                "warning_count": len(research_gate.get("warnings", [])),
            },
            "research_gate": research_gate,
            "compat_info": compat_info,
            "readiness": readiness,
            "interpretation": {
                "what_this_means": [
                    "研究/選股輸出是否足以接到 decision / execution",
                    "資料是否至少具備 ticker/action/price",
                    "是否發生 research 有輸出但 signal 轉換為 0 的情況"
                ],
                "next_focus": [
                    "若 failure_count > 0，先修 research 輸出欄位",
                    "若 warning_count > 0，優先檢查 scoring / action mapping / price 欄位"
                ]
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        log(f"📘 已輸出 research decision report：{self.path}")
        return self.path, report
