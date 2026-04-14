# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 4 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_research_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchQualityStatsBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_quality_stats.json"

    def build(self, compat_info: dict):
        row_count = compat_info.get("row_count", 0)
        rows_with_ticker = compat_info.get("rows_with_ticker", 0)
        rows_with_action = compat_info.get("rows_with_action", 0)
        rows_with_price = compat_info.get("rows_with_price", 0)

        def ratio(x):
            return round(x / row_count, 4) if row_count else 0

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "row_count": row_count,
            "stats": {
                "ticker_coverage": ratio(rows_with_ticker),
                "action_coverage": ratio(rows_with_action),
                "price_coverage": ratio(rows_with_price),
            },
            "status": "quality_stats_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🔬 已輸出 research quality stats：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_research_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class ResearchSelectionRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_selection_registry.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "research_selection_layer": {
                "role": "研究層/選股層候選輸出登錄，不直接覆蓋真倉",
                "current_status": "registered_and_isolated_from_live",
                "isolation_rules": [
                    "candidate params 與 approved params 分離",
                    "candidate features 不覆蓋 models/selected_features.pkl",
                    "alpha 候選需經 validation/OOT/promotion",
                    "研究模組不得直接寫 production config 或正式模型檔"
                ],
                "merged_old_modules": {
                    "research_only": [
                        "advanced_optimizer.py", "optimizer.py", "auto_optimizer.py",
                        "feature_selector.py", "alpha_miner.py"
                    ],
                    "serviceized_into_mainline": [
                        "market_language.py", "kline_cache.py", "param_storage.py"
                    ]
                },
                "artifact_root": str(PATHS.runtime_dir / 'research_lab'),
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔎 已輸出 research selection registry：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_research_suite.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_research_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ResearchVersioningBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "research_versioning.json"

    def build(self, compat_info: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "versioning": {
                "research_output_version": getattr(CONFIG, "package_version", "v53"),
                "decision_input_rows": compat_info.get("row_count", 0),
                "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                "rows_with_action": compat_info.get("rows_with_action", 0),
                "rows_with_price": compat_info.get("rows_with_price", 0),
            },
            "required_metadata": [
                "research_output_version",
                "generated_at",
                "decision_input_rows",
            ],
            "status": "versioning_defined"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🏷️ 已輸出 research versioning：{self.path}")
        return self.path, payload
