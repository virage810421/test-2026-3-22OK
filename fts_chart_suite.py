# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 7 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_chart_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartArtifactRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_artifact_registry.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "artifact_policy": {
                "source_mode": "service_primary_with_external_facade",
                "expected_artifacts": [
                    "advanced_chart output",
                    "research chart png/html artifact",
                    "decision-related chart summary"
                ],
                "governance": {
                    "registry_enabled": True,
                    "contract_defined": True,
                    "status": "governed_not_fully_replatformed"
                }
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🖼️ 已輸出 chart artifact registry：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_chart_suite.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_chart_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartBridgeRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_bridge_registry.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "charting": {
                "current_mode": "service_facade_only",
                "rendering_source": "正式主線已改走 fts_chart_service，舊檔名僅保留外部相容入口",
                "status": "legacy_facade_decommissioning",
                "notes": [
                    "目前畫圖/圖表多數仍沿用舊版模組輸出",
                    "新主控已可把 research/decision/execution 納回流程，但尚未完全重寫成新圖表引擎",
                    "之後可再升級成 chart contract / chart registry / chart artifact governance"
                ]
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🖼️ 已輸出 chart bridge registry：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_chart_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartOutputContract:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_output_contract.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "required_fields": [
                "ticker",
                "chart_type",
                "artifact_path_or_id",
            ],
            "optional_fields": [
                "timeframe",
                "signal_context",
                "strategy_name",
                "notes",
            ],
            "status": "defined",
            "notes": "v40 先把圖表輸出契約定義好，後續可再把舊圖表模組逐步遷移到此契約。"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📊 已輸出 chart output contract：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_chart_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartDecisionBridgeSummary:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_decision_bridge_summary.json"

    def build(self, compat_info: dict, readiness: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "bridge_summary": {
                "decision_rows": compat_info.get("row_count", 0),
                "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                "signal_count": readiness.get("total_signals", 0),
                "bridge_mode": "decision_to_chart_context_registered",
                "status": "summary_only"
            },
            "notes": [
                "目前圖表與 decision 仍多透過舊模組間接連接",
                "v40 開始把 decision 與 chart 的橋接資訊正式輸出成摘要"
            ]
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🌉 已輸出 chart decision bridge summary：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_chart_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ChartInterfaceAudit:
    def __init__(self):
        self.path = PATHS.runtime_dir / "chart_interface_audit.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "aligned": {
                "research_to_chart_bridge": True,
                "decision_to_chart_context": True,
                "chart_artifact_registry": True,
                "chart_output_contract": True,
            },
            "partial_or_not_fully_governed": {
                "legacy_chart_renderer_runtime_binding": "partial",
                "chart_artifact_versioning": "partial",
            },
            "not_aligned_yet": [
                "舊圖表引擎尚未完全替換成純新框架 rendering",
                "chart artifact 版本化與淘汰策略尚未完整",
            ]
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧩 已輸出 chart interface audit：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_chart_suite.py
# ==============================================================================
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
