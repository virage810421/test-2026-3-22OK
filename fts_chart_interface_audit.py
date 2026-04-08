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
