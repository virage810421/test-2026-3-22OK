# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class MainlineAbsorptionReport:
    """將 legacy 檔案收編狀態輸出成報告。"""

    MODULE_VERSION = "v83_mainline_absorption_report"

    def __init__(self):
        self.path = PATHS.runtime_dir / "mainline_absorption_report.json"

    def build(self):
        batch1 = [
            {
                "legacy": "daily_chip_etl.py",
                "mainline": "fts_etl_daily_chip_service.py",
                "status": "absorbed_wrapper_ready",
                "method": "抽離 ETL/排程引擎；舊檔改成 wrapper",
            },
            {
                "legacy": "monthly_revenue_simple.py",
                "mainline": "fts_etl_monthly_revenue_service.py",
                "status": "absorbed_wrapper_ready",
                "method": "抽離月營收抓取/清洗/SQL 寫入；舊檔改成 wrapper",
            },
            {
                "legacy": "ml_data_generator.py",
                "mainline": "fts_training_data_builder.py",
                "status": "absorbed_wrapper_ready",
                "method": "抽離訓練資料 builder；舊檔改成 wrapper",
            },
        ]
        batch2 = [
            {
                "legacy": "advanced_chart.py",
                "mainline": "fts_chart_service.py",
                "status": "absorbed_wrapper_ready",
                "method": "抽離圖表渲染服務；舊檔改成 wrapper",
            }
        ]
        batch3 = [
            {
                "legacy": "ml_trainer.py",
                "mainline": "fts_trainer_backend.py",
                "status": "wrapper_kept_mainline_switched",
                "method": "保留舊入口，主線改走 trainer backend",
            },
            {
                "legacy": "model_governance.py",
                "mainline": "fts_training_governance_mainline.py",
                "status": "core_service_retained",
                "method": "保留治理核心服務，由 mainline 統一調度",
            },
        ]
        payload: dict[str, Any] = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "system_name": CONFIG.system_name,
            "meaning_of_keep_old_name": "舊檔名與舊啟動方式仍可用，但真正工作邏輯已搬到新 service / backend。",
            "batch_completion": {
                "batch1_core_extraction": "complete",
                "batch2_chart_service_refactor": "complete",
                "batch3_call_path_switch": "complete",
            },
            "batch1": batch1,
            "batch2": batch2,
            "batch3": batch3,
            "status": "mainline_absorption_ready",
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"🧩 已輸出 mainline absorption report：{self.path}")
        return self.path, payload


def main() -> int:
    MainlineAbsorptionReport().build()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
