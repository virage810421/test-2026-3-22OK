# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

from fts_config import PATHS
from fts_utils import now_str, log


class ABDiffAudit:
    MODULE_VERSION = 'v83_ab_diff_audit'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'ab_diff_audit.json'

    def build(self) -> tuple[Any, dict[str, Any]]:
        rows = [
            {
                'module': 'yahoo_csv_to_sql.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_fundamentals_etl_mainline.py',
                'note': '不要整支重吸收，只補 retry / smart sync / batch commit / checkpoint 細節',
            },
            {
                'module': 'daily_chip_etl.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_etl_daily_chip_service.py',
                'note': '只補抓取規則與補抓差異，不回退到舊腳本主控',
            },
            {
                'module': 'monthly_revenue_simple.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_etl_monthly_revenue_service.py',
                'note': '只補欄位映射與日期規則差異',
            },
            {
                'module': 'ml_data_generator.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_training_data_builder.py',
                'note': '只補特徵欄位與標籤規則差異',
            },
            {
                'module': 'advanced_chart.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_chart_service.py',
                'note': '只補圖層或參數差異，不把繪圖塞進主交易回路',
            },
            {
                'module': 'config.py',
                'state': 'manual_merge_values_only',
                'paired_mainline': 'fts_config.py',
                'note': '不要整支覆蓋，只人工搬參數值',
            },
        ]
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'audit_rows': rows,
            'status': 'diff_patch_plan_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧾 A/B diff audit ready: {self.runtime_path}')
        return self.runtime_path, payload
