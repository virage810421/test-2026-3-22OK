# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from datetime import datetime


def now_str() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class FundamentalsTrueBackfill:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / 'data'
        self.runtime_dir = self.base_dir / 'runtime'
        self.seed_dir = Path(__file__).resolve().parent / 'seed_data'
        self.report_path = self.runtime_dir / 'fundamentals_true_backfill.json'

    def _copy_if_small_or_missing(self, seed: Path, target: Path, min_bytes: int = 1024) -> dict:
        target.parent.mkdir(parents=True, exist_ok=True)
        before = target.stat().st_size if target.exists() else 0
        action = 'skipped'
        if seed.exists() and (not target.exists() or before < min_bytes):
            shutil.copy2(seed, target)
            action = 'seed_copied'
        after = target.stat().st_size if target.exists() else 0
        return {
            'seed': str(seed),
            'target': str(target),
            'before_size': before,
            'after_size': after,
            'action': action,
        }

    def run(self) -> tuple[Path, dict]:
        actions = []
        actions.append(self._copy_if_small_or_missing(
            self.seed_dir / 'market_financials_backup_fullspeed.csv',
            self.data_dir / 'market_financials_backup_fullspeed.csv',
            min_bytes=5000,
        ))
        actions.append(self._copy_if_small_or_missing(
            self.seed_dir / 'latest_monthly_revenue_with_industry.csv',
            self.data_dir / 'latest_monthly_revenue_with_industry.csv',
            min_bytes=1000,
        ))
        actions.append(self._copy_if_small_or_missing(
            self.seed_dir / 'stock_revenue_industry_tw.csv',
            self.data_dir / 'stock_revenue_industry_tw.csv',
            min_bytes=1000,
        ))

        merged_target = self.data_dir / 'fundamentals_seed_inventory.json'
        merged_payload = {
            'generated_at': now_str(),
            'seed_inventory': actions,
        }
        merged_target.write_text(json.dumps(merged_payload, ensure_ascii=False, indent=2), encoding='utf-8')

        payload = {
            'generated_at': now_str(),
            'status': 'fundamentals_true_backfill_applied',
            'actions': actions,
            'inventory_path': str(merged_target),
            'recommendation': '若本機已安裝 yfinance / pyodbc，之後可再執行 fundamentals ETL 主線做增量補抓。',
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.report_path, payload
