# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


def now_str() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


class FundamentalsTrueBackfill:
    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / 'data'
        self.runtime_dir = self.base_dir / 'runtime'
        self.seed_dir = Path(__file__).resolve().parent / 'seed_data'
        self.report_path = self.runtime_dir / 'fundamentals_true_backfill.json'

    def _copy_if_small_or_missing(self, seed: Path, target: Path, min_bytes: int = 1024) -> dict[str, Any]:
        target.parent.mkdir(parents=True, exist_ok=True)
        before = target.stat().st_size if target.exists() else 0
        action = 'skipped'
        if seed.exists() and (not target.exists() or before < min_bytes):
            shutil.copy2(seed, target)
            action = 'seed_copied'
        after = target.stat().st_size if target.exists() else 0
        return {'seed': str(seed), 'target': str(target), 'before_size': before, 'after_size': after, 'action': action}

    def _check_staleness(self, path: Path, preferred_date_cols: list[str], max_days: int) -> dict[str, Any]:
        if not path.exists():
            return {'exists': False, 'stale': True, 'latest_date': None, 'rows': 0}
        try:
            df = pd.read_csv(path)
        except Exception:
            return {'exists': True, 'stale': True, 'latest_date': None, 'rows': 0, 'read_error': True}
        latest_date = None
        for c in preferred_date_cols:
            if c in df.columns:
                vals = pd.to_datetime(df[c], errors='coerce').dropna()
                if len(vals):
                    latest_date = vals.max().to_pydatetime()
                    break
        stale = True
        if latest_date is not None:
            stale = (datetime.now() - latest_date) > timedelta(days=max_days)
        return {'exists': True, 'stale': stale, 'latest_date': latest_date.strftime('%Y-%m-%d') if latest_date else None, 'rows': int(len(df))}

    def _dedupe_file(self, path: Path, key_cols: list[str]) -> dict[str, Any]:
        if not path.exists():
            return {'path': str(path), 'deduped': False, 'rows_before': 0, 'rows_after': 0}
        try:
            df = pd.read_csv(path)
        except Exception:
            return {'path': str(path), 'deduped': False, 'rows_before': 0, 'rows_after': 0, 'read_error': True}
        rows_before = len(df)
        usable = [c for c in key_cols if c in df.columns]
        if usable:
            df = df.drop_duplicates(subset=usable, keep='last')
        rows_after = len(df)
        if rows_after != rows_before:
            df.to_csv(path, index=False, encoding='utf-8-sig')
        return {'path': str(path), 'deduped': rows_after != rows_before, 'rows_before': rows_before, 'rows_after': rows_after, 'keys': usable}

    def run(self) -> tuple[Path, dict[str, Any]]:
        actions = [
            self._copy_if_small_or_missing(self.seed_dir / 'market_financials_backup_fullspeed.csv', self.data_dir / 'market_financials_backup_fullspeed.csv', min_bytes=5000),
            self._copy_if_small_or_missing(self.seed_dir / 'latest_monthly_revenue_with_industry.csv', self.data_dir / 'latest_monthly_revenue_with_industry.csv', min_bytes=1000),
            self._copy_if_small_or_missing(self.seed_dir / 'stock_revenue_industry_tw.csv', self.data_dir / 'stock_revenue_industry_tw.csv', min_bytes=1000),
        ]
        staleness = {
            'fundamentals': self._check_staleness(self.data_dir / 'market_financials_backup_fullspeed.csv', ['資料年月日', 'Date', 'date'], 180),
            'monthly_revenue': self._check_staleness(self.data_dir / 'latest_monthly_revenue_with_industry.csv', ['資料年月日', 'date', 'Date'], 45),
        }
        dedupe = [
            self._dedupe_file(self.data_dir / 'market_financials_backup_fullspeed.csv', ['Ticker SYMBOL', '資料年月日']),
            self._dedupe_file(self.data_dir / 'latest_monthly_revenue_with_industry.csv', ['Ticker SYMBOL', '資料年月日']),
        ]
        merged_target = self.data_dir / 'fundamentals_seed_inventory.json'
        merged_payload = {'generated_at': now_str(), 'seed_inventory': actions, 'staleness': staleness, 'dedupe': dedupe}
        merged_target.write_text(json.dumps(merged_payload, ensure_ascii=False, indent=2), encoding='utf-8')
        payload = {
            'generated_at': now_str(),
            'status': 'fundamentals_true_backfill_applied',
            'actions': actions,
            'staleness': staleness,
            'dedupe': dedupe,
            'inventory_path': str(merged_target),
            'recommendation': '若 stale=True 或 API 多日缺資料，請再執行 fundamentals ETL smart_sync 增量補抓。',
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.report_path, payload
