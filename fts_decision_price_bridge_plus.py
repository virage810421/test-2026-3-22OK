# -*- coding: utf-8 -*-
from __future__ import annotations

import json

import pandas as pd

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log, safe_float
from fts_price_snapshot_auto_builder import AutoPriceSnapshotBuilder
from fts_decision_desk_builder import DecisionDeskBuilder


class DecisionPriceBridgePlus:
    def __init__(self):
        self.report_path = PATHS.runtime_dir / 'decision_price_bridge_plus.json'
        self.template_path = PATHS.data_dir / 'last_price_snapshot_template.csv'
        self.enriched_path = PATHS.data_dir / 'normalized_decision_output_enriched.csv'
        self.auto_price = AutoPriceSnapshotBuilder()

    def build(self):
        norm = PATHS.data_dir / 'normalized_decision_output.csv'
        if not norm.exists() or norm.stat().st_size == 0:
            DecisionDeskBuilder().build_decision_desk()
        if not norm.exists() or norm.stat().st_size == 0:
            template = pd.DataFrame([{'Ticker': '2330.TW', 'Reference_Price': ''}])
            template.to_csv(self.template_path, index=False, encoding='utf-8-sig')
            payload = {
                'generated_at': now_str(),
                'system_name': CONFIG.system_name,
                'status': 'normalized_decision_missing',
                'action': 'prepare_price_template_only',
                'template_path': str(self.template_path),
            }
            self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.report_path, payload

        df = pd.read_csv(norm, encoding='utf-8-sig')
        if df.empty:
            payload = {'generated_at': now_str(), 'system_name': CONFIG.system_name, 'status': 'normalized_decision_empty'}
            self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.report_path, payload

        tickers = df.get('Ticker', pd.Series(dtype=str)).astype(str).tolist()
        snapshot_path, snapshot_payload, snapshot_map = self.auto_price.build(tickers)
        before_ok = int((pd.to_numeric(df.get('Reference_Price', pd.Series(dtype=float)), errors='coerce').fillna(0) > 0).sum()) if 'Reference_Price' in df.columns else 0
        if 'Reference_Price' not in df.columns:
            df['Reference_Price'] = 0.0
        df['Reference_Price'] = pd.to_numeric(df['Reference_Price'], errors='coerce').fillna(0.0)
        for idx, row in df.iterrows():
            if float(row.get('Reference_Price', 0.0) or 0.0) <= 0:
                df.at[idx, 'Reference_Price'] = safe_float(snapshot_map.get(str(row.get('Ticker', '')).strip(), 0.0), 0.0)
        after_ok = int((pd.to_numeric(df.get('Reference_Price', pd.Series(dtype=float)), errors='coerce').fillna(0) > 0).sum())
        df.to_csv(self.enriched_path, index=False, encoding='utf-8-sig')
        missing = df.loc[pd.to_numeric(df['Reference_Price'], errors='coerce').fillna(0) <= 0, 'Ticker'].astype(str).drop_duplicates().tolist()[:100]
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'status': ('bridge_ready' if after_ok >= len(df) and len(df) > 0 else ('bridge_partial' if after_ok > 0 else 'waiting_for_price_sources')), 
            'price_snapshot_source': str(snapshot_path),
            'auto_price_status': snapshot_payload.get('status'),
            'rows_total': int(len(df)),
            'rows_with_price_before': before_ok,
            'rows_with_price_after': after_ok,
            'rows_still_missing_price': max(0, int(len(df) - after_ok)),
            'missing_price_tickers_preview': missing,
            'template_path': str(self.template_path),
            'enriched_output_path': str(self.enriched_path),
            'usage': [
                '現在會先自動讀 last_price_snapshot / kline_cache / yfinance',
                '若仍缺價，再看手動 override CSV',
                'control tower 會優先讀 enriched output'
            ]
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧩 decision price bridge ready: {self.report_path}')
        return self.report_path, payload
