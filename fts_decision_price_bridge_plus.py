# -*- coding: utf-8 -*-
import json
import pandas as pd
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log, safe_float

class DecisionPriceBridgePlus:
    def __init__(self):
        self.report_path = PATHS.runtime_dir / "decision_price_bridge_plus.json"
        self.template_path = PATHS.data_dir / "last_price_snapshot_template.csv"
        self.enriched_path = PATHS.data_dir / "normalized_decision_output_enriched.csv"

    def _load_price_snapshot(self):
        candidates = [
            PATHS.base_dir / "last_price_snapshot.csv",
            PATHS.base_dir / "daily_price_snapshot.csv",
            PATHS.data_dir / "last_price_snapshot.csv",
        ]
        for p in candidates:
            if p.exists():
                try:
                    df = pd.read_csv(p, encoding='utf-8-sig')
                    tcol = next((c for c in df.columns if c.lower() in ('ticker','ticker symbol','symbol')), None)
                    pcol = next((c for c in df.columns if c.lower() in ('close','price','reference_price') or '收盤' in c), None)
                    if tcol and pcol:
                        out = df[[tcol,pcol]].copy()
                        out.columns = ['Ticker','Reference_Price']
                        out['Ticker'] = out['Ticker'].astype(str).str.strip()
                        out['Reference_Price'] = out['Reference_Price'].apply(lambda x: safe_float(x,0.0))
                        return out[out['Reference_Price']>0], str(p)
                except Exception:
                    pass
        return pd.DataFrame(columns=['Ticker','Reference_Price']), ''

    def build(self):
        norm = PATHS.data_dir / 'normalized_decision_output.csv'
        if not norm.exists():
            template = pd.DataFrame([
                {'Ticker':'2330.TW','Reference_Price':950.0},
                {'Ticker':'2317.TW','Reference_Price':150.0},
            ])
            template.to_csv(self.template_path, index=False, encoding='utf-8-sig')
            payload = {
                'generated_at': now_str(),
                'system_name': CONFIG.system_name,
                'status': 'normalized_decision_missing',
                'action': 'prepare_price_template_only',
                'template_path': str(self.template_path),
            }
            with open(self.report_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            log(f"🧩 已輸出 decision price template：{self.template_path}")
            return self.report_path, payload

        df = pd.read_csv(norm, encoding='utf-8-sig')
        template = pd.DataFrame([
            {'Ticker':'2330.TW','Reference_Price':950.0},
            {'Ticker':'2317.TW','Reference_Price':150.0},
        ])
        template.to_csv(self.template_path, index=False, encoding='utf-8-sig')

        snap, snap_path = self._load_price_snapshot()
        before_ok = int((df.get('Reference_Price',0).fillna(0).astype(float) > 0).sum()) if 'Reference_Price' in df.columns else 0
        if not snap.empty:
            merged = df.merge(snap, on='Ticker', how='left', suffixes=('','_snap'))
            if 'Reference_Price_snap' in merged.columns:
                mask = merged['Reference_Price'].fillna(0).astype(float) <= 0
                merged.loc[mask, 'Reference_Price'] = merged.loc[mask, 'Reference_Price_snap'].fillna(0)
                merged = merged.drop(columns=['Reference_Price_snap'])
            df = merged
        after_ok = int((df.get('Reference_Price',0).fillna(0).astype(float) > 0).sum()) if 'Reference_Price' in df.columns else 0
        df.to_csv(self.enriched_path, index=False, encoding='utf-8-sig')
        missing = []
        if 'Reference_Price' in df.columns:
            bad = df[df['Reference_Price'].fillna(0).astype(float) <= 0]
            missing = bad['Ticker'].astype(str).drop_duplicates().tolist()[:100]

        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'status': 'bridge_ready' if after_ok > before_ok else 'template_ready_waiting_for_prices',
            'price_snapshot_source': snap_path,
            'rows_total': int(len(df)),
            'rows_with_price_before': before_ok,
            'rows_with_price_after': after_ok,
            'rows_still_missing_price': max(0, int(len(df) - after_ok)),
            'missing_price_tickers_preview': missing,
            'template_path': str(self.template_path),
            'enriched_output_path': str(self.enriched_path),
            'usage': [
                '把最新收盤價整理成 last_price_snapshot.csv',
                '欄位至少要有 Ticker 與 Reference_Price 或 Close',
                '重新啟動主控後即可把 decision 補成可執行格式'
            ]
        }
        with open(self.report_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧩 已輸出 decision price bridge plus：{self.report_path}")
        return self.report_path, payload
