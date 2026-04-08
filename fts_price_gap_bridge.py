# -*- coding: utf-8 -*-
import json
from pathlib import Path
from typing import Tuple, List, Dict
import pandas as pd
from fts_config import PATHS, CONFIG
from fts_utils import now_str, safe_float


class PriceGapBridge:
    MODULE_VERSION = 'v79'

    def __init__(self):
        self.report_path = PATHS.runtime_dir / 'price_gap_bridge.json'
        self.candidates_path = PATHS.data_dir / 'auto_price_snapshot_candidates.csv'
        self.template_path = PATHS.data_dir / 'manual_price_snapshot_template.csv'
        self.override_path = PATHS.data_dir / 'manual_price_snapshot_overrides.csv'

    def _discover_csvs(self) -> List[Path]:
        patterns = ('*price*.csv', '*quote*.csv', '*close*.csv', '*ohlcv*.csv', '*kline*.csv', '*history*.csv', '*snapshot*.csv')
        found = []
        max_depth = max(int(getattr(CONFIG, 'scan_recursive_depth', 3)), 1)
        for d in PATHS.price_scan_dirs:
            if not d.exists():
                continue
            for pat in patterns:
                try:
                    found.extend(d.glob(pat))
                    for sub in d.rglob(pat):
                        rel_parts = len(sub.relative_to(d).parts)
                        if rel_parts <= max_depth + 1:
                            found.append(sub)
                except Exception:
                    continue
        unique = []
        seen = set()
        for p in found:
            try:
                rp = str(p.resolve())
            except Exception:
                rp = str(p)
            if rp not in seen and p.is_file():
                seen.add(rp)
                unique.append(p)
        return unique[:500]

    def _normalize_ticker(self, x: str) -> str:
        s = str(x).strip().upper().replace(' ', '')
        if not s:
            return ''
        if s.endswith('.TW') or s.endswith('.TWO'):
            return s
        digits = ''.join(ch for ch in s if ch.isdigit())
        if len(digits) == 4:
            return f'{digits}.TW'
        return s

    def _extract(self, p: Path) -> pd.DataFrame:
        for enc in ('utf-8-sig', 'utf-8', None):
            try:
                df = pd.read_csv(p, encoding=enc) if enc else pd.read_csv(p)
                break
            except Exception:
                df = None
        if df is None or df.empty:
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        ticker_candidates = [c for c in df.columns if c.lower() in ('ticker','symbol','ticker symbol','stock_id') or '代號' in c or '股票' in c]
        price_candidates = [c for c in df.columns if c.lower() in ('reference_price','close','adj close','price','last','last_price') or '收盤' in c or '價格' in c]
        date_candidates = [c for c in df.columns if c.lower() in ('date','datetime','timestamp','trading_date') or '日期' in c]
        if not ticker_candidates or not price_candidates:
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        tcol = ticker_candidates[0]
        pcol = price_candidates[0]
        work = df[[tcol, pcol] + date_candidates[:1]].copy()
        ren = {tcol: 'Ticker', pcol: 'Reference_Price'}
        if date_candidates:
            ren[date_candidates[0]] = 'Source_Date'
        work = work.rename(columns=ren)
        work['Ticker'] = work['Ticker'].map(self._normalize_ticker)
        work['Reference_Price'] = work['Reference_Price'].map(lambda x: safe_float(x, 0.0))
        work = work[(work['Ticker'] != '') & (work['Reference_Price'] > 0)]
        if work.empty:
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        if 'Source_Date' in work.columns:
            work['Source_Date'] = pd.to_datetime(work['Source_Date'], errors='coerce')
            work = work.sort_values(['Ticker', 'Source_Date'], ascending=[True, False])
        work = work.drop_duplicates(['Ticker'], keep='first')
        work['Source'] = str(p)
        return work[['Ticker','Reference_Price','Source']]

    def _load_manual_overrides(self) -> pd.DataFrame:
        if not self.override_path.exists():
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        try:
            df = pd.read_csv(self.override_path, encoding='utf-8-sig')
        except Exception:
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        if 'Ticker' not in df.columns or 'Reference_Price' not in df.columns:
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        df = df[['Ticker','Reference_Price']].copy()
        df['Ticker'] = df['Ticker'].map(self._normalize_ticker)
        df['Reference_Price'] = df['Reference_Price'].map(lambda x: safe_float(x,0.0))
        df = df[df['Reference_Price'] > 0]
        df['Source'] = 'manual_override'
        return df

    def _extract_from_decision(self, required_tickers: List[str]) -> pd.DataFrame:
        candidates = [PATHS.base_dir / 'daily_decision_desk.csv', PATHS.data_dir / 'normalized_decision_output.csv']
        frames = []
        for p in candidates:
            if not p.exists():
                continue
            try:
                df = pd.read_csv(p, encoding='utf-8-sig')
            except Exception:
                continue
            ticker_col = 'Ticker' if 'Ticker' in df.columns else None
            if not ticker_col:
                continue
            price_col = next((c for c in df.columns if c in ('Reference_Price','Close','Latest_Close','收盤價','最新收盤價')), None)
            if not price_col:
                continue
            out = df[[ticker_col, price_col]].copy()
            out.columns = ['Ticker','Reference_Price']
            out['Ticker'] = out['Ticker'].map(self._normalize_ticker)
            out['Reference_Price'] = out['Reference_Price'].map(lambda x: safe_float(x,0.0))
            out = out[(out['Ticker'].isin([self._normalize_ticker(x) for x in required_tickers])) & (out['Reference_Price']>0)]
            if not out.empty:
                out['Source'] = f'decision:{p.name}'
                frames.append(out)
        if not frames:
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        return pd.concat(frames, ignore_index=True).drop_duplicates(['Ticker'], keep='first')

    def _refresh_manual_files(self, required_norm, missing):
        missing_out = missing[['Ticker']].copy()
        missing_out['Reference_Price'] = ''
        missing_out.to_csv(self.template_path, index=False, encoding='utf-8-sig')
        if self.override_path.exists():
            try:
                existing = pd.read_csv(self.override_path, encoding='utf-8-sig')
            except Exception:
                existing = pd.DataFrame(columns=['Ticker','Reference_Price'])
        else:
            existing = pd.DataFrame(columns=['Ticker','Reference_Price'])
        if 'Ticker' not in existing.columns:
            existing['Ticker'] = []
        if 'Reference_Price' not in existing.columns:
            existing['Reference_Price'] = []
        existing['Ticker'] = existing['Ticker'].astype(str).map(self._normalize_ticker)
        existing = existing[['Ticker','Reference_Price']]
        required_df = pd.DataFrame({'Ticker': sorted(set(required_norm))})
        merged = required_df.merge(existing, on='Ticker', how='left')
        merged.to_csv(self.override_path, index=False, encoding='utf-8-sig')
        return int(merged['Reference_Price'].notna().sum())

    def build(self, required_tickers: List[str]) -> Tuple[Path, Dict]:
        required_norm = [self._normalize_ticker(x) for x in required_tickers]
        frames = [self._load_manual_overrides(), self._extract_from_decision(required_norm)]
        scanned = self._discover_csvs()
        scanned_success = 0
        for p in scanned:
            ext = self._extract(p)
            if not ext.empty:
                scanned_success += 1
                frames.append(ext)
        frames = [x for x in frames if not x.empty]
        if frames:
            cat = pd.concat(frames, ignore_index=True)
            cat = cat.drop_duplicates(['Ticker'], keep='first')
        else:
            cat = pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        cat.to_csv(self.candidates_path, index=False, encoding='utf-8-sig')
        req = pd.DataFrame({'Ticker': sorted(set(required_norm))})
        merged = req.merge(cat[['Ticker','Reference_Price']], on='Ticker', how='left')
        missing = merged[merged['Reference_Price'].isna() | (merged['Reference_Price']<=0)].copy()
        prefilled_override_rows = self._refresh_manual_files(required_norm, missing)
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'scanned_csv_count': len(scanned),
            'scanned_csv_success_count': scanned_success,
            'candidate_rows': int(len(cat)),
            'required_tickers': int(len(req)),
            'matched_tickers': int((pd.to_numeric(merged['Reference_Price'], errors='coerce').fillna(0)>0).sum()),
            'missing_tickers': missing['Ticker'].tolist(),
            'price_scan_dirs': [str(x) for x in PATHS.price_scan_dirs],
            'source_mount_dirs': [str(x) for x in PATHS.source_mount_dirs],
            'manual_override_path': str(self.override_path),
            'manual_override_prefilled_rows': prefilled_override_rows,
            'candidates_path': str(self.candidates_path),
            'manual_template_path': str(self.template_path),
            'status': 'price_gap_reduced' if not cat.empty else 'waiting_for_price_sources',
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.candidates_path, payload
