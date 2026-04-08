# -*- coding: utf-8 -*-
import json
from pathlib import Path
from typing import Tuple, List, Dict
import pandas as pd
import pyodbc
from fts_config import PATHS, CONFIG
from fts_utils import now_str, safe_float


class PriceGapBridge:
    MODULE_VERSION = 'v79'

    def __init__(self):
        self.report_path = PATHS.runtime_dir / 'price_gap_bridge.json'
        self.candidates_path = PATHS.data_dir / 'auto_price_snapshot_candidates.csv'
        self.template_path = PATHS.data_dir / 'manual_price_snapshot_template.csv'
        self.override_path = PATHS.data_dir / 'manual_price_snapshot_overrides.csv'
        self.last_snapshot_path = PATHS.data_dir / 'last_price_snapshot.csv'

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



    def _load_last_snapshot(self) -> pd.DataFrame:
        if not self.last_snapshot_path.exists():
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        try:
            df = pd.read_csv(self.last_snapshot_path, encoding='utf-8-sig')
        except Exception:
            try:
                df = pd.read_csv(self.last_snapshot_path)
            except Exception:
                return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        tcol = 'Ticker' if 'Ticker' in df.columns else ('Ticker SYMBOL' if 'Ticker SYMBOL' in df.columns else None)
        pcol = 'Reference_Price' if 'Reference_Price' in df.columns else ('Close' if 'Close' in df.columns else None)
        if not tcol or not pcol:
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        out = df[[tcol, pcol]].copy()
        out.columns = ['Ticker','Reference_Price']
        out['Ticker'] = out['Ticker'].map(self._normalize_ticker)
        out['Reference_Price'] = out['Reference_Price'].map(lambda x: safe_float(x, 0.0))
        out = out[out['Reference_Price'] > 0]
        out['Source'] = 'last_snapshot'
        return out

    def _extract_from_kline_cache(self, required_tickers: List[str]) -> pd.DataFrame:
        rows = []
        for raw in required_tickers:
            ticker = self._normalize_ticker(raw)
            for p in [PATHS.data_dir / 'kline_cache' / f'{ticker}_ohlcv.csv', PATHS.data_dir / 'kline_cache' / f'{ticker}_2y.csv']:
                if not p.exists():
                    continue
                try:
                    df = pd.read_csv(p, encoding='utf-8-sig')
                except Exception:
                    try:
                        df = pd.read_csv(p)
                    except Exception:
                        continue
                if df.empty or 'Close' not in df.columns:
                    continue
                ser = pd.to_numeric(df['Close'], errors='coerce').dropna()
                ser = ser[ser > 0]
                if ser.empty:
                    continue
                rows.append({'Ticker': ticker, 'Reference_Price': float(ser.iloc[-1]), 'Source': f'kline_cache:{p.name}'})
                break
        return pd.DataFrame(rows, columns=['Ticker','Reference_Price','Source'])

    def _extract_from_sql(self, required_tickers: List[str]) -> pd.DataFrame:
        if not required_tickers:
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        rows = []
        try:
            conn_str = (
                r"DRIVER={ODBC Driver 17 for SQL Server};"
                r"SERVER=localhost;"
                r"DATABASE=股票online;"
                r"Trusted_Connection=yes;"
            )
            with pyodbc.connect(conn_str) as conn:
                cur = conn.cursor()
                sql = """
                SELECT TOP 1 [Close] FROM daily_price_data
                WHERE [Ticker SYMBOL] IN (?, ?)
                ORDER BY [Date] DESC
                """
                for raw in required_tickers:
                    ticker = self._normalize_ticker(raw)
                    alt = ticker[:-3] + '.TWO' if ticker.endswith('.TW') else (ticker[:-4] + '.TW' if ticker.endswith('.TWO') else ticker)
                    cur.execute(sql, ticker, alt)
                    row = cur.fetchone()
                    if row and safe_float(row[0], 0.0) > 0:
                        rows.append({'Ticker': ticker, 'Reference_Price': float(row[0]), 'Source': 'sql:daily_price_data'})
        except Exception:
            return pd.DataFrame(columns=['Ticker','Reference_Price','Source'])
        return pd.DataFrame(rows, columns=['Ticker','Reference_Price','Source'])

    def _write_last_snapshot(self, required_norm, merged_df: pd.DataFrame) -> int:
        req = pd.DataFrame({'Ticker': sorted(set(required_norm))})
        if merged_df is None or merged_df.empty:
            out = req.copy()
            out['Reference_Price'] = ''
            out['Source'] = ''
        else:
            out = req.merge(merged_df[['Ticker','Reference_Price','Source']], on='Ticker', how='left')
        out.to_csv(self.last_snapshot_path, index=False, encoding='utf-8-sig')
        return int(pd.to_numeric(out['Reference_Price'], errors='coerce').fillna(0).gt(0).sum())

    def _load_decision_tickers_fallback(self) -> List[str]:
        out = []
        for p in [PATHS.base_dir / 'daily_decision_desk.csv', PATHS.base_dir / 'daily_decision_desk_prerisk.csv', PATHS.data_dir / 'training_bootstrap_universe.csv']:
            if not p.exists():
                continue
            try:
                df = pd.read_csv(p, encoding='utf-8-sig')
            except Exception:
                continue
            tcol = 'Ticker' if 'Ticker' in df.columns else ('Ticker SYMBOL' if 'Ticker SYMBOL' in df.columns else None)
            if not tcol:
                continue
            out.extend(df[tcol].astype(str).tolist())
        return [self._normalize_ticker(x) for x in out if self._normalize_ticker(x)]

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
        if not required_norm:
            required_norm = sorted(set(self._load_decision_tickers_fallback()))
        frames = [
            self._load_manual_overrides(),
            self._load_last_snapshot(),
            self._extract_from_decision(required_norm),
            self._extract_from_kline_cache(required_norm),
            self._extract_from_sql(required_norm),
        ]
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
        matched_rows = self._write_last_snapshot(required_norm, cat)
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
            'last_snapshot_path': str(self.last_snapshot_path),
            'last_snapshot_matched_rows': matched_rows,
            'manual_override_path': str(self.override_path),
            'manual_override_prefilled_rows': prefilled_override_rows,
            'candidates_path': str(self.candidates_path),
            'manual_template_path': str(self.template_path),
            'status': 'price_gap_reduced' if not cat.empty else 'waiting_for_price_sources',
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.candidates_path, payload



def main():
    bridge = PriceGapBridge()
    tickers = bridge._load_decision_tickers_fallback()
    path, payload = bridge.build(tickers)
    print(f"✅ PriceGapBridge 完成 | candidates={path} | matched={payload.get('matched_tickers', 0)} | missing={len(payload.get('missing_tickers', []))}")


if __name__ == '__main__':
    main()
