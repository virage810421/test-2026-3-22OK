# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        data_dir = base_dir / 'data'
        runtime_dir = base_dir / 'runtime'
    PATHS = _Paths()

try:
    from fts_utils import now_str, log  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')
    def log(msg: str) -> None:
        print(msg)


class EventCalendarService:
    MODULE_VERSION = 'v83_event_calendar_service_full'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'event_calendar_service.json'
        self.table_path = PATHS.data_dir / 'feature_event_calendar.csv'
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)
        Path(PATHS.data_dir).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _read_csv_if_exists(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def _norm_ticker(v: Any) -> str:
        s = str(v or '').strip()
        if not s:
            return ''
        return s.replace('.TW', '').replace('.TWO', '')

    @staticmethod
    def _find_col(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    def _revenue_events(self) -> pd.DataFrame:
        candidates = [
            PATHS.data_dir / 'latest_monthly_revenue_with_industry.csv',
            PATHS.base_dir / 'seed_data' / 'latest_monthly_revenue_with_industry.csv',
        ]
        out = []
        for p in candidates:
            df = self._read_csv_if_exists(p)
            if df.empty:
                continue
            tcol = self._find_col(df, ['Ticker SYMBOL', 'ticker', 'stock_id', '代號'])
            dcol = self._find_col(df, ['資料年月日', 'Revenue_Release_Date', '月營收發布日', '最新營收日期'])
            if not tcol:
                continue
            if dcol:
                dates = pd.to_datetime(df[dcol], errors='coerce')
            else:
                ymcol = self._find_col(df, ['資料年月', 'YearMonth'])
                dates = pd.to_datetime(df[ymcol].astype(str) + '-15', errors='coerce') if ymcol else pd.NaT
            tmp = pd.DataFrame({'ticker': df[tcol].map(self._norm_ticker), 'event_date': dates, 'event_type': 'revenue'})
            out.append(tmp)
        if not out:
            return pd.DataFrame(columns=['ticker', 'event_date', 'event_type'])
        df = pd.concat(out, ignore_index=True)
        return df.dropna(subset=['ticker', 'event_date']).drop_duplicates()

    def _earnings_events(self) -> pd.DataFrame:
        candidates = [
            PATHS.data_dir / 'market_financials_backup_fullspeed.csv',
            PATHS.base_dir / 'seed_data' / 'market_financials_backup_fullspeed.csv',
        ]
        out = []
        for p in candidates:
            df = self._read_csv_if_exists(p)
            if df.empty:
                continue
            tcol = self._find_col(df, ['Ticker SYMBOL', 'ticker', 'stock_id', '代號'])
            dcol = self._find_col(df, ['資料年月日', 'Earnings_Release_Date', '財報發布日', 'Quarterly_Report_Date'])
            if not tcol:
                continue
            if dcol:
                dates = pd.to_datetime(df[dcol], errors='coerce')
            else:
                ymcol = self._find_col(df, ['資料年月', 'Quarter', 'YearQuarter'])
                if ymcol:
                    dates = pd.to_datetime(
                        df[ymcol].astype(str)
                        .str.replace('Q1', '-05-15')
                        .str.replace('Q2', '-08-15')
                        .str.replace('Q3', '-11-15')
                        .str.replace('Q4', '-03-31'),
                        errors='coerce',
                    )
                else:
                    dates = pd.NaT
            tmp = pd.DataFrame({'ticker': df[tcol].map(self._norm_ticker), 'event_date': dates, 'event_type': 'earnings'})
            out.append(tmp)
        if not out:
            return pd.DataFrame(columns=['ticker', 'event_date', 'event_type'])
        df = pd.concat(out, ignore_index=True)
        return df.dropna(subset=['ticker', 'event_date']).drop_duplicates()

    def _dividend_events(self) -> pd.DataFrame:
        candidates = [PATHS.data_dir / 'latest_monthly_revenue_with_industry.csv', PATHS.data_dir / 'market_financials_backup_fullspeed.csv']
        out = []
        for p in candidates:
            df = self._read_csv_if_exists(p)
            if df.empty:
                continue
            tcol = self._find_col(df, ['Ticker SYMBOL', 'ticker', 'stock_id', '代號'])
            dcol = self._find_col(df, ['ExDividend_Date', '除權息日'])
            if not tcol or not dcol:
                continue
            tmp = pd.DataFrame({'ticker': df[tcol].map(self._norm_ticker), 'event_date': pd.to_datetime(df[dcol], errors='coerce'), 'event_type': 'dividend'})
            out.append(tmp)
        if not out:
            return pd.DataFrame(columns=['ticker', 'event_date', 'event_type'])
        df = pd.concat(out, ignore_index=True)
        return df.dropna(subset=['ticker', 'event_date']).drop_duplicates()

    def build_event_table(self) -> tuple[Path, pd.DataFrame]:
        df = pd.concat([self._revenue_events(), self._earnings_events(), self._dividend_events()], ignore_index=True)
        if df.empty:
            df = pd.DataFrame(columns=['ticker', 'event_date', 'event_type'])
        else:
            df = df.sort_values(['ticker', 'event_type', 'event_date']).drop_duplicates()
        df.to_csv(self.table_path, index=False, encoding='utf-8-sig')
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'table_path': str(self.table_path),
            'rows': int(len(df)),
            'event_counts': {k: int(v) for k, v in df['event_type'].value_counts().to_dict().items()} if not df.empty else {},
            'status': 'event_calendar_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🗓️ event calendar ready: {self.runtime_path}')
        return self.table_path, df

    def _load_table(self) -> pd.DataFrame:
        if not self.table_path.exists():
            _, df = self.build_event_table()
            return df
        return self._read_csv_if_exists(self.table_path)

    @staticmethod
    def _nearest(event_dates: pd.Series, as_of: pd.Timestamp) -> tuple[int, int]:
        if event_dates.empty or pd.isna(as_of):
            return 999, 999
        past = event_dates[event_dates <= as_of]
        future = event_dates[event_dates >= as_of]
        since = int((as_of - past.max()).days) if not past.empty else 999
        to = int((future.min() - as_of).days) if not future.empty else 999
        return since, to

    def event_vector(self, ticker: str, as_of_date: Any) -> dict[str, float]:
        bare = self._norm_ticker(ticker)
        as_of = pd.to_datetime(as_of_date, errors='coerce')
        df = self._load_table()
        if df.empty or not bare:
            return {
                'Event_Days_Since_Revenue': 999.0, 'Event_Days_To_Revenue': 999.0,
                'Revenue_Window_1': 0.0, 'Revenue_Window_3': 0.0, 'Revenue_Window_5': 0.0, 'Revenue_Window_10': 0.0,
                'Event_Days_Since_Earnings': 999.0, 'Event_Days_To_Earnings': 999.0,
                'Earnings_Window_3': 0.0, 'Earnings_Window_7': 0.0, 'Earnings_Window_14': 0.0,
                'Earnings_Window_Flag': 0.0, 'Dividend_Window_7': 0.0,
            }
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
        tdf = df[df['ticker'] == bare]
        rev = tdf[tdf['event_type'] == 'revenue']['event_date'].dropna()
        ern = tdf[tdf['event_type'] == 'earnings']['event_date'].dropna()
        div = tdf[tdf['event_type'] == 'dividend']['event_date'].dropna()
        rev_since, rev_to = self._nearest(rev, as_of)
        ern_since, ern_to = self._nearest(ern, as_of)
        div_since, div_to = self._nearest(div, as_of)
        return {
            'Event_Days_Since_Revenue': float(rev_since),
            'Event_Days_To_Revenue': float(rev_to),
            'Revenue_Window_1': float(abs(rev_since) <= 1 or abs(rev_to) <= 1),
            'Revenue_Window_3': float(abs(rev_since) <= 3 or abs(rev_to) <= 3),
            'Revenue_Window_5': float(abs(rev_since) <= 5 or abs(rev_to) <= 5),
            'Revenue_Window_10': float(abs(rev_since) <= 10 or abs(rev_to) <= 10),
            'Event_Days_Since_Earnings': float(ern_since),
            'Event_Days_To_Earnings': float(ern_to),
            'Earnings_Window_3': float(abs(ern_since) <= 3 or abs(ern_to) <= 3),
            'Earnings_Window_7': float(abs(ern_since) <= 7 or abs(ern_to) <= 7),
            'Earnings_Window_14': float(abs(ern_since) <= 14 or abs(ern_to) <= 14),
            'Earnings_Window_Flag': float(abs(ern_since) <= 7 or abs(ern_to) <= 7),
            'Dividend_Window_7': float(abs(div_since) <= 7 or abs(div_to) <= 7),
        }

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        _, df = self.build_event_table()
        payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'event_table_rows': int(len(df)), 'table_path': str(self.table_path), 'status': 'event_calendar_ready'}
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.runtime_path, payload
