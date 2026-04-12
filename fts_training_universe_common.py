# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from fts_config import PATHS
from fts_utils import now_str

try:
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover
    pyodbc = None

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)


def get_conn():
    if pyodbc is None:
        raise RuntimeError('pyodbc_unavailable')
    return pyodbc.connect(DB_CONN_STR)


def safe_read_sql(query: str) -> pd.DataFrame:
    if pyodbc is None:
        return pd.DataFrame()
    try:
        with get_conn() as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


def table_exists(table_name: str) -> bool:
    q = f"""
    SELECT 1 AS ok
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='{str(table_name).replace("'", "''")}'
    """
    return not safe_read_sql(q).empty


def safe_read_first_sql(queries: list[str]) -> pd.DataFrame:
    for query in queries:
        df = safe_read_sql(query)
        if not df.empty:
            return df
    return pd.DataFrame()


def safe_read_first_csv(paths: Iterable[Path | str]) -> pd.DataFrame:
    for path_like in paths:
        path = Path(path_like)
        if not path.exists():
            continue
        for kwargs in ({'encoding': 'utf-8-sig'}, {}):
            try:
                return pd.read_csv(path, **kwargs)
            except Exception:
                continue
    return pd.DataFrame()


def latest_per_ticker(df: pd.DataFrame, ticker_col: str = 'Ticker SYMBOL', date_candidates: list[str] | None = None) -> pd.DataFrame:
    if df.empty or ticker_col not in df.columns:
        return df.copy()
    date_candidates = date_candidates or ['資料日期', '資料年月日', 'Date', '資料年月', '月份']
    out = df.copy()
    for col in date_candidates:
        if col in out.columns:
            try:
                out[col] = pd.to_datetime(out[col], errors='coerce')
                out = out.sort_values([ticker_col, col]).drop_duplicates([ticker_col], keep='last')
                return out.reset_index(drop=True)
            except Exception:
                continue
    return out.drop_duplicates([ticker_col], keep='last').reset_index(drop=True)


def normalize_ticker(val: Any) -> str:
    s = str(val or '').strip()
    if not s:
        return ''
    if re.fullmatch(r'\d{4}', s):
        return f'{s}.TW'
    return s


def clip01(v: Any) -> float:
    try:
        x = float(v)
    except Exception:
        return 0.0
    if math.isnan(x) or math.isinf(x):
        return 0.0
    return max(0.0, min(1.0, x))


def score_linear(value: Any, low: float, high: float, reverse: bool = False, default: float = 0.0) -> float:
    try:
        x = float(value)
        if math.isnan(x) or math.isinf(x):
            return default
    except Exception:
        return default
    if high == low:
        return 1.0 if not reverse else 0.0
    score = (x - low) / (high - low)
    score = clip01(score)
    return float(1.0 - score) if reverse else float(score)


def bool_flag(val: Any) -> int:
    if isinstance(val, bool):
        return int(val)
    s = str(val or '').strip().lower()
    return 1 if s in {'1', 'true', 't', 'yes', 'y'} else 0


def infer_market(ticker: str) -> str:
    t = str(ticker or '').upper()
    if t.endswith('.TW'):
        return '上市'
    if t.endswith('.TWO') or t.endswith('.TWO.'):
        return '上櫃'
    return '未知'


def infer_is_etf(ticker: str, name: str = '') -> int:
    t = str(ticker or '').split('.')[0]
    n = str(name or '')
    if t.startswith('00') or 'ETF' in n.upper():
        return 1
    return 0


def runtime_csv(name: str) -> Path:
    return PATHS.runtime_dir / name


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def full_refresh_table(table_name: str, df: pd.DataFrame) -> dict[str, Any]:
    if pyodbc is None:
        return {'status': 'pyodbc_unavailable', 'table': table_name, 'rows': int(len(df))}
    if df.empty:
        return {'status': 'empty_skip', 'table': table_name, 'rows': 0}
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.fast_executemany = True
            cur.execute(f"DELETE FROM dbo.[{table_name}]")
            cols = list(df.columns)
            placeholders = ','.join(['?'] * len(cols))
            col_expr = ','.join(f'[{c}]' for c in cols)
            sql = f"INSERT INTO dbo.[{table_name}] ({col_expr}) VALUES ({placeholders})"
            rows = []
            for row in df.itertuples(index=False, name=None):
                cleaned = []
                for v in row:
                    if pd.isna(v):
                        cleaned.append(None)
                    elif hasattr(v, 'to_pydatetime'):
                        cleaned.append(v.to_pydatetime())
                    elif isinstance(v, pd.Timestamp):
                        cleaned.append(v.to_pydatetime())
                    else:
                        cleaned.append(v)
                rows.append(tuple(cleaned))
            cur.executemany(sql, rows)
            conn.commit()
        return {'status': 'refreshed', 'table': table_name, 'rows': int(len(df))}
    except Exception as exc:
        return {'status': 'failed', 'table': table_name, 'rows': int(len(df)), 'error': str(exc)}
