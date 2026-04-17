# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
import pyodbc

TARGET_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票Online;"
    r"Trusted_Connection=yes;"
)

try:
    from fts_config import PATHS  # type: ignore
except Exception:
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        data_dir = base_dir / 'data'
        runtime_dir = base_dir / 'runtime'
    PATHS = _Paths()


def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute("SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(?) AND type = 'U'", f'dbo.{table_name}')
    return cursor.fetchone() is not None


def _safe_print(msg) -> None:
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        import sys
        enc = getattr(sys.stdout, 'encoding', None) or 'utf-8'
        fallback = str(msg).encode(enc, errors='replace').decode(enc, errors='replace')
        print(fallback, flush=True)


def _read(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path) if path.exists() else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _to_decimal(value, precision: int, scale: int, default: float = 0.0) -> Decimal:
    try:
        if value is None or value == '':
            num = float(default)
        else:
            num = float(value)
        if not math.isfinite(num):
            num = float(default)
    except Exception:
        num = float(default)
    q = Decimal(1).scaleb(-scale)
    max_abs = Decimal(10) ** (precision - scale) - q
    dec = Decimal(str(num)).quantize(q, rounding=ROUND_HALF_UP)
    if dec > max_abs:
        dec = max_abs
    elif dec < -max_abs:
        dec = -max_abs
    return dec


def sync_all():
    runtime_path = Path(PATHS.runtime_dir) / 'sql_feature_snapshot_sync.json'
    conn = pyodbc.connect(TARGET_CONN_STR)
    cur = conn.cursor()
    synced = {}
    clamp_counts = {}

    snap = _read(Path(PATHS.data_dir) / 'feature_cross_section_snapshot.csv')
    if not snap.empty and _table_exists(cur, 'feature_cross_section_snapshot'):
        cur.execute('DELETE FROM dbo.feature_cross_section_snapshot')
        for _, r in snap.iterrows():
            cur.execute(
                """
                INSERT INTO dbo.feature_cross_section_snapshot (
                    [股票代號],[完整代號],[產業名稱],[收盤價],[二十日報酬],[相對大盤二十日強弱],[相對產業二十日強弱],
                    [ATR百分比],[二十日實現波動率],[成交額代理],[二十日平均成交額代理],[營收年增率],[籌碼總比率],
                    [相對大盤分位數],[相對產業分位數],[營收年增率分位數],[籌碼總比率分位數],[成交額分位數],[平均成交額分位數],[ATR百分比分位數],[實現波動率分位數]
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                str(r.get('Ticker SYMBOL', '')), str(r.get('Ticker Full', '')), str(r.get('Sector', '')),
                _to_decimal(r.get('Close', 0), 28, 6), _to_decimal(r.get('Return_20', 0), 28, 10), _to_decimal(r.get('RS_vs_Market_20', 0), 28, 10),
                _to_decimal(r.get('RS_vs_Sector_20', 0), 28, 10), _to_decimal(r.get('ATR_Pct', 0), 28, 10), _to_decimal(r.get('RealizedVol_20', 0), 28, 10),
                _to_decimal(r.get('Turnover_Proxy', 0), 38, 6), _to_decimal(r.get('ADV20_Proxy', 0), 38, 6), _to_decimal(r.get('Revenue_YoY', 0), 28, 10),
                _to_decimal(r.get('Chip_Total_Ratio', 0), 28, 10), _to_decimal(r.get('RS_vs_Market_20_Pctl', 0.5), 18, 10), _to_decimal(r.get('RS_vs_Sector_20_Pctl', 0.5), 18, 10),
                _to_decimal(r.get('Revenue_YoY_Pctl', 0.5), 18, 10), _to_decimal(r.get('Chip_Total_Ratio_Pctl', 0.5), 18, 10), _to_decimal(r.get('Turnover_Pctl', 0.5), 18, 10),
                _to_decimal(r.get('ADV20_Pctl', 0.5), 18, 10), _to_decimal(r.get('ATR_Pct_Pctl', 0.5), 18, 10), _to_decimal(r.get('RealizedVol_20_Pctl', 0.5), 18, 10),
            )
        synced['feature_cross_section_snapshot'] = int(len(snap))
    elif not snap.empty:
        synced['feature_cross_section_snapshot'] = 'skipped_missing_table'

    events = _read(Path(PATHS.data_dir) / 'feature_event_calendar.csv')
    if not events.empty and _table_exists(cur, 'feature_event_calendar'):
        cur.execute('DELETE FROM dbo.feature_event_calendar')
        for _, r in events.iterrows():
            dt = pd.to_datetime(r.get('event_date'), errors='coerce')
            cur.execute(
                "INSERT INTO dbo.feature_event_calendar ([股票代號],[事件日期],[事件類型],[來源檔名]) VALUES (?,?,?,?)",
                str(r.get('ticker', '')),
                dt.date() if pd.notna(dt) else None,
                str(r.get('event_type', '')),
                'feature_event_calendar.csv',
            )
        synced['feature_event_calendar'] = int(len(events))
    elif not events.empty:
        synced['feature_event_calendar'] = 'skipped_missing_table'

    mounts = _read(Path(PATHS.data_dir) / 'selected_live_feature_mounts.csv')
    if not mounts.empty and _table_exists(cur, 'live_feature_mount'):
        cur.execute('DELETE FROM dbo.live_feature_mount')
        mounted_rows = 0
        skipped_rows = 0
        for _, r in mounts.iterrows():
            feature_name = str(r.get('feature_name', '')).strip()
            ticker = str(r.get('ticker', '')).strip()
            if not feature_name or not ticker:
                skipped_rows += 1
                continue
            val = _to_decimal(r.get('feature_value', 0), 38, 10)
            cur.execute(
                "INSERT INTO dbo.live_feature_mount ([掛載時間],[股票代號],[特徵名稱],[特徵值],[來源]) VALUES (?,?,?,?,?)",
                pd.Timestamp.now().to_pydatetime(), ticker, feature_name, val, 'selected_live_feature_mounts.csv',
            )
            mounted_rows += 1
        synced['live_feature_mount'] = int(mounted_rows)
        if skipped_rows:
            clamp_counts['live_feature_mount_skipped_rows'] = int(skipped_rows)
    elif not mounts.empty:
        synced['live_feature_mount'] = 'skipped_missing_table'

    reg = _read(Path(PATHS.data_dir) / 'training_feature_registry.csv')
    if not reg.empty and _table_exists(cur, 'training_feature_registry'):
        cur.execute('DELETE FROM dbo.training_feature_registry')
        for _, r in reg.iterrows():
            cur.execute(
                "INSERT INTO dbo.training_feature_registry ([特徵名稱],[特徵桶],[是否Percentile驅動],[是否事件窗特徵],[是否實戰啟用],[是否訓練啟用]) VALUES (?,?,?,?,?,?)",
                str(r.get('feature_name', '')), str(r.get('feature_bucket', '')),
                1 if 'Pctl' in str(r.get('feature_name', '')) else 0,
                1 if 'Window' in str(r.get('feature_name', '')) or 'Event_' in str(r.get('feature_name', '')) else 0,
                1, 1,
            )
        synced['training_feature_registry'] = int(len(reg))
    elif not reg.empty:
        synced['training_feature_registry'] = 'skipped_missing_table'

    conn.commit()
    conn.close()
    payload = {'status': 'sql_feature_snapshot_sync_ready', 'synced_rows': synced, 'notes': clamp_counts}
    runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    _safe_print(f'✅ sql feature snapshots synced {payload}')
    return runtime_path, payload


if __name__ == '__main__':
    sync_all()
