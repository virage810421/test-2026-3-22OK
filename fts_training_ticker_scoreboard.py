
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from config import WATCH_LIST, TRAINING_POOL
try:
    from fts_prelive_runtime import PATHS, now_str, write_json
except Exception:
    def now_str():
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    def write_json(path: Path, payload: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return path
    class _Paths:
        base_dir = Path('.')
        runtime_dir = Path('runtime')
        data_dir = Path('data')
    PATHS = _Paths()


def _infer_lane(row: pd.Series) -> str:
    direction = str(row.get('Direction') or row.get('方向') or '').upper()
    setup = str(row.get('Setup_Tag') or row.get('Setup') or '').upper()
    regime = str(row.get('Regime') or '').strip()
    if 'SHORT' in direction or '空' in direction or 'SHORT' in setup:
        return 'SHORT'
    if regime == '區間盤整' or 'RANGE' in setup:
        return 'RANGE'
    return 'LONG'


def _lane_return(row: pd.Series, lane: str) -> float:
    raw = row.get('Target_Return', None)
    if raw is None or str(raw) == 'nan':
        raw = (float(pd.to_numeric(row.get('Future_Return_Pct', 0.0), errors='coerce') or 0.0) / 100.0)
    r = float(pd.to_numeric(raw, errors='coerce') or 0.0)
    # Backward compatibility: old datasets stored percent points in Target_Return.
    if abs(r) > 0.80 and abs(r) <= 100.0:
        r = r / 100.0
    if lane == 'SHORT':
        return -r
    if lane == 'RANGE':
        return max(0.0, 0.5 * abs(r) - max(0.0, abs(r) - 0.08))
    return r


def build_scoreboard() -> tuple[str, dict[str, Any]]:
    runtime_dir = Path(getattr(PATHS, 'runtime_dir', Path('runtime')))
    data_path = Path(getattr(PATHS, 'data_dir', Path('data'))) / 'ml_training_data.csv'
    scoreboard_path = runtime_dir / 'training_ticker_scoreboard.csv'
    summary_path = runtime_dir / 'training_ticker_scoreboard.json'
    runtime_dir.mkdir(parents=True, exist_ok=True)
    if not data_path.exists():
        payload = {'generated_at': now_str(), 'status': 'dataset_missing', 'dataset_path': str(data_path)}
        write_json(summary_path, payload)
        return str(summary_path), payload
    try:
        df = pd.read_csv(data_path)
    except Exception as e:
        payload = {'generated_at': now_str(), 'status': 'dataset_unreadable', 'error': str(e), 'dataset_path': str(data_path)}
        write_json(summary_path, payload)
        return str(summary_path), payload
    if df.empty:
        payload = {'generated_at': now_str(), 'status': 'dataset_empty', 'dataset_path': str(data_path)}
        write_json(summary_path, payload)
        return str(summary_path), payload
    ticker_col = 'Ticker SYMBOL' if 'Ticker SYMBOL' in df.columns else ('Ticker' if 'Ticker' in df.columns else None)
    if ticker_col is None:
        payload = {'generated_at': now_str(), 'status': 'ticker_column_missing'}
        write_json(summary_path, payload)
        return str(summary_path), payload
    df = df.copy()
    df['__lane__'] = df.apply(_infer_lane, axis=1)
    df['__ret_long__'] = df.apply(lambda r: _lane_return(r, 'LONG'), axis=1)
    df['__ret_short__'] = df.apply(lambda r: _lane_return(r, 'SHORT'), axis=1)
    df['__ret_range__'] = df.apply(lambda r: _lane_return(r, 'RANGE'), axis=1)
    rows = []
    universe = list(dict.fromkeys(list(WATCH_LIST) + list(TRAINING_POOL) + [str(x) for x in df[ticker_col].dropna().unique()]))
    for ticker in universe:
        sub = df[df[ticker_col].astype(str) == str(ticker)].copy()
        if sub.empty:
            rows.append({'ticker': ticker, 'Long_OOT_EV': 0.0, 'Short_OOT_EV': 0.0, 'Range_OOT_EV': 0.0,
                         'Long_HitRate': 0.0, 'Short_HitRate': 0.0, 'Range_HitRate': 0.0,
                         'Long_Trade_Count': 0, 'Short_Trade_Count': 0, 'Range_Trade_Count': 0,
                         'Ticker_Promotion_Score_Long': 0.2, 'Ticker_Promotion_Score_Short': 0.15, 'Ticker_Promotion_Score_Range': 0.15})
            continue
        row = {'ticker': ticker}
        for lane, ret_col, score_seed in [('LONG', '__ret_long__', 0.25), ('SHORT', '__ret_short__', 0.20), ('RANGE', '__ret_range__', 0.20)]:
            lane_sub = sub if lane == 'LONG' else sub[sub['__lane__'].isin([lane, 'LONG'] if lane != 'LONG' else ['LONG'])]
            vals = pd.to_numeric(lane_sub[ret_col], errors='coerce').fillna(0.0)
            hit = float((vals > 0).mean()) if len(vals) else 0.0
            ev = float(vals.tail(max(1, len(vals)//5)).mean()) if len(vals) else 0.0
            pf_num = float(vals[vals > 0].sum()) if (vals > 0).any() else 0.0
            pf_den = float(abs(vals[vals < 0].sum())) if (vals < 0).any() else 0.0
            pf = pf_num / pf_den if pf_den > 1e-12 else (1.0 if len(vals) else 0.0)
            stability = 1.0 - min(float(vals.std()) if len(vals) > 1 else 0.0, 1.0)
            count = int(len(vals))
            score = score_seed + ev * 10 + hit * 2 + min(pf, 3.0) * 0.2 + stability * 0.2 + min(count / 100.0, 0.5)
            row[f'{lane.title()}_OOT_EV'] = ev
            row[f'{lane.title()}_HitRate'] = hit
            row[f'{lane.title()}_PF'] = pf
            row[f'{lane.title()}_MaxDD'] = 0.0
            row[f'{lane.title()}_Trade_Count'] = count
            row[f'{lane.title()}_Stability_Score'] = stability
            row[f'Ticker_Promotion_Score_{lane.title()}'] = score
        rows.append(row)
    out = pd.DataFrame(rows)
    out.to_csv(scoreboard_path, index=False, encoding='utf-8-sig')
    for lane in ['long', 'short', 'range']:
        out.to_csv(runtime_dir / f'training_ticker_scoreboard_{lane}.csv', index=False, encoding='utf-8-sig')
    payload = {'generated_at': now_str(), 'status': 'scoreboard_built', 'rows': int(len(out)), 'path': str(scoreboard_path)}
    write_json(summary_path, payload)
    return str(summary_path), payload


if __name__ == '__main__':
    print(build_scoreboard())
