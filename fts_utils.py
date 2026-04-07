# -*- coding: utf-8 -*-
from datetime import datetime
import pandas as pd
from fts_config import PATHS, CONFIG

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    with open(PATHS.log_dir / "formal_trading_system_v15.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")

def safe_float(v, default=0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default

def safe_int(v, default=0) -> int:
    try:
        if pd.isna(v):
            return default
        return int(float(v))
    except Exception:
        return default

def round_price(v: float) -> float:
    return round(float(v), CONFIG.price_round)

def resolve_decision_csv():
    for p in PATHS.decision_csv_candidates:
        if p.exists():
            return p
    return PATHS.decision_csv_candidates[0]
