# -*- coding: utf-8 -*-
from datetime import datetime
import sys
import pandas as pd
from fts_config import PATHS, CONFIG
import threading
import time
from contextlib import contextmanager


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_console_write(line: str) -> None:
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        enc = getattr(sys.stdout, 'encoding', None) or 'utf-8'
        print(line.encode(enc, errors='replace').decode(enc, errors='replace'), flush=True)


def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    _safe_console_write(line)
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


def render_progress_bar(progress: float, width: int = 26) -> str:
    """
    保留原函式名稱，避免舊主線 import 壞掉；
    但不再輸出視覺進度條，只輸出純文字百分比。
    """
    progress = max(0.0, min(float(progress), 1.0))
    return f"{int(progress * 100):>3d}%"


class StageProgress:
    def __init__(self, total_stages: int, heartbeat_seconds: float = 2.0):
        self.total_stages = max(int(total_stages), 1)
        self.heartbeat_seconds = max(float(heartbeat_seconds), 0.5)
        self.current_stage = 0

    def _emit(self, label: str, progress: float, state: str) -> None:
        pct = int(max(0.0, min(float(progress), 1.0)) * 100)
        log(f'📊 {label}｜{state}｜{pct}%')

    @contextmanager
    def stage(self, stage_no: int, label: str):
        start_ts = time.time()
        start_progress = max(0.0, min((stage_no - 1) / self.total_stages, 1.0))
        end_progress = max(0.0, min(stage_no / self.total_stages, 1.0))
        self.current_stage = stage_no
        self._emit(label, start_progress, '開始')

        stop_event = threading.Event()

        def heartbeat():
            while not stop_event.wait(self.heartbeat_seconds):
                elapsed = int(time.time() - start_ts)
                pct = int(start_progress * 100)
                log(f'⏳ {label}｜執行中｜{pct}%｜已耗時 {elapsed}s')

        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop_event.set()
            elapsed = int(time.time() - start_ts)
            log(f'📊 {label}｜完成｜{int(end_progress * 100)}%｜耗時 {elapsed}s')
