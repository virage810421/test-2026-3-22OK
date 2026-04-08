# -*- coding: utf-8 -*-
from datetime import datetime
import pandas as pd
from fts_config import PATHS, CONFIG
import threading
import time
from contextlib import contextmanager


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
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
    progress = max(0.0, min(float(progress), 1.0))
    filled = int(round(width * progress))
    bar = '█' * filled + '░' * (width - filled)
    return f'[{bar}] {int(progress * 100):>3d}%'


class StageProgress:
    def __init__(self, total_stages: int, heartbeat_seconds: float = 2.0):
        self.total_stages = max(int(total_stages), 1)
        self.heartbeat_seconds = max(float(heartbeat_seconds), 0.5)
        self.current_stage = 0

    def _emit(self, label: str, progress: float, state: str) -> None:
        log(f'📊 {render_progress_bar(progress)} {label}｜{state}')

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
                log(f'⏳ {render_progress_bar(start_progress)} {label}｜執行中，已耗時 {elapsed}s')

        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop_event.set()
            elapsed = int(time.time() - start_ts)
            self._emit(label, end_progress, f'完成，耗時 {elapsed}s')
