# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, write_json, append_jsonl, safe_float, safe_int, normalize_key  # type: ignore
except Exception:
    @dataclass
    class _FallbackPaths:
        base_dir: Path = Path.cwd()
        data_dir: Path = Path.cwd() / 'data'
        log_dir: Path = Path.cwd() / 'logs'
        model_dir: Path = Path.cwd() / 'models'
        state_dir: Path = Path.cwd() / 'state'
        runtime_dir: Path = Path.cwd() / 'runtime'

        def __post_init__(self):
            for d in [self.data_dir, self.log_dir, self.model_dir, self.state_dir, self.runtime_dir]:
                d.mkdir(parents=True, exist_ok=True)

    @dataclass
    class _FallbackConfig:
        mode: str = 'PAPER'
        broker_type: str = 'paper'
        max_single_position_pct: float = 0.10
        max_industry_exposure_pct: float = 0.25
        daily_loss_limit_pct: float = 0.03
        max_order_notional: float = 500_000
        live_manual_arm: bool = False
        require_dual_confirmation: bool = True
        enable_live_kill_switch: bool = True
        starting_cash: float = 1_000_000

    PATHS = _FallbackPaths()
    CONFIG = _FallbackConfig()

    def now_str() -> str:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def log(msg: str) -> None:
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
        print(line, flush=True)

    def load_json(path: Path, default: Any = None) -> Any:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            return default

    def write_json(path: Path, payload: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return path

    def append_jsonl(path: Path, payload: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(payload, ensure_ascii=False) + '\n')
        return path

    def safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value in ('', None):
                return default
            return float(value)
        except Exception:
            return default

    def safe_int(value: Any, default: int = 0) -> int:
        try:
            if value in ('', None):
                return default
            return int(float(value))
        except Exception:
            return default

    def normalize_key(value: Any) -> str:
        return str(value or '').strip().upper()
