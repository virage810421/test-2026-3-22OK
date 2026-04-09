# -*- coding: utf-8 -*-
"""Compatibility helpers for the formal trader upgrade pack.
This file lets the new modules run even if parts of the original project are
still being consolidated.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


try:  # pragma: no cover - depends on user project layout
    from fts_config import PATHS, CONFIG  # type: ignore
except Exception:  # pragma: no cover
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
        system_name: str = '正式交易主控版_upgrade_pack'
        package_version: str = 'seal_v1'
        mode: str = 'PAPER'
        broker_type: str = 'paper'
        lot_size: int = 1000
        price_round: int = 2
        max_single_position_pct: float = 0.10
        daily_loss_limit_pct: float = 0.03
        max_industry_exposure_pct: float = 0.25
        max_order_notional: float = 500_000
        live_manual_arm: bool = False
        require_dual_confirmation: bool = True
        enable_live_kill_switch: bool = True

    PATHS = _FallbackPaths()
    CONFIG = _FallbackConfig()


def now_str() -> str:
    return _now()


def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        PATHS.log_dir.mkdir(parents=True, exist_ok=True)
        with open(PATHS.log_dir / 'formal_trader_upgrade.log', 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception:
        pass


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


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> Path:
    ensure_parent(path)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return path


def append_jsonl(path: Path, payload: Any) -> Path:
    ensure_parent(path)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(payload, ensure_ascii=False) + '\n')
    return path


def normalize_key(value: Any) -> str:
    return str(value or '').strip().upper()


def pct(numerator: float, denominator: float) -> float:
    denominator = float(denominator or 0)
    if denominator == 0:
        return 0.0
    return round(float(numerator or 0) / denominator, 6)


def getenv_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}
