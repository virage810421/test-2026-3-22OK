# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Iterable


def normalize_ticker(v: str) -> str:
    s = str(v or '').strip().upper()
    if not s:
        return ''
    if s.endswith('.TW') or s.endswith('.TWO'):
        return s
    if s.isdigit():
        return f'{s}.TW'
    return s


def load_optional_tickers_from_csvs(optional_universe_files: Iterable[Path]) -> list[str]:
    out: list[str] = []
    env_raw = os.getenv('FTS_EXTRA_TICKERS', '').strip()
    if env_raw:
        for token in env_raw.replace(';', ',').split(','):
            ticker = normalize_ticker(token)
            if ticker:
                out.append(ticker)
    for path in optional_universe_files:
        path = Path(path)
        if not path.exists():
            continue
        try:
            with open(path, 'r', encoding='utf-8-sig', newline='') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    for col in ('Ticker SYMBOL', 'Ticker', 'ticker', 'symbol', 'stock_id', '代號'):
                        ticker = normalize_ticker(row.get(col, ''))
                        if ticker:
                            out.append(ticker)
                            break
        except (OSError, UnicodeDecodeError, csv.Error):
            continue
    return out


def build_dynamic_watch_list(
    watch_list: list[str],
    training_pool: list[str],
    break_test_pool: list[str],
    optional_universe_files: Iterable[Path],
    *,
    training_only: bool = False,
    max_names: int | None = None,
) -> list[str]:
    merged: list[str] = []
    sources = [training_pool] if training_only else [watch_list, training_pool, break_test_pool, load_optional_tickers_from_csvs(optional_universe_files)]
    max_items = max_names if max_names is not None else max(int(os.getenv('FTS_MAX_DYNAMIC_TICKERS', '60') or 60), len(watch_list), len(training_pool))
    for pool in sources:
        for ticker in pool:
            normalized = normalize_ticker(ticker)
            if normalized and normalized not in merged:
                merged.append(normalized)
            if len(merged) >= max_items:
                return merged
    return merged


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, '').strip().lower()
    if not raw:
        return default
    return raw in {'1', 'true', 'yes', 'y', 'on'}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, '').strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def build_dynamic_watch_list_from_env(
    watch_list: list[str],
    training_pool: list[str],
    break_test_pool: list[str],
    optional_universe_files: Iterable[Path],
) -> list[str]:
    return build_dynamic_watch_list(
        watch_list=watch_list,
        training_pool=training_pool,
        break_test_pool=break_test_pool,
        optional_universe_files=optional_universe_files,
        training_only=_env_flag('FTS_TRAINING_POOL_ONLY', False),
        max_names=max(_env_int('FTS_MAX_DYNAMIC_TICKERS', 60), len(watch_list), len(training_pool)),
    )
