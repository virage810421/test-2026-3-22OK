# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

from fts_config import PATHS
from fts_utils import now_str, log


def _load_config_lists() -> tuple[list[str], list[str], list[str]]:
    try:
        import config  # type: ignore
        watch = list(getattr(config, 'WATCH_LIST', []))
        training = list(getattr(config, 'TRAINING_POOL', []))
        break_pool = list(getattr(config, 'BREAK_TEST_POOL', []))
        return watch, training, break_pool
    except Exception:
        return [], [], []


class WatchlistService:
    MODULE_VERSION = 'v83_watchlist_service'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'watchlist_service.json'

    def get_dynamic_watchlist(self) -> list[str]:
        try:
            import config  # type: ignore
            fn = getattr(config, 'get_dynamic_watch_list', None)
            if callable(fn):
                return list(dict.fromkeys(fn()))
        except Exception:
            pass
        watch, training, break_pool = _load_config_lists()
        merged = []
        for pool in [watch, training, break_pool]:
            for ticker in pool:
                if ticker not in merged:
                    merged.append(ticker)
        return merged

    def build_final_watchlist(self, extra_candidates: list[str] | None = None, limit: int = 40) -> list[str]:
        merged = self.get_dynamic_watchlist()
        for ticker in extra_candidates or []:
            if ticker not in merged:
                merged.append(ticker)
        return merged[:limit]

    def build_summary(self) -> tuple[Any, dict[str, Any]]:
        dyn = self.get_dynamic_watchlist()
        final = self.build_final_watchlist()
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'dynamic_watchlist_count': len(dyn),
            'final_watchlist_count': len(final),
            'status': 'wave3_watchlist_rules_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🎯 watchlist service ready: {self.runtime_path}')
        return self.runtime_path, payload
