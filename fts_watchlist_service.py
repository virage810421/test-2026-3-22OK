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
    MODULE_VERSION = 'v86_watchlist_service_approved_live_mount'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'watchlist_service.json'

    def _load_live_watchlist(self) -> list[str]:
        try:
            from fts_live_watchlist_loader import ApprovedLiveWatchlistLoader  # type: ignore
            return list(dict.fromkeys(ApprovedLiveWatchlistLoader().load_live_watchlist()))
        except Exception:
            pass
        try:
            import config  # type: ignore
            fn = getattr(config, 'get_live_watch_list', None)
            if callable(fn):
                return list(dict.fromkeys(fn()))
        except Exception:
            pass
        watch, _, _ = _load_config_lists()
        return list(dict.fromkeys(watch))

    def _load_training_watchlist(self) -> list[str]:
        try:
            import config  # type: ignore
            fn = getattr(config, 'get_training_watch_list', None)
            if callable(fn):
                return list(dict.fromkeys(fn()))
            fallback = getattr(config, 'get_dynamic_watch_list', None)
            if callable(fallback):
                return list(dict.fromkeys(fallback(mode='training')))
        except Exception:
            pass
        watch, training, break_pool = _load_config_lists()
        merged = []
        for pool in [watch, training, break_pool]:
            for ticker in pool:
                if ticker not in merged:
                    merged.append(ticker)
        return merged

    def get_dynamic_watchlist(self, mode: str = 'live') -> list[str]:
        return self._load_live_watchlist() if str(mode).lower() == 'live' else self._load_training_watchlist()

    def build_final_watchlist(self, extra_candidates: list[str] | None = None, limit: int = 40, mode: str = 'live') -> list[str]:
        merged = self.get_dynamic_watchlist(mode=mode)
        for ticker in extra_candidates or []:
            if ticker not in merged:
                merged.append(ticker)
        return merged[:limit]

    def build_summary(self) -> tuple[Any, dict[str, Any]]:
        live = self.get_dynamic_watchlist(mode='live')
        training = self.get_dynamic_watchlist(mode='training')
        watch, train_pool, break_pool = _load_config_lists()
        approved_loader_payload = {}
        try:
            from fts_live_watchlist_loader import ApprovedLiveWatchlistLoader  # type: ignore
            _, approved_loader_payload = ApprovedLiveWatchlistLoader().build_summary()
        except Exception:
            approved_loader_payload = {}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'watch_count': len(watch),
            'training_count': len(train_pool),
            'break_test_count': len(break_pool),
            'dynamic_live_watchlist_count': len(live),
            'dynamic_training_watchlist_count': len(training),
            'final_live_watchlist_count': len(self.build_final_watchlist(mode='live')),
            'final_training_watchlist_count': len(self.build_final_watchlist(mode='training', limit=200)),
            'approved_live_loader': approved_loader_payload,
            'status': 'watchlist_split_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🎯 watchlist service ready: {self.runtime_path}')
        return self.runtime_path, payload


if __name__ == '__main__':
    path, payload = WatchlistService().build_summary()
    print(path)
    print(payload.get('status'))
