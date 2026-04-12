
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any
import json
from pathlib import Path

from fts_prelive_runtime import PATHS, now_str, load_json, write_json, append_jsonl, normalize_key

LANES = ['LONG', 'SHORT', 'RANGE']


class LiveWatchlistRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'live_watchlist_registry.json'
        self.events_path = PATHS.runtime_dir / 'live_watchlist_registry_events.jsonl'

    def _load(self) -> dict[str, Any]:
        return load_json(self.path, default={'latest': {}, 'history': []}) or {'latest': {}, 'history': []}

    def register_watchlist(self, pool_type: str, payload: dict[str, Any], source_path: str = '') -> tuple[str, dict[str, Any]]:
        state = self._load()
        lane = normalize_key(pool_type) or 'UNKNOWN'
        version = str(payload.get('approved_version') or payload.get('generated_at') or now_str())
        entry = {
            'registered_at': now_str(),
            'pool_type': lane,
            'approved_version': version,
            'rollback_source': state.get('latest', {}).get(lane, {}).get('approved_version', ''),
            'promotion_batch_id': str(payload.get('promotion_batch_id') or payload.get('generated_at') or ''),
            'source_path': source_path,
            'item_count': int(payload.get('count') or len(payload.get('items', [])) or 0),
        }
        state.setdefault('history', []).append(entry)
        state.setdefault('latest', {})[lane] = entry
        write_json(self.path, state)
        append_jsonl(self.events_path, entry)
        return str(self.path), state

    def latest_watchlist(self, pool_type: str) -> tuple[str, dict[str, Any]]:
        state = self._load()
        lane = normalize_key(pool_type) or 'UNKNOWN'
        payload = {'generated_at': now_str(), 'status': 'latest_watchlist', 'lane': lane, 'entry': state.get('latest', {}).get(lane, {})}
        return str(self.path), payload

    def rollback_watchlist(self, pool_type: str) -> tuple[str, dict[str, Any]]:
        state = self._load()
        lane = normalize_key(pool_type) or 'UNKNOWN'
        history = [h for h in state.get('history', []) if normalize_key(h.get('pool_type')) == lane]
        previous = history[-2] if len(history) >= 2 else (history[-1] if history else {})
        state.setdefault('latest', {})[lane] = previous
        write_json(self.path, state)
        payload = {'generated_at': now_str(), 'status': 'rolled_back', 'lane': lane, 'entry': previous}
        append_jsonl(self.events_path, payload)
        return str(self.path), payload
