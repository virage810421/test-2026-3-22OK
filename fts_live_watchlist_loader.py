
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any
from collections import defaultdict
import pandas as pd

from fts_prelive_runtime import PATHS, now_str, load_json, write_json, normalize_key
from config import WATCH_LIST, PARAMS

LANES = ['LONG', 'SHORT', 'RANGE']

class LiveWatchlistLoader:
    def __init__(self):
        self.runtime_dir = PATHS.runtime_dir
        self.summary_path = self.runtime_dir / 'live_watchlist_loader.json'

    def _approved(self, lane: str) -> list[dict[str, Any]]:
        path = self.runtime_dir / f'approved_live_watchlist_{lane.lower()}.json'
        payload = load_json(path, default={}) or {}
        items = payload.get('items', payload if isinstance(payload, list) else [])
        return items if isinstance(items, list) else []

    def _fallback(self) -> list[dict[str, Any]]:
        return [{'ticker': t, 'lane': 'LONG', 'sector': '未知', 'promotion_score': 0.0, 'liquidity_score': 1.0, 'feature_coverage': 1.0, 'source': 'WATCH_LIST'} for t in WATCH_LIST]

    def resolve_live_watchlist(self) -> tuple[str, dict[str, Any]]:
        lane_items = {lane: self._approved(lane) for lane in LANES}
        if not any(lane_items.values()):
            merged = self._fallback()
            payload = {'generated_at': now_str(), 'status': 'fallback_watch_list', 'items': merged, 'lanes': {lane: [] for lane in LANES}, 'total_count': len(merged)}
            write_json(self.summary_path, payload)
            return str(self.summary_path), payload

        max_per_sector = int(PARAMS.get('LIVE_WATCHLIST_MAX_PER_SECTOR', 3))
        total_cap = int(PARAMS.get('LIVE_WATCHLIST_TOTAL_MAX_NAMES', 18))
        min_liq = float(PARAMS.get('LIVE_WATCHLIST_MIN_LIQUIDITY_SCORE', 0.20))
        min_cov = float(PARAMS.get('LIVE_WATCHLIST_MIN_FEATURE_COVERAGE', 0.95))
        max_short_over_long = float(PARAMS.get('LIVE_WATCHLIST_MAX_NET_SHORT_OVER_LONG', 0.60))
        max_long_over_short = float(PARAMS.get('LIVE_WATCHLIST_MAX_NET_LONG_OVER_SHORT', 1.20))

        by_ticker: dict[str, dict[str, Any]] = {}
        for lane, items in lane_items.items():
            for item in items:
                t = str(item.get('ticker') or '').strip()
                if not t:
                    continue
                score = float(item.get('promotion_score', 0.0) or 0.0)
                liq = float(item.get('liquidity_score', 1.0) or 0.0)
                cov = float(item.get('feature_coverage', 1.0) or 0.0)
                if liq < min_liq or cov < min_cov:
                    continue
                current = by_ticker.get(t)
                if current is None or score > float(current.get('promotion_score', 0.0) or 0.0):
                    merged = dict(item)
                    merged['lane'] = lane
                    by_ticker[t] = merged
        items = list(by_ticker.values())
        # optimizer penalty proxy: sector concentration and overlap penalty
        sector_counts = defaultdict(int)
        selected = []
        long_n = short_n = range_n = 0
        for item in sorted(items, key=lambda x: float(x.get('promotion_score', 0.0) or 0.0), reverse=True):
            lane = normalize_key(item.get('lane')) or 'LONG'
            sector = str(item.get('sector') or '未知')
            if sector_counts[sector] >= max_per_sector:
                continue
            projected_long = long_n + (1 if lane == 'LONG' else 0)
            projected_short = short_n + (1 if lane == 'SHORT' else 0)
            if projected_long > 0 and projected_short / projected_long > max_short_over_long:
                if lane == 'SHORT':
                    continue
            if projected_short > 0 and projected_long / max(projected_short, 1) > max_long_over_short:
                if lane == 'LONG' and projected_short > 0 and projected_long > 1:
                    continue
            item = dict(item)
            item['optimizer_penalty'] = sector_counts[sector] * 0.05
            item['optimized_score'] = float(item.get('promotion_score', 0.0) or 0.0) - item['optimizer_penalty']
            selected.append(item)
            sector_counts[sector] += 1
            long_n = projected_long
            short_n = projected_short
            range_n = range_n + (1 if lane == 'RANGE' else 0)
            if len(selected) >= total_cap:
                break
        lanes = {lane: [x for x in selected if normalize_key(x.get('lane')) == lane] for lane in LANES}
        payload = {
            'generated_at': now_str(),
            'status': 'directional_watchlist_loaded',
            'items': selected,
            'lanes': lanes,
            'total_count': len(selected),
            'sector_counts': dict(sector_counts),
            'net_exposure_proxy': {'LONG': long_n, 'SHORT': short_n, 'RANGE': range_n},
        }
        write_json(self.summary_path, payload)
        return str(self.summary_path), payload
