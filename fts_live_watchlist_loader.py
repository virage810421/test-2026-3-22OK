
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any
from collections import defaultdict

from fts_prelive_runtime import PATHS, now_str, load_json, write_json, normalize_key
from config import WATCH_LIST, TRAINING_POOL, PARAMS

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

    def _fallback_lane(self, lane: str, count: int) -> list[dict[str, Any]]:
        universe = list(dict.fromkeys(list(WATCH_LIST) + list(TRAINING_POOL)))
        if lane == 'SHORT':
            universe = list(reversed(universe))
        elif lane == 'RANGE':
            universe = universe[::2] + universe[1::2]
        return [{
            'ticker': t, 'lane': lane, 'sector': '未知', 'promotion_score': 0.1,
<<<<<<< HEAD
            'liquidity_score': 1.0, 'feature_coverage': 1.0, 'source': 'lane_fallback_provisional',
            'approval_reason': 'lane_fallback_provisional', 'oot_ev': 0.0, 'hit_rate': 0.5,
            'provisional': True, 'readiness_tier': 'fallback_only',
=======
            'liquidity_score': 1.0, 'feature_coverage': 1.0, 'source': 'lane_fallback',
            'approval_reason': 'lane_fallback', 'oot_ev': 0.0, 'hit_rate': 0.5,
>>>>>>> ad1db6bec225a276b4ad4c7df6c049d994a30092
        } for t in universe[:count]]

    def _sanitize_lane_items(self, items: list[dict[str, Any]], lane: str, min_liq: float, min_cov: float) -> list[dict[str, Any]]:
        best: dict[str, dict[str, Any]] = {}
        for raw in items:
            item = dict(raw)
            t = str(item.get('ticker') or '').strip()
            if not t:
                continue
            liq = float(item.get('liquidity_score', 1.0) or 0.0)
            cov = float(item.get('feature_coverage', 1.0) or 0.0)
            if liq < min_liq or cov < min_cov:
                continue
            item['lane'] = lane
            item['promotion_score'] = float(item.get('promotion_score', 0.0) or 0.0)
<<<<<<< HEAD
            item['provisional'] = bool(item.get('provisional', False))
            item['readiness_tier'] = str(item.get('readiness_tier', 'approved')).strip() or 'approved'
=======
>>>>>>> ad1db6bec225a276b4ad4c7df6c049d994a30092
            cur = best.get(t)
            if cur is None or item['promotion_score'] > float(cur.get('promotion_score', 0.0) or 0.0):
                best[t] = item
        return sorted(best.values(), key=lambda x: float(x.get('promotion_score', 0.0) or 0.0), reverse=True)

    def resolve_live_watchlist(self) -> tuple[str, dict[str, Any]]:
        lane_items = {lane: self._approved(lane) for lane in LANES}
        min_counts = {
            'LONG': int(PARAMS.get('LIVE_WATCHLIST_MIN_PER_LANE_LONG', 2)),
            'SHORT': int(PARAMS.get('LIVE_WATCHLIST_MIN_PER_LANE_SHORT', 2)),
            'RANGE': int(PARAMS.get('LIVE_WATCHLIST_MIN_PER_LANE_RANGE', 2)),
        }
        # ensure each lane has some raw items before optimization
        for lane in LANES:
            if len(lane_items[lane]) < max(1, min_counts[lane]):
                seed = self._fallback_lane(lane, max(1, min_counts[lane]))
                seen = {str(x.get('ticker')) for x in lane_items[lane]}
                for item in seed:
                    if item['ticker'] not in seen:
                        lane_items[lane].append(item)
                        seen.add(item['ticker'])

        max_per_sector = int(PARAMS.get('LIVE_WATCHLIST_MAX_PER_SECTOR', 3))
        total_cap = int(PARAMS.get('LIVE_WATCHLIST_TOTAL_MAX_NAMES', 18))
        min_liq = float(PARAMS.get('LIVE_WATCHLIST_MIN_LIQUIDITY_SCORE', 0.20))
        min_cov = float(PARAMS.get('LIVE_WATCHLIST_MIN_FEATURE_COVERAGE', 0.95))
        max_short_over_long = float(PARAMS.get('LIVE_WATCHLIST_MAX_NET_SHORT_OVER_LONG', 0.60))
        max_long_over_short = float(PARAMS.get('LIVE_WATCHLIST_MAX_NET_LONG_OVER_SHORT', 1.20))
        unknown_soft_cap = bool(PARAMS.get('LIVE_WATCHLIST_UNKNOWN_SECTOR_SOFT_CAP', True))

        lane_pools = {lane: self._sanitize_lane_items(items, lane, min_liq, min_cov) for lane, items in lane_items.items()}

        selected: list[dict[str, Any]] = []
        selected_keys: set[tuple[str, str]] = set()
        sector_counts = defaultdict(int)
        lane_counts = {'LONG': 0, 'SHORT': 0, 'RANGE': 0}

        def sector_key(item: dict[str, Any]) -> str:
            lane = normalize_key(item.get('lane')) or 'LONG'
            sector = str(item.get('sector') or '未知')
            if unknown_soft_cap and sector == '未知':
                return f'未知::{lane}'
            return sector

        def accept(item: dict[str, Any], phase: str) -> bool:
            nonlocal lane_counts
            lane = normalize_key(item.get('lane')) or 'LONG'
            t = str(item.get('ticker') or '').strip()
            key = (t, lane)
            if not t or key in selected_keys:
                return False
            s_key = sector_key(item)
            if sector_counts[s_key] >= max_per_sector:
                return False
            projected = dict(lane_counts)
            projected[lane] += 1
            if phase == 'fill':
                if projected['LONG'] > 0 and lane == 'SHORT' and projected['SHORT'] / projected['LONG'] > max_short_over_long:
                    return False
                if projected['SHORT'] > 0 and lane == 'LONG' and projected['LONG'] / max(projected['SHORT'], 1) > max_long_over_short and projected['LONG'] > 1:
                    return False
            item['optimizer_penalty'] = sector_counts[s_key] * 0.05
            item['optimized_score'] = float(item.get('promotion_score', 0.0) or 0.0) - item['optimizer_penalty']
            selected.append(item)
            selected_keys.add(key)
            sector_counts[s_key] += 1
            lane_counts = projected
            return True

        # phase 1: guarantee minimum names per lane
        for lane in LANES:
            target = min_counts[lane]
            for item in lane_pools[lane]:
                if lane_counts[lane] >= target or len(selected) >= total_cap:
                    break
                accept(dict(item), phase='guarantee')

        # phase 2: fill remainder globally by optimized score
        remaining: list[dict[str, Any]] = []
        for lane in LANES:
            remaining.extend(lane_pools[lane])
        remaining.sort(key=lambda x: float(x.get('promotion_score', 0.0) or 0.0), reverse=True)
        for item in remaining:
            if len(selected) >= total_cap:
                break
            accept(dict(item), phase='fill')

        lanes = {lane: [x for x in selected if normalize_key(x.get('lane')) == lane] for lane in LANES}
        lane_readiness = {}
        provisional_item_count = 0
        for lane in LANES:
            lane_items = lanes.get(lane, [])
            lane_provisional = sum(1 for x in lane_items if bool(x.get('provisional', False)))
            provisional_item_count += lane_provisional
            lane_readiness[lane] = {
                'count': len(lane_items),
                'approved_count': len(lane_items) - lane_provisional,
                'provisional_count': lane_provisional,
                'status': 'approved_only' if lane_provisional == 0 else ('provisional_only' if lane_provisional == len(lane_items) else 'mixed_with_provisional'),
            }
        payload = {
            'generated_at': now_str(),
            'status': 'directional_watchlist_loaded' if provisional_item_count == 0 else 'directional_watchlist_loaded_with_provisional_fallback',
            'readiness_status': 'approved_only' if provisional_item_count == 0 else 'contains_provisional_fallback',
            'items': selected,
            'lanes': lanes,
            'lane_readiness': lane_readiness,
            'total_count': len(selected),
            'provisional_item_count': provisional_item_count,
            'approved_item_count': max(0, len(selected) - provisional_item_count),
            'sector_counts': dict(sector_counts),
            'net_exposure_proxy': dict(lane_counts),
            'min_counts': min_counts,
        }
        write_json(self.summary_path, payload)
        return str(self.summary_path), payload


if __name__ == '__main__':
    print(LiveWatchlistLoader().resolve_live_watchlist())
