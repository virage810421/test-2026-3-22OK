# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 4 files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_live_watchlist_registry.py
# ==============================================================================
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


# ==============================================================================
# Merged from: fts_live_watchlist_loader.py
# ==============================================================================
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
            'liquidity_score': 1.0, 'feature_coverage': 1.0, 'source': 'lane_fallback_provisional',
            'approval_reason': 'lane_fallback_provisional', 'oot_ev': 0.0, 'hit_rate': 0.5,
            'provisional': True, 'readiness_tier': 'fallback_only',
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
            item['provisional'] = bool(item.get('provisional', False))
            item['readiness_tier'] = str(item.get('readiness_tier', 'approved')).strip() or 'approved'
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


# ==============================================================================
# Merged from: fts_live_watchlist_promoter.py
# ==============================================================================
from typing import Any
import pandas as pd

from fts_prelive_runtime import PATHS, now_str, write_json
from config import PARAMS, WATCH_LIST, TRAINING_POOL

LANES = ['LONG', 'SHORT', 'RANGE']
LANE_SCORE_COL = {'LONG': 'Ticker_Promotion_Score_Long', 'SHORT': 'Ticker_Promotion_Score_Short', 'RANGE': 'Ticker_Promotion_Score_Range'}
LANE_HR_COL = {'LONG': 'Long_HitRate', 'SHORT': 'Short_HitRate', 'RANGE': 'Range_HitRate'}
LANE_EV_COL = {'LONG': 'Long_OOT_EV', 'SHORT': 'Short_OOT_EV', 'RANGE': 'Range_OOT_EV'}
SEED_SCORE = {'LONG': float(PARAMS.get('LONG_SEED_SCORE', 0.45)), 'SHORT': float(PARAMS.get('SHORT_SEED_SCORE', 0.25)), 'RANGE': float(PARAMS.get('RANGE_SEED_SCORE', 0.35))}

class LiveWatchlistPromoter:
    def __init__(self):
        self.runtime_dir = PATHS.runtime_dir
        self.registry = LiveWatchlistRegistry()
        self.summary_path = self.runtime_dir / 'live_watchlist_promoter.json'

    def _scoreboard(self) -> pd.DataFrame:
        candidates = [self.runtime_dir / 'training_ticker_scoreboard.csv', PATHS.base_dir / 'training_ticker_scoreboard.csv']
        for p in candidates:
            if p.exists():
                try:
                    df = pd.read_csv(p)
                    if not df.empty:
                        return df
                except Exception:
                    pass
        if bool(PARAMS.get('DIRECTIONAL_SCOREBOARD_AUTO_BUILD', True)):
            try:
                from fts_training_ticker_scoreboard import build_scoreboard
                build_scoreboard()
                for p in candidates:
                    if p.exists():
                        try:
                            df = pd.read_csv(p)
                            if not df.empty:
                                return df
                        except Exception:
                            pass
            except Exception:
                pass
        return pd.DataFrame()

    def _seed_items(self, lane: str, count: int) -> list[dict[str, Any]]:
        universe = list(dict.fromkeys(list(WATCH_LIST) + list(TRAINING_POOL)))
        if lane == 'SHORT':
            universe = list(reversed(universe))
        elif lane == 'RANGE':
            universe = universe[::2] + universe[1::2]
        items = []
        for t in universe[:count]:
            items.append({
                'ticker': t,
                'lane': lane,
                'promotion_score': SEED_SCORE[lane],
                'oot_ev': 0.0,
                'hit_rate': 0.5 if lane == 'LONG' else 0.48,
                'sector': '未知',
                'liquidity_score': 1.0,
                'feature_coverage': 1.0,
                'approval_reason': 'seed_from_core_watchlist',
                'provisional': True,
                'readiness_tier': 'seed_only',
            })
        return items

    def _records_for_lane(self, df: pd.DataFrame, lane: str) -> list[dict[str, Any]]:
        lane_max = int(PARAMS.get(f'LIVE_WATCHLIST_{lane}_MAX_NAMES', 8))
        min_count = max(int(PARAMS.get('DIRECTIONAL_PROMOTION_MIN_COUNT', 3)), int(PARAMS.get(f'LIVE_WATCHLIST_MIN_PER_LANE_{lane}', 2)))
        if df is None or df.empty:
            return self._seed_items(lane, max(min_count, lane_max // 2))
        score_col = LANE_SCORE_COL[lane]
        hr_col = LANE_HR_COL[lane]
        ev_col = LANE_EV_COL[lane]
        if score_col not in df.columns:
            df = df.copy()
            df[score_col] = float(SEED_SCORE[lane])
        items = []
        for _, row in df.sort_values(score_col, ascending=False).iterrows():
            ticker = str(row.get('ticker') or row.get('Ticker') or row.get('Ticker SYMBOL') or row.get('股票代號') or '').strip()
            if not ticker:
                continue
            item = {
                'ticker': ticker,
                'lane': lane,
                'promotion_score': float(row.get(score_col, SEED_SCORE[lane]) or 0.0),
                'oot_ev': float(row.get(ev_col, 0.0) or 0.0),
                'hit_rate': float(row.get(hr_col, 0.0) or 0.0),
                'sector': str(row.get('industry') or row.get('產業類別') or row.get('sector') or '未知'),
                'liquidity_score': float(row.get('流動性分數', row.get('Liquidity_Score', 1.0)) or 0.0),
                'feature_coverage': float(row.get('selected_feature_coverage', row.get('feature_coverage', 1.0)) or 0.0),
                'approval_reason': 'scoreboard_promotion',
                'provisional': False,
                'readiness_tier': 'approved',
            }
            items.append(item)
            if len(items) >= lane_max:
                break
        if len(items) < min_count and bool(PARAMS.get('DIRECTIONAL_SEED_FROM_CORE_WATCHLIST', True)):
            seen = {x['ticker'] for x in items}
            for extra in self._seed_items(lane, min_count - len(items)):
                if extra['ticker'] not in seen:
                    items.append(extra)
                    seen.add(extra['ticker'])
        return items[:lane_max]

    def build(self) -> tuple[str, dict[str, Any]]:
        df = self._scoreboard()
        payload = {'generated_at': now_str(), 'status': 'directional_watchlists_promoted', 'lanes': {}}
        for lane in LANES:
            items = self._records_for_lane(df, lane)
            provisional_count = sum(1 for x in items if bool(x.get('provisional', False)))
            candidate_path = self.runtime_dir / f'candidate_live_watchlist_{lane.lower()}.json'
            approved_path = self.runtime_dir / f'approved_live_watchlist_{lane.lower()}.json'
            candidate_payload = {
                'generated_at': now_str(), 'lane': lane, 'items': items, 'count': len(items), 'promotion_batch_id': now_str(),
                'provisional_count': provisional_count, 'approved_count': len(items) - provisional_count,
                'readiness_status': 'approved_only' if provisional_count == 0 else 'contains_provisional_seed',
            }
            approved_payload = {
                'generated_at': now_str(), 'lane': lane, 'items': items, 'count': len(items), 'approved_version': now_str(), 'promotion_batch_id': now_str(),
                'provisional_count': provisional_count, 'approved_count': len(items) - provisional_count,
                'readiness_status': 'approved_only' if provisional_count == 0 else 'contains_provisional_seed',
            }
            write_json(candidate_path, candidate_payload)
            write_json(approved_path, approved_payload)
            self.registry.register_watchlist(lane, approved_payload, str(approved_path))
            payload['lanes'][lane] = {
                'candidate_path': str(candidate_path), 'approved_path': str(approved_path), 'count': len(items),
                'provisional_count': provisional_count, 'approved_count': len(items) - provisional_count,
                'readiness_status': approved_payload['readiness_status'],
            }
        write_json(self.summary_path, payload)
        return str(self.summary_path), payload


# ==============================================================================
# Merged from: fts_watchlist_service.py
# ==============================================================================
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
            # merged: use LiveWatchlistLoader directly
            return list(dict.fromkeys(LiveWatchlistLoader().load_live_watchlist()))
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
            # merged: use LiveWatchlistLoader directly
            _, approved_loader_payload = LiveWatchlistLoader().build_summary()
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
