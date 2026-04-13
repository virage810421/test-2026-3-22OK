
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any
import pandas as pd

from fts_prelive_runtime import PATHS, now_str, write_json
from fts_live_watchlist_registry import LiveWatchlistRegistry
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
<<<<<<< HEAD
                'provisional': True,
                'readiness_tier': 'seed_only',
=======
>>>>>>> ad1db6bec225a276b4ad4c7df6c049d994a30092
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
