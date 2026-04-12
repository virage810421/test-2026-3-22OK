
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any
import pandas as pd

from fts_prelive_runtime import PATHS, now_str, write_json, normalize_key
from fts_live_watchlist_registry import LiveWatchlistRegistry
from config import PARAMS

LANES = ['LONG', 'SHORT', 'RANGE']
LANE_SCORE_COL = {
    'LONG': 'Ticker_Promotion_Score_Long',
    'SHORT': 'Ticker_Promotion_Score_Short',
    'RANGE': 'Ticker_Promotion_Score_Range',
}
LANE_HR_COL = {'LONG': 'Long_HitRate', 'SHORT': 'Short_HitRate', 'RANGE': 'Range_HitRate'}
LANE_EV_COL = {'LONG': 'Long_OOT_EV', 'SHORT': 'Short_OOT_EV', 'RANGE': 'Range_OOT_EV'}

class LiveWatchlistPromoter:
    def __init__(self):
        self.runtime_dir = PATHS.runtime_dir
        self.registry = LiveWatchlistRegistry()
        self.summary_path = self.runtime_dir / 'live_watchlist_promoter.json'

    def _scoreboard(self) -> pd.DataFrame:
        for p in [self.runtime_dir / 'training_ticker_scoreboard.csv', PATHS.base_dir / 'training_ticker_scoreboard.csv']:
            if p.exists():
                try:
                    return pd.read_csv(p)
                except Exception:
                    continue
        return pd.DataFrame()

    def _records_for_lane(self, df: pd.DataFrame, lane: str) -> list[dict[str, Any]]:
        if df is None or df.empty:
            return []
        score_col = LANE_SCORE_COL[lane]
        hr_col = LANE_HR_COL[lane]
        ev_col = LANE_EV_COL[lane]
        if score_col not in df.columns:
            df = df.copy()
            df[score_col] = df.get('training_universe_score', 0.0)
        items = []
        lane_max = int(PARAMS.get(f'LIVE_WATCHLIST_{lane}_MAX_NAMES', 8))
        for _, row in df.sort_values(score_col, ascending=False).head(max(lane_max * 3, lane_max)).iterrows():
            ticker = str(row.get('ticker') or row.get('Ticker') or row.get('Ticker SYMBOL') or row.get('股票代號') or '').strip()
            if not ticker:
                continue
            item = {
                'ticker': ticker,
                'lane': lane,
                'promotion_score': float(row.get(score_col, 0.0) or 0.0),
                'oot_ev': float(row.get(ev_col, 0.0) or 0.0),
                'hit_rate': float(row.get(hr_col, 0.0) or 0.0),
                'sector': str(row.get('industry') or row.get('產業類別') or row.get('sector') or '未知'),
                'liquidity_score': float(row.get('流動性分數', row.get('Liquidity_Score', 1.0)) or 0.0),
                'feature_coverage': float(row.get('selected_feature_coverage', row.get('feature_coverage', 1.0)) or 0.0),
                'approval_reason': 'scoreboard_promotion',
            }
            items.append(item)
        return items[:lane_max]

    def build(self) -> tuple[str, dict[str, Any]]:
        df = self._scoreboard()
        payload = {'generated_at': now_str(), 'status': 'directional_watchlists_promoted', 'lanes': {}}
        for lane in LANES:
            items = self._records_for_lane(df, lane)
            candidate_path = self.runtime_dir / f'candidate_live_watchlist_{lane.lower()}.json'
            approved_path = self.runtime_dir / f'approved_live_watchlist_{lane.lower()}.json'
            candidate_payload = {'generated_at': now_str(), 'lane': lane, 'items': items, 'count': len(items), 'promotion_batch_id': now_str()}
            approved_payload = {'generated_at': now_str(), 'lane': lane, 'items': items, 'count': len(items), 'approved_version': now_str(), 'promotion_batch_id': now_str()}
            write_json(candidate_path, candidate_payload)
            write_json(approved_path, approved_payload)
            self.registry.register_watchlist(lane, approved_payload, str(approved_path))
            payload['lanes'][lane] = {'candidate_path': str(candidate_path), 'approved_path': str(approved_path), 'count': len(items)}
        write_json(self.summary_path, payload)
        return str(self.summary_path), payload
