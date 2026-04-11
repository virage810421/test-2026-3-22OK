# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from config import PARAMS
from fts_config import PATHS
from fts_utils import now_str, log
from fts_training_ticker_scoreboard import TrainingTickerScoreboard
from fts_live_watchlist_registry import save_candidate, approve_latest_candidate, summary as registry_summary


class LiveWatchlistPromoter:
    MODULE_VERSION = 'v86_live_watchlist_promoter'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'live_watchlist_promoter.json'
        self.scoreboard = TrainingTickerScoreboard()

    def _load_scoreboard(self) -> pd.DataFrame:
        df = self.scoreboard.load_scoreboard()
        if df.empty:
            self.scoreboard.build_from_dataset()
            df = self.scoreboard.load_scoreboard()
        return df

    @staticmethod
    def _bounded(series: pd.Series, denom: float) -> pd.Series:
        return series.clip(lower=0.0).div(max(float(denom), 1e-9)).clip(upper=1.0)

    def _rank(self, df: pd.DataFrame) -> pd.DataFrame:
        ranked = df.copy()
        for col in ['oot_ev', 'oot_hit_rate', 'walk_forward_ev', 'stability_score', 'liquidity_score', 'selected_feature_coverage']:
            ranked[col] = pd.to_numeric(ranked.get(col, 0.0), errors='coerce').fillna(0.0)
        for col in ['oot_samples', 'train_samples']:
            ranked[col] = pd.to_numeric(ranked.get(col, 0), errors='coerce').fillna(0).astype(int)
        ranked['promotion_score'] = (
            30.0 * self._bounded(ranked['oot_ev'], 0.05)
            + 20.0 * ranked['oot_hit_rate'].clip(0.0, 1.0)
            + 20.0 * self._bounded(ranked['walk_forward_ev'], 0.05)
            + 10.0 * ((ranked['oot_samples'] + ranked['train_samples']).clip(lower=0).div(60.0).clip(upper=1.0))
            + 10.0 * ranked['stability_score'].clip(0.0, 1.0)
            + 10.0 * ranked['liquidity_score'].clip(0.0, 1.0)
        )
        ranked = ranked.sort_values(['promotion_score', 'oot_ev', 'oot_hit_rate'], ascending=[False, False, False]).reset_index(drop=True)
        ranked['global_rank'] = np.arange(1, len(ranked) + 1)
        ranked['sector_rank'] = ranked.groupby('sector')['promotion_score'].rank(method='dense', ascending=False).astype(int)
        ranked['regime_rank'] = ranked.groupby('regime')['promotion_score'].rank(method='dense', ascending=False).astype(int)
        ranked['feature_integrity_ok'] = ranked['selected_feature_coverage'] >= float(PARAMS.get('LIVE_WATCHLIST_MIN_FEATURE_COVERAGE', 0.95))
        ranked['liquidity_ok'] = ranked['liquidity_score'] >= float(PARAMS.get('LIVE_WATCHLIST_MIN_LIQUIDITY_SCORE', 0.10))
        return ranked

    def _approve_gate(self, ranked: pd.DataFrame) -> pd.DataFrame:
        gated = ranked.copy()
        min_hit = float(PARAMS.get('LIVE_WATCHLIST_MIN_OOT_HIT_RATE', 0.52))
        min_ev = float(PARAMS.get('LIVE_WATCHLIST_MIN_OOT_EV', 0.0))
        min_samples = int(PARAMS.get('LIVE_WATCHLIST_MIN_TOTAL_SAMPLES', 30))
        max_mdd = float(PARAMS.get('LIVE_WATCHLIST_MAX_DRAWDOWN', 0.25))
        recent_floor = float(PARAMS.get('LIVE_WATCHLIST_MIN_RECENT_TREND', -0.02))
        gated = gated[
            (gated['oot_ev'] > min_ev)
            & (gated['oot_hit_rate'] >= min_hit)
            & ((gated['oot_samples'] + gated['train_samples']) >= min_samples)
            & (gated['max_drawdown'] <= max_mdd)
            & (gated['recent_20d_score_trend'] >= recent_floor)
            & (gated['feature_integrity_ok'])
        ].copy()
        max_total = int(PARAMS.get('LIVE_WATCHLIST_MAX_NAMES', 12))
        max_per_sector = int(PARAMS.get('LIVE_WATCHLIST_MAX_PER_SECTOR', 3))
        if gated.empty:
            return gated
        chosen_rows: list[pd.Series] = []
        sector_counts: dict[str, int] = {}
        for _, row in gated.sort_values(['promotion_score', 'sector_rank', 'global_rank'], ascending=[False, True, True]).iterrows():
            sector = str(row.get('sector', 'OTHERS'))
            if sector_counts.get(sector, 0) >= max_per_sector:
                continue
            chosen_rows.append(row)
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            if len(chosen_rows) >= max_total:
                break
        return pd.DataFrame(chosen_rows)

    def run(self, auto_approve: bool = True) -> tuple[Path, dict[str, Any]]:
        scoreboard = self._load_scoreboard()
        if scoreboard.empty:
            payload = {
                'generated_at': now_str(),
                'module_version': self.MODULE_VERSION,
                'status': 'scoreboard_missing',
            }
            self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.runtime_path, payload

        ranked = self._rank(scoreboard)
        approved_df = self._approve_gate(ranked)
        candidate_summary = {
            'row_count': int(len(ranked)),
            'approved_count': int(len(approved_df)),
            'top_tickers': ranked['ticker'].head(10).tolist(),
            'top_by_sector': {
                str(sec): grp.sort_values('promotion_score', ascending=False)['ticker'].head(3).tolist()
                for sec, grp in ranked.groupby('sector')
            },
        }
        candidate_payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'candidate_summary': candidate_summary,
            'rows': ranked.to_dict(orient='records'),
            'approved_rows': approved_df.to_dict(orient='records'),
            'status': 'candidate_live_watchlist_ready',
        }
        saved = save_candidate(candidate_payload, source_module='fts_live_watchlist_promoter')
        approved = None
        if auto_approve:
            approved = approve_latest_candidate(approver='auto_pipeline', note='training winners promoted after approval gate')
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'candidate_id': saved.get('candidate_id'),
            'candidate_summary': candidate_summary,
            'approved_ticker_count': len((approved or {}).get('rows', [])) if approved else 0,
            'registry_summary': registry_summary(),
            'status': 'live_watchlist_promoter_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🚦 live watchlist promoter ready: {self.runtime_path}')
        return self.runtime_path, payload


if __name__ == '__main__':
    path, payload = LiveWatchlistPromoter().run(auto_approve=True)
    print(path)
    print(payload.get('status'))
