# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import now_str, log, resolve_decision_csv, safe_float
from fts_watchlist_service import WatchlistService
from fts_market_climate_service import MarketClimateService
from fts_screening_engine import ScreeningEngine
from fts_signal_gate import evaluate_signal_gate


class DecisionDeskBuilder:
    MODULE_VERSION = 'v86_decision_desk_builder_fallback_hardened'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'decision_desk_builder.json'
        self.output_path = PATHS.data_dir / 'normalized_decision_output.csv'
        self.watchlist = WatchlistService()
        self.market = MarketClimateService()
        self.screen = ScreeningEngine()

    def _normalize_existing(self, path) -> pd.DataFrame:
        try:
            df = pd.read_csv(path, encoding='utf-8-sig')
        except Exception:
            df = pd.read_csv(path)
        rename_map = {}
        if 'Ticker SYMBOL' in df.columns and 'Ticker' not in df.columns:
            rename_map['Ticker SYMBOL'] = 'Ticker'
        if '結構' in df.columns and 'Structure' not in df.columns:
            rename_map['結構'] = 'Structure'
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _fallback_row(self, ticker: str, result: dict[str, Any]) -> dict[str, Any]:
        direction = str(result.get('Golden_Type', result.get('Direction', 'LONG')) or 'LONG')
        row = {
            'Ticker': ticker,
            'Structure': result.get('Structure', 'AI訊號'),
            'Regime': result.get('Regime', '區間盤整'),
            'Direction': direction,
            'Golden_Type': direction,
            'AI_Proba': safe_float(result.get('AI_Proba', 0.5), 0.5),
            'Realized_EV': safe_float(result.get('Realized_EV', 0.0), 0.0),
            'Sample_Size': int(safe_float(result.get('Sample_Size', result.get('歷史訊號樣本數', 0)), 0)),
            'Weighted_Buy_Score': safe_float(result.get('Weighted_Buy_Score', result.get('Buy_Score', 0.0)), 0.0),
            'Weighted_Sell_Score': safe_float(result.get('Weighted_Sell_Score', result.get('Sell_Score', 0.0)), 0.0),
            'Score_Gap': safe_float(result.get('Score_Gap', 0.0), 0.0),
            'Kelly_Pos': 0.0,
            'Health': 'REVIEW_REQUIRED',
            'DecisionSource': 'fallback_build',
            'RequiresReview': True,
            'FallbackBuild': True,
            'CanAutoSubmit': False,
        }
        gate = evaluate_signal_gate(row)
        row['SignalGatePassed'] = bool(gate['passed'])
        row['SignalGateNote'] = gate['note']
        row['SignalGateBlockers'] = '|'.join(gate['blockers'])
        row['SignalGateWarnings'] = '|'.join(gate['warnings'])
        row['HeuristicRole'] = gate['heuristic_role']
        return row

    def build_decision_desk(self, limit: int = 12) -> pd.DataFrame:
        existing = resolve_decision_csv()
        if existing.exists():
            df = self._normalize_existing(existing)
        else:
            rows = []
            for ticker in self.watchlist.build_final_watchlist(limit=limit)[:limit]:
                result = self.screen.inspect_stock(ticker)
                if not result:
                    continue
                rows.append(self._fallback_row(ticker, result))
            df = pd.DataFrame(rows)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        except Exception:
            pass
        return df

    def build_summary(self) -> tuple[Any, dict[str, Any]]:
        desk = self.build_decision_desk()
        climate = self.market.analyze_market_climate()
        fallback_rows = int((desk.get('FallbackBuild', pd.Series(dtype=bool)).fillna(False)).sum()) if not desk.empty else 0
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'output_path': str(self.output_path),
            'row_count': int(len(desk)),
            'fallback_row_count': fallback_rows,
            'market_climate': climate,
            'status': 'decision_desk_ready_with_review_fallbacks' if fallback_rows > 0 else 'decision_desk_ready',
            'fallback_policy': {
                'kelly_default': 0.0,
                'health_default': 'REVIEW_REQUIRED',
                'auto_submit_allowed': False,
            },
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧭 decision desk builder ready: {self.runtime_path}')
        return self.runtime_path, payload
