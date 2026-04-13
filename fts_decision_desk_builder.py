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
from fts_signal_gate import passes_signal_gate


class DecisionDeskBuilder:
    MODULE_VERSION = 'v83_decision_desk_builder'

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

    @staticmethod
    def _metric_from_result(result: dict[str, Any], key: str, default: float = 0.0) -> float:
        nested = result.get('ai_features_latest', {}) if isinstance(result.get('ai_features_latest', {}), dict) else {}
        if key in result:
            return safe_float(result.get(key, default), default)
        return safe_float(nested.get(key, default), default)

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
                row = {
                    'Ticker': ticker,
                    'Structure': result.get('Structure', 'AI訊號'),
                    'Regime': result.get('Regime', '區間盤整'),
                    'AI_Proba': result.get('AI_Proba', 0.5),
                    'Realized_EV': result.get('Realized_EV', 0.0),
                    'Sample_Size': result.get('Sample_Size', 0),
                    'Weighted_Buy_Score': self._metric_from_result(result, 'Weighted_Buy_Score', 0.0),
                    'Weighted_Sell_Score': self._metric_from_result(result, 'Weighted_Sell_Score', 0.0),
                    'Score_Gap': self._metric_from_result(result, 'Score_Gap', 0.0),
                    'Kelly_Pos': 0.05,
                    'Health': 'KEEP',
                }
                gate, note = passes_signal_gate(row)
                row['SignalGatePassed'] = gate
                row['SignalGateNote'] = note
                rows.append(row)
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
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'output_path': str(self.output_path),
            'row_count': int(len(desk)),
            'market_climate': climate,
            'status': 'wave3_decision_desk_rules_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧭 decision desk builder ready: {self.runtime_path}')
        return self.runtime_path, payload
