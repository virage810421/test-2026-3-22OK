# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import now_str, log, resolve_decision_csv, safe_float


class MarketClimateService:
    MODULE_VERSION = 'v83_market_climate_service'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'market_climate_service.json'

    def analyze_market_climate(self) -> dict[str, Any]:
        decision_path = resolve_decision_csv()
        if decision_path.exists():
            try:
                df = pd.read_csv(decision_path, encoding='utf-8-sig')
            except Exception:
                df = pd.read_csv(decision_path)
        else:
            df = pd.DataFrame()
        if df.empty:
            return {'regime': '未知', 'risk_mode': 'neutral', 'avg_ai_proba': 0.5, 'avg_score_gap': 0.0, 'avg_ev': 0.0}
        avg_ai = safe_float(pd.to_numeric(df.get('AI_Proba', pd.Series([0.5]*len(df))), errors='coerce').mean(), 0.5)
        avg_gap = safe_float(pd.to_numeric(df.get('Score_Gap', pd.Series([0.0]*len(df))), errors='coerce').mean(), 0.0)
        avg_ev = safe_float(pd.to_numeric(df.get('Expected_Return', df.get('Heuristic_EV', df.get('Live_EV', df.get('Realized_EV', pd.Series([0.0]*len(df)))))), errors='coerce').mean(), 0.0)
        if avg_ai >= 0.57 and avg_gap > 0 and avg_ev > 0:
            regime = '趨勢多頭'
            risk_mode = 'risk_on'
        elif avg_ai <= 0.47 or avg_gap < 0 or avg_ev < 0:
            regime = '趨勢空頭'
            risk_mode = 'risk_off'
        else:
            regime = '區間盤整'
            risk_mode = 'balanced'
        return {'regime': regime, 'risk_mode': risk_mode, 'avg_ai_proba': round(avg_ai, 4), 'avg_score_gap': round(avg_gap, 4), 'avg_expected_return': round(avg_ev, 4)}

    def should_retrain(self) -> dict[str, Any]:
        metrics = self.analyze_market_climate()
        should = metrics['risk_mode'] != 'risk_off'
        return {'should_retrain': should, 'reason': 'risk_off 時暫停重訓' if not should else '市場狀態允許重訓', 'market_climate': metrics}

    def build_summary(self) -> tuple[Any, dict[str, Any]]:
        climate = self.analyze_market_climate()
        retrain = self.should_retrain()
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'market_climate': climate,
            'retrain_policy': retrain,
            'status': 'wave3_market_climate_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🌤️ market climate ready: {self.runtime_path}')
        return self.runtime_path, payload
