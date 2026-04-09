# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from fts_config import PATHS
from fts_utils import now_str, safe_float, safe_int, log


class FeatureService:
    MODULE_VERSION = 'v83_feature_service_detached'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'feature_service.json'

    def extract_ai_features(self, row: Mapping[str, Any]) -> dict[str, Any]:
        close_p = safe_float(row.get('Close', 1), 1)
        open_p = safe_float(row.get('Open', 1), 1)
        high_p = safe_float(row.get('High', close_p), close_p)
        low_p = safe_float(row.get('Low', close_p), close_p)
        ma20 = safe_float(row.get('MA20', close_p), close_p)
        bb_upper = safe_float(row.get('BB_Upper', row.get('Upper', close_p)), close_p)
        bb_lower = safe_float(row.get('BB_Lower', row.get('Lower', close_p)), close_p)
        volume = safe_float(row.get('Volume', 0), 0)
        vol_ma20 = safe_float(row.get('Vol_MA20', 0), 0)

        features = {
            'K_Body_Pct': ((close_p - open_p) / open_p) if open_p else 0.0,
            'Upper_Shadow': ((high_p - max(close_p, open_p)) / close_p) if close_p else 0.0,
            'Lower_Shadow': ((min(close_p, open_p) - low_p) / close_p) if close_p else 0.0,
            'Dist_to_MA20': ((close_p - ma20) / (ma20 + 1e-4)),
            'Volume_Ratio': (volume / (vol_ma20 + 1e-3)) if vol_ma20 else 1.0,
            'BB_Width': safe_float(row.get('BB_Width', 0), 0),
            'RSI': safe_float(row.get('RSI', 50), 50),
            'MACD_Hist': safe_float(row.get('MACD_Hist', 0), 0),
            'ADX': safe_float(row.get('ADX14', row.get('ADX', 25)), 25),
            'Foreign_Ratio': safe_float(row.get('Foreign_Ratio', 0), 0),
            'Trust_Ratio': safe_float(row.get('Trust_Ratio', 0), 0),
            'Total_Ratio': safe_float(row.get('Total_Ratio', 0), 0),
            'Foreign_Consec_Days': safe_int(row.get('Foreign_Consecutive', 0), 0),
            'Trust_Consec_Days': safe_int(row.get('Trust_Consecutive', 0), 0),
            'Weighted_Buy_Score': safe_float(row.get('Weighted_Buy_Score', row.get('Buy_Score', 0)), 0),
            'Weighted_Sell_Score': safe_float(row.get('Weighted_Sell_Score', row.get('Sell_Score', 0)), 0),
            'Score_Gap': safe_float(row.get('Score_Gap', 0), 0),
            'Signal_Conflict': safe_int(row.get('Signal_Conflict', 0), 0),
        }

        for key in [
            'buy_c2', 'buy_c3', 'buy_c4', 'buy_c5', 'buy_c6', 'buy_c7', 'buy_c8', 'buy_c9',
            'sell_c2', 'sell_c3', 'sell_c4', 'sell_c5', 'sell_c6', 'sell_c7', 'sell_c8', 'sell_c9',
        ]:
            features[key] = safe_int(row.get(key, 0), 0)

        fake_breakout = bool(row.get('Fake_Breakout', False))
        bear_trap = bool(row.get('Bear_Trap', False))
        features['Trap_Signal'] = 1 if fake_breakout else (-1 if bear_trap else 0)
        features['Vol_Squeeze'] = safe_int(row.get('Vol_Squeeze', False), 0)
        features['Absorption'] = safe_int(row.get('Absorption', False), 0)

        features['MR_Long_Spring'] = 1 if (
            (low_p < bb_lower) and (volume < vol_ma20 if vol_ma20 else False) and (features['Total_Ratio'] >= -0.01)
        ) else 0
        features['MR_Short_Trap'] = 1 if (
            (high_p > bb_upper) and (features['Upper_Shadow'] > 0.02) and (features['RSI'] > 65)
        ) else 0
        features['MR_Long_Accumulation'] = 1 if (features['RSI'] < 35 and features['Total_Ratio'] > 0.05) else 0
        features['MR_Short_Distribution'] = 1 if (features['RSI'] > 65 and features['Total_Ratio'] < -0.05) else 0
        return features

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        sample = self.extract_ai_features({'Close': 100, 'Open': 98, 'High': 102, 'Low': 97, 'Volume': 1200, 'Vol_MA20': 1000})
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'legacy_screening_dependency': False,
            'sample_feature_count': len(sample),
            'status': 'feature_service_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧩 feature service ready: {self.runtime_path}')
        return self.runtime_path, payload
