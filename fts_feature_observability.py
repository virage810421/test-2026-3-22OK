# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib

from fts_config import PATHS, CONFIG
from fts_feature_catalog import LIVE_SAFE_FEATURES, APPROVED_LIVE_DIRECTIONAL_FEATURES
from fts_utils import now_str

MODEL_DIR = Path(getattr(PATHS, 'models_dir', getattr(PATHS, 'model_dir', Path('models'))))


class FeatureObservability:
    MODULE_VERSION = 'v87_feature_observability_vetted_live'

    def __init__(self):
        self.out = PATHS.runtime_dir / 'feature_observability.json'
        self.model_dir = MODEL_DIR

    def _load_features(self, name: str) -> list[str]:
        p = self.model_dir / name
        if not p.exists():
            return []
        try:
            return [str(x) for x in joblib.load(p) if str(x).strip()]
        except Exception:
            return []

    def build(self) -> tuple[str, dict[str, Any]]:
        shared = self._load_features('selected_features.pkl')
        directional = {
            'LONG': self._load_features('selected_features_long.pkl'),
            'SHORT': self._load_features('selected_features_short.pkl'),
            'RANGE': self._load_features('selected_features_range.pkl'),
        }
        selected_union = sorted(set(shared).union(*[set(v) for v in directional.values()]))
        live_directional_enabled = bool(getattr(CONFIG, 'enable_directional_features_in_live', True)) and (not bool(getattr(CONFIG, 'force_shared_feature_universe', False)))
        policy = 'shared_only' if not live_directional_enabled else str(getattr(CONFIG, 'live_feature_policy', 'shared_plus_vetted_directional'))

        directional_union = sorted(set().union(*[set(v) for v in directional.values()]))
        approved_directional_live = sorted([f for f in directional_union if f in APPROVED_LIVE_DIRECTIONAL_FEATURES])
        blocked_directional_live = sorted([f for f in directional_union if f not in approved_directional_live]) if live_directional_enabled else sorted(directional_union)
        shared_live = sorted([f for f in selected_union if f in LIVE_SAFE_FEATURES and f not in APPROVED_LIVE_DIRECTIONAL_FEATURES])

        artifacts = {
            'selected_features.pkl': (self.model_dir / 'selected_features.pkl').exists(),
            'selected_features_long.pkl': (self.model_dir / 'selected_features_long.pkl').exists(),
            'selected_features_short.pkl': (self.model_dir / 'selected_features_short.pkl').exists(),
            'selected_features_range.pkl': (self.model_dir / 'selected_features_range.pkl').exists(),
            'feature_parity_status.json': (PATHS.runtime_dir / 'feature_parity_status.json').exists(),
            'live_feature_mount.json': (PATHS.runtime_dir / 'live_feature_mount.json').exists(),
        }
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'strict_feature_parity': bool(getattr(CONFIG, 'strict_feature_parity', True)),
            'force_shared_feature_universe': bool(getattr(CONFIG, 'force_shared_feature_universe', False)),
            'directional_live_enabled': live_directional_enabled,
            'selected_feature_count': len(selected_union),
            'shared_selected_count': len(shared),
            'directional_selected_counts': {k: len(v) for k, v in directional.items()},
            'shared_live_features': shared_live,
            'approved_live_directional_features': approved_directional_live,
            'mounted_directional_live_features': approved_directional_live if live_directional_enabled else [],
            'blocked_directional_live_features': blocked_directional_live,
            'research_only_features': blocked_directional_live,
            'live_feature_policy': policy,
            'artifacts': artifacts,
            'status': 'observable' if selected_union else 'waiting_for_selected_features',
        }
        self.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return str(self.out), payload
