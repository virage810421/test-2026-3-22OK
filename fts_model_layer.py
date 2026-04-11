# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from config import PARAMS
try:
    from fts_config import PATHS, CONFIG  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
        model_dir = Path('models')
        models_dir = model_dir
    class _Config:
        strict_feature_parity = True
        selected_features_min_count_for_live = 6
    PATHS = _Paths()
    CONFIG = _Config()

RUNTIME_PATH = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'model_layer_status.json'
MODEL_DIR = Path(getattr(PATHS, 'models_dir', getattr(PATHS, 'model_dir', Path('models'))))
SELECTED_PATH = MODEL_DIR / 'selected_features.pkl'

AI_MODELS: dict[str, Any] = {}
SELECTED_FEATURES: list[str] = []
STRICT_PARITY = bool(PARAMS.get('LIVE_REQUIRE_SELECTED_FEATURES', True)) or bool(getattr(CONFIG, 'strict_feature_parity', True))
MIN_LIVE_FEATURES = int(getattr(CONFIG, 'selected_features_min_count_for_live', 6))
SELECTED_SOURCE = str(SELECTED_PATH)


@dataclass
class ModelDecision:
    regime: str
    model_source: str
    proba: float
    realized_ev: float
    sample_size: int
    signal_confidence: float
    min_proba: float
    approved: bool
    veto_reasons: list[str]
    conviction_multiplier: float
    selected_features_ready: bool
    strict_parity: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _load_artifacts() -> None:
    global SELECTED_FEATURES, AI_MODELS, SELECTED_SOURCE
    SELECTED_FEATURES = []
    AI_MODELS = {}
    SELECTED_SOURCE = str(SELECTED_PATH)
    try:
        from fts_approved_artifact_loader import ApprovedArtifactLoader  # type: ignore
        if bool(PARAMS.get('APPROVED_FEATURE_SNAPSHOT_USE_IN_LIVE', True)):
            loader = ApprovedArtifactLoader()
            scope = str(PARAMS.get('APPROVED_DEFAULT_SCOPE', 'default'))
            approved = loader.load_approved_selected_features(scope=scope)
            if approved:
                SELECTED_FEATURES = approved
                SELECTED_SOURCE = str(loader.preferred_selected_features_path(True, scope))
    except Exception:
        pass
    if not SELECTED_FEATURES and SELECTED_PATH.exists():
        try:
            SELECTED_FEATURES = [str(x) for x in joblib.load(SELECTED_PATH) if str(x).strip()]
            SELECTED_FEATURES = list(dict.fromkeys(SELECTED_FEATURES))
            SELECTED_SOURCE = str(SELECTED_PATH)
        except Exception:
            SELECTED_FEATURES = []
    for regime in ['趨勢多頭', '區間盤整', '趨勢空頭']:
        p = MODEL_DIR / f'model_{regime}.pkl'
        if p.exists():
            try:
                AI_MODELS[regime] = joblib.load(p)
            except Exception:
                pass


def selected_features_ready() -> bool:
    return len(SELECTED_FEATURES) >= MIN_LIVE_FEATURES


def refresh_model_runtime() -> Path:
    _load_artifacts()
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'selected_features_present': bool(SELECTED_FEATURES),
        'selected_feature_count': len(SELECTED_FEATURES),
        'selected_features_ready': selected_features_ready(),
        'selected_features_source': SELECTED_SOURCE,
        'selected_features_min_required': MIN_LIVE_FEATURES,
        'loaded_regimes': sorted(list(AI_MODELS.keys())),
        'strict_parity': bool(STRICT_PARITY),
        'status': 'model_layer_ready' if selected_features_ready() else 'model_layer_degraded',
    }
    RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return RUNTIME_PATH


def _feature_input_from_row(latest_row, regime: str) -> tuple[dict[str, float], str]:
    signal_conf = _safe_float(latest_row.get('訊號信心分數(%)', 50.0), 50.0) / 100.0
    source = 'signal_confidence_fallback'
    if regime in AI_MODELS and selected_features_ready():
        try:
            from screening import extract_ai_features  # lazy import
            features_dict = extract_ai_features(latest_row)
            X = {f: _safe_float(features_dict.get(f, 0.0), 0.0) for f in SELECTED_FEATURES}
            proba = float(AI_MODELS[regime].predict_proba(pd.DataFrame([X]))[0][1])
            return {'proba': max(0.01, min(0.99, proba)), **X}, 'ai_model'
        except Exception:
            pass
    return {'proba': max(0.01, min(0.99, signal_conf))}, source


def evaluate_model_signal(latest_row, regime: str, min_proba: float = 0.5, base_multiplier: float = 1.0) -> ModelDecision:
    if not AI_MODELS and not SELECTED_FEATURES:
        refresh_model_runtime()

    realized_ev = _safe_float(latest_row.get('Realized_EV', 0.0), 0.0)
    sample_size = int(_safe_float(latest_row.get('歷史訊號樣本數', latest_row.get('Sample_Size', 0)), 0.0))
    signal_conf = _safe_float(latest_row.get('訊號信心分數(%)', latest_row.get('AI_Proba', 0.5) * 100.0), 50.0) / 100.0

    veto_reasons: list[str] = []
    selected_ready = selected_features_ready()
    if STRICT_PARITY and not selected_ready:
        model_source = 'parity_locked_signal_confidence'
        proba = max(0.01, min(0.99, signal_conf))
        veto_reasons.append(f'selected_features_not_ready:min_required_{MIN_LIVE_FEATURES}')
    else:
        payload, model_source = _feature_input_from_row(latest_row, regime)
        proba = float(payload.get('proba', signal_conf))

    if sample_size < 8:
        proba = 0.5 + (proba - 0.5) * 0.4
    elif sample_size < 15:
        proba = 0.5 + (proba - 0.5) * 0.7

    if proba < float(min_proba):
        veto_reasons.append(f'proba_below_threshold:{proba:.3f}<{float(min_proba):.3f}')
    if realized_ev <= 0:
        veto_reasons.append(f'non_positive_ev:{realized_ev:.4f}')

    ev_boost = 1.0
    if realized_ev > 1.5:
        ev_boost = 1.15
    elif realized_ev > 0.5:
        ev_boost = 1.05

    sample_boost = 1.0
    if sample_size >= 20:
        sample_boost = 1.10
    elif sample_size >= 10:
        sample_boost = 1.03

    conviction = max(0.0, min(proba * (float(base_multiplier) * 2.0) * ev_boost * sample_boost, 2.5))
    approved = len(veto_reasons) == 0

    decision = ModelDecision(
        regime=str(regime),
        model_source=model_source,
        proba=proba,
        realized_ev=realized_ev,
        sample_size=sample_size,
        signal_confidence=signal_conf,
        min_proba=float(min_proba),
        approved=approved,
        veto_reasons=veto_reasons,
        conviction_multiplier=conviction if approved else 0.0,
        selected_features_ready=selected_ready,
        strict_parity=bool(STRICT_PARITY),
    )
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_PATH.write_text(json.dumps(decision.as_dict(), ensure_ascii=False, indent=2), encoding='utf-8')
    return decision


refresh_model_runtime()
