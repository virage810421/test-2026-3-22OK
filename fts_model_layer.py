# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
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
DIRECTIONAL_MODELS: dict[str, dict[str, Any]] = {'LONG': {}, 'SHORT': {}, 'RANGE': {}}
DIRECTIONAL_FEATURES: dict[str, list[str]] = {'LONG': [], 'SHORT': [], 'RANGE': []}
STRICT_PARITY = bool(PARAMS.get('LIVE_REQUIRE_SELECTED_FEATURES', True)) or bool(getattr(CONFIG, 'strict_feature_parity', True))
MIN_LIVE_FEATURES = int(getattr(CONFIG, 'selected_features_min_count_for_live', 6))
MIN_DIRECTIONAL_LIVE_FEATURES = int(getattr(CONFIG, 'live_directional_min_feature_count', 4))
ENABLE_DIRECTIONAL = bool(PARAMS.get('ENABLE_DIRECTIONAL_MODEL_LOADING', True))
ENABLE_BOOTSTRAP = bool(PARAMS.get('ENABLE_DIRECTIONAL_ARTIFACT_BOOTSTRAP', True))
ALLOW_HEURISTIC_FALLBACK = bool(getattr(CONFIG, 'allow_heuristic_model_fallback', False))
DIRECTIONAL_LIVE_ENABLED = bool(getattr(CONFIG, 'enable_directional_features_in_live', True)) and (not bool(getattr(CONFIG, 'force_shared_feature_universe', False)))
ALLOW_SHARED_FALLBACK_PER_LANE = bool(getattr(CONFIG, 'live_allow_directional_shared_fallback', True))


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
    model_scope: str = 'SHARED'
    model_bucket: str = 'SHARED'
    feature_scope: str = 'SHARED'
    direction_scope: str = 'SHARED'
    heuristic_fallback_active: bool = False
    debug: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _normalize_scope(value: Any) -> str:
    s = str(value or '').strip().upper()
    return s if s in {'LONG', 'SHORT', 'RANGE'} else 'SHARED'



def _lane_seed_from_shared(scope: str) -> None:
    scope = _normalize_scope(scope)
    if scope == 'SHARED':
        return
    if not DIRECTIONAL_FEATURES.get(scope) and SELECTED_FEATURES and ENABLE_BOOTSTRAP:
        DIRECTIONAL_FEATURES[scope] = list(SELECTED_FEATURES)
    if not DIRECTIONAL_MODELS.get(scope) and AI_MODELS and ENABLE_BOOTSTRAP:
        DIRECTIONAL_MODELS[scope] = dict(AI_MODELS)


def _load_artifacts() -> None:
    global SELECTED_FEATURES, AI_MODELS, DIRECTIONAL_MODELS, DIRECTIONAL_FEATURES
    SELECTED_FEATURES = []
    AI_MODELS = {}
    DIRECTIONAL_MODELS = {'LONG': {}, 'SHORT': {}, 'RANGE': {}}
    DIRECTIONAL_FEATURES = {'LONG': [], 'SHORT': [], 'RANGE': []}
    if SELECTED_PATH.exists():
        try:
            SELECTED_FEATURES = [str(x) for x in joblib.load(SELECTED_PATH) if str(x).strip()]
            SELECTED_FEATURES = list(dict.fromkeys(SELECTED_FEATURES))
        except Exception:
            SELECTED_FEATURES = []
    for regime in ['趨勢多頭', '區間盤整', '趨勢空頭']:
        p = MODEL_DIR / f'model_{regime}.pkl'
        if p.exists():
            try:
                AI_MODELS[regime] = joblib.load(p)
            except Exception:
                pass
    if ENABLE_DIRECTIONAL:
        for scope in ['LONG', 'SHORT', 'RANGE']:
            sf = MODEL_DIR / f'selected_features_{scope.lower()}.pkl'
            if sf.exists():
                try:
                    DIRECTIONAL_FEATURES[scope] = [str(x) for x in joblib.load(sf) if str(x).strip()]
                except Exception:
                    DIRECTIONAL_FEATURES[scope] = []
            for regime in ['趨勢多頭', '區間盤整', '趨勢空頭']:
                p = MODEL_DIR / f'model_{scope.lower()}_{regime}.pkl'
                if p.exists():
                    try:
                        DIRECTIONAL_MODELS[scope][regime] = joblib.load(p)
                    except Exception:
                        pass
            _lane_seed_from_shared(scope)


def _selected_features_for_scope(scope: str = 'SHARED') -> list[str]:
    scope = _normalize_scope(scope)
    if scope == 'SHARED' or not ENABLE_DIRECTIONAL or not DIRECTIONAL_LIVE_ENABLED:
        return list(dict.fromkeys(SELECTED_FEATURES))
    lane = list(DIRECTIONAL_FEATURES.get(scope, []))
    shared = list(SELECTED_FEATURES) if ALLOW_SHARED_FALLBACK_PER_LANE else []
    merged = [str(x) for x in lane + shared if str(x).strip()]
    return list(dict.fromkeys(merged))


def selected_features_ready(scope: str = 'SHARED') -> bool:
    scope = _normalize_scope(scope)
    feats = _selected_features_for_scope(scope)
    threshold = MIN_LIVE_FEATURES if scope == 'SHARED' else min(MIN_LIVE_FEATURES, max(MIN_DIRECTIONAL_LIVE_FEATURES, 1))
    return len(feats) >= threshold


def refresh_model_runtime() -> Path:
    _load_artifacts()
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'selected_features_present': bool(SELECTED_FEATURES),
        'selected_feature_count': len(SELECTED_FEATURES),
        'selected_features_ready': selected_features_ready('SHARED'),
        'effective_directional_feature_counts': {k: len(_selected_features_for_scope(k)) for k in DIRECTIONAL_FEATURES},
        'selected_features_min_required': MIN_LIVE_FEATURES,
        'loaded_regimes': sorted(list(AI_MODELS.keys())),
        'strict_parity': bool(STRICT_PARITY),
        'directional_model_counts': {k: len(v) for k, v in DIRECTIONAL_MODELS.items()},
        'directional_bootstrap_enabled': bool(ENABLE_BOOTSTRAP),
        'directional_feature_counts': {k: len(v) for k, v in DIRECTIONAL_FEATURES.items()},
        'status': 'model_layer_ready' if selected_features_ready('SHARED') else 'model_layer_degraded',
        'allow_heuristic_model_fallback': bool(ALLOW_HEURISTIC_FALLBACK),
    }
    RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return RUNTIME_PATH


def _feature_input_from_row(latest_row, regime: str, scope: str = 'SHARED') -> tuple[dict[str, float], str]:
    signal_conf = _safe_float(latest_row.get('訊號信心分數(%)', 50.0), 50.0) / 100.0
    source = 'signal_confidence_fallback'
    scope = _normalize_scope(scope)
    models = AI_MODELS if scope == 'SHARED' else DIRECTIONAL_MODELS.get(scope, {})
    selected = _selected_features_for_scope(scope)
    if regime in models and selected_features_ready(scope):
        try:
            from fts_feature_service import FeatureService
            features_dict = FeatureService().extract_ai_features(latest_row, history_df=None)
            X = {f: _safe_float(features_dict.get(f, latest_row.get(f, 0.0)), 0.0) for f in selected}
            proba = float(models[regime].predict_proba(pd.DataFrame([X]))[0][1])
            return {'proba': max(0.01, min(0.99, proba)), **X}, ('ai_model_directional' if scope != 'SHARED' and scope in DIRECTIONAL_MODELS and DIRECTIONAL_MODELS.get(scope) else ('ai_model_bootstrapped_from_shared' if scope != 'SHARED' else 'ai_model'))
        except Exception:
            pass
    return {'proba': max(0.01, min(0.99, signal_conf))}, source


def evaluate_model_signal(latest_row, regime: str, min_proba: float = 0.5, base_multiplier: float = 1.0, direction_scope: str = 'SHARED') -> ModelDecision:
    if not AI_MODELS and not SELECTED_FEATURES:
        refresh_model_runtime()

    direction_scope = _normalize_scope(direction_scope)
    realized_ev = _safe_float(latest_row.get('Realized_EV', 0.0), 0.0)
    sample_size = int(_safe_float(latest_row.get('歷史訊號樣本數', latest_row.get('Sample_Size', 0)), 0.0))
    signal_conf = _safe_float(latest_row.get('訊號信心分數(%)', latest_row.get('AI_Proba', 0.5) * 100.0), 50.0) / 100.0

    veto_reasons: list[str] = []
    entry_state = str(latest_row.get('Entry_State', 'NO_ENTRY')).upper()
    selected_ready = selected_features_ready(direction_scope) if direction_scope != 'SHARED' else selected_features_ready('SHARED')
    if STRICT_PARITY and not selected_ready:
        model_source = 'parity_locked_signal_confidence'
        proba = max(0.01, min(0.99, signal_conf))
        veto_reasons.append(f'selected_features_not_ready:min_required_{MIN_LIVE_FEATURES}')
    else:
        payload, model_source = _feature_input_from_row(latest_row, regime, direction_scope)
        proba = float(payload.get('proba', signal_conf))

    heuristic_fallback_active = 'signal_confidence' in str(model_source)
    if heuristic_fallback_active and not ALLOW_HEURISTIC_FALLBACK:
        veto_reasons.append('heuristic_model_fallback_blocked')

    if sample_size < 8:
        proba = 0.5 + (proba - 0.5) * 0.4
    elif sample_size < 15:
        proba = 0.5 + (proba - 0.5) * 0.7

    effective_min_proba = float(min_proba)
    if entry_state == 'PILOT_ENTRY':
        effective_min_proba = max(0.45, float(min_proba) - float(PARAMS.get('PILOT_MIN_PROBA_BUFFER', 0.04)))
    if proba < effective_min_proba:
        veto_reasons.append(f'proba_below_threshold:{proba:.3f}<{effective_min_proba:.3f}')
    if realized_ev <= 0:
        veto_reasons.append(f'non_positive_ev:{realized_ev:.4f}')

    entry_readiness = _safe_float(latest_row.get('Entry_Readiness', 0.0), 0.0)
    breakout_risk = _safe_float(latest_row.get('Breakout_Risk_Next3', 0.0), 0.0)
    reversal_risk = _safe_float(latest_row.get('Reversal_Risk_Next3', 0.0), 0.0)
    exit_hazard = _safe_float(latest_row.get('Exit_Hazard_Score', 0.0), 0.0)
    ev_boost = 1.15 if realized_ev > 1.5 else 1.05 if realized_ev > 0.5 else 1.0
    sample_boost = 1.10 if sample_size >= 20 else 1.03 if sample_size >= 10 else 1.0
    readiness_boost = 1.08 if entry_readiness >= 0.60 else 1.02 if entry_readiness >= 0.35 else 1.0
    risk_penalty = 0.82 if max(breakout_risk, reversal_risk, exit_hazard) >= 0.80 else 0.92 if max(breakout_risk, reversal_risk, exit_hazard) >= 0.60 else 1.0
    conviction = max(0.0, min(proba * (float(base_multiplier) * 2.0) * ev_boost * sample_boost * readiness_boost * risk_penalty, 2.5))
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
        model_scope=direction_scope,
        model_bucket=direction_scope,
        feature_scope=direction_scope,
        direction_scope=direction_scope,
        heuristic_fallback_active=heuristic_fallback_active,
        debug={'selected_count': len(_selected_features_for_scope(direction_scope)), 'allow_heuristic_model_fallback': bool(ALLOW_HEURISTIC_FALLBACK), 'Regime_Label': latest_row.get('Regime_Label', regime), 'Transition_Label': latest_row.get('Transition_Label', ''), 'Entry_State': entry_state, 'PreEntry_Score': latest_row.get('PreEntry_Score'), 'Confirm_Entry_Score': latest_row.get('Confirm_Entry_Score'), 'Entry_Readiness': entry_readiness, 'Breakout_Risk_Next3': breakout_risk, 'Reversal_Risk_Next3': reversal_risk, 'Exit_Hazard_Score': exit_hazard, 'Next_Regime_Prob_Bull': latest_row.get('Next_Regime_Prob_Bull'), 'Next_Regime_Prob_Bear': latest_row.get('Next_Regime_Prob_Bear'), 'Next_Regime_Prob_Range': latest_row.get('Next_Regime_Prob_Range')},
    )
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_PATH.write_text(json.dumps(decision.as_dict(), ensure_ascii=False, indent=2), encoding='utf-8')
    return decision


refresh_model_runtime()

# -----------------------------------------------------------------------------
# vNext overlay: independent exit AI model workflow
# -----------------------------------------------------------------------------
EXIT_MODELS: dict[str, Any] = {}
EXIT_SELECTED_FEATURES: list[str] = []
EXIT_MODEL_FILES = {
    'DEFEND': lambda: str(getattr(CONFIG, 'exit_defend_model_filename', 'exit_model_defend.pkl')),
    'REDUCE': lambda: str(getattr(CONFIG, 'exit_reduce_model_filename', 'exit_model_reduce.pkl')),
    'CONFIRM': lambda: str(getattr(CONFIG, 'exit_confirm_model_filename', 'exit_model_confirm.pkl')),
}
ENABLE_EXIT_MODEL_WORKFLOW = bool(getattr(CONFIG, 'enable_exit_model_workflow', True)) and bool(PARAMS.get('ENABLE_EXIT_MODEL_WORKFLOW', True))
EXIT_MODEL_PRIMARY = bool(getattr(CONFIG, 'exit_model_primary', True)) and bool(PARAMS.get('EXIT_MODEL_PRIMARY', True))
EXIT_MODEL_MIN_FEATURES = int(getattr(CONFIG, 'exit_model_min_features', PARAMS.get('EXIT_MODEL_MIN_FEATURES', 6)))
EXIT_FALLBACK_TO_HAZARD = bool(getattr(CONFIG, 'exit_model_fallback_to_hazard', True)) and bool(PARAMS.get('EXIT_MODEL_FALLBACK_TO_HAZARD', True))


@dataclass
class ExitDecision:
    model_source: str
    exit_state: str
    exit_action: str
    defend_proba: float
    reduce_proba: float
    confirm_proba: float
    exit_hazard_score: float
    target_position_multiplier: float
    stop_tighten_multiplier: float
    approved: bool
    selected_features_ready: bool
    veto_reasons: list[str]
    debug: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _load_exit_artifacts() -> None:
    global EXIT_MODELS, EXIT_SELECTED_FEATURES
    EXIT_MODELS = {}
    EXIT_SELECTED_FEATURES = []
    if not ENABLE_EXIT_MODEL_WORKFLOW:
        return
    sf = MODEL_DIR / str(getattr(CONFIG, 'exit_selected_features_filename', 'selected_features_exit.pkl'))
    if sf.exists():
        try:
            EXIT_SELECTED_FEATURES = [str(x) for x in joblib.load(sf) if str(x).strip()]
            EXIT_SELECTED_FEATURES = list(dict.fromkeys(EXIT_SELECTED_FEATURES))
        except Exception:
            EXIT_SELECTED_FEATURES = []
    for key, fn in EXIT_MODEL_FILES.items():
        p = MODEL_DIR / fn()
        if p.exists():
            try:
                EXIT_MODELS[key] = joblib.load(p)
            except Exception:
                pass


def _exit_selected_features_ready() -> bool:
    return len(EXIT_SELECTED_FEATURES) >= max(1, EXIT_MODEL_MIN_FEATURES)


def _exit_features_from_row(row: Any) -> dict[str, float]:
    if not EXIT_SELECTED_FEATURES:
        return {}
    try:
        from fts_feature_service import FeatureService
        features_dict = FeatureService().extract_ai_features(row, history_df=None)
    except Exception:
        features_dict = {}
    return {f: _safe_float(features_dict.get(f, row.get(f, 0.0) if hasattr(row, 'get') else 0.0), 0.0) for f in EXIT_SELECTED_FEATURES}


def _predict_exit_proba(model_key: str, row: Any) -> tuple[float, str]:
    if not EXIT_MODELS and not EXIT_SELECTED_FEATURES:
        _load_exit_artifacts()
    if ENABLE_EXIT_MODEL_WORKFLOW and EXIT_MODEL_PRIMARY and model_key in EXIT_MODELS and _exit_selected_features_ready():
        try:
            X = _exit_features_from_row(row)
            if len(X) >= max(1, EXIT_MODEL_MIN_FEATURES):
                proba = float(EXIT_MODELS[model_key].predict_proba(pd.DataFrame([X]))[0][1])
                return max(0.01, min(0.99, proba)), 'exit_ai_model'
        except Exception:
            pass
    hazard = _safe_float(row.get('Exit_Hazard_Score', 0.0), 0.0) if hasattr(row, 'get') else 0.0
    if EXIT_FALLBACK_TO_HAZARD:
        if model_key == 'DEFEND':
            return max(0.01, min(0.99, hazard * 0.85 + 0.10)), 'exit_hazard_fallback'
        if model_key == 'REDUCE':
            return max(0.01, min(0.99, hazard * 0.95)), 'exit_hazard_fallback'
        return max(0.01, min(0.99, hazard * 1.05 - 0.03)), 'exit_hazard_fallback'
    return 0.01, 'exit_model_unavailable'


def evaluate_exit_signal(row: Any) -> ExitDecision:
    if not EXIT_MODELS and not EXIT_SELECTED_FEATURES:
        _load_exit_artifacts()
    hazard = _safe_float(row.get('Exit_Hazard_Score', 0.0), 0.0) if hasattr(row, 'get') else 0.0
    defend_p, src_def = _predict_exit_proba('DEFEND', row)
    reduce_p, src_red = _predict_exit_proba('REDUCE', row)
    confirm_p, src_con = _predict_exit_proba('CONFIRM', row)
    source = 'exit_ai_model' if {'DEFEND', 'REDUCE', 'CONFIRM'}.issubset(set(EXIT_MODELS.keys())) and _exit_selected_features_ready() else ','.join(sorted({src_def, src_red, src_con}))
    defend_th = float(PARAMS.get('EXIT_DEFEND_THRESHOLD', 0.58))
    reduce_th = float(PARAMS.get('EXIT_REDUCE_THRESHOLD', 0.62))
    confirm_th = float(PARAMS.get('EXIT_CONFIRM_THRESHOLD', 0.66))
    if confirm_p >= confirm_th:
        state, action = 'EXIT', 'FLAT_EXIT'
        target_mult = float(PARAMS.get('EXIT_CONFIRM_POSITION_MULTIPLIER', 0.0))
        stop_mult = float(PARAMS.get('EXIT_CONFIRM_STOP_TIGHTEN', 0.0))
    elif reduce_p >= reduce_th:
        state, action = 'REDUCE', 'TRIM_POSITION'
        target_mult = float(PARAMS.get('EXIT_REDUCE_POSITION_MULTIPLIER', 0.35))
        stop_mult = float(PARAMS.get('EXIT_REDUCE_STOP_TIGHTEN', 0.60))
    elif defend_p >= defend_th:
        state, action = 'DEFEND', 'TIGHTEN_AND_DEFEND'
        target_mult = float(PARAMS.get('EXIT_DEFEND_POSITION_MULTIPLIER', 0.60))
        stop_mult = float(PARAMS.get('EXIT_DEFEND_STOP_TIGHTEN', 0.80))
    else:
        state, action = 'HOLD', 'HOLD'
        target_mult = 1.0
        stop_mult = 1.0
    veto: list[str] = []
    ready = _exit_selected_features_ready()
    if EXIT_MODEL_PRIMARY and not ready and not EXIT_FALLBACK_TO_HAZARD:
        veto.append('exit_selected_features_not_ready')
    return ExitDecision(
        model_source=source,
        exit_state=state,
        exit_action=action,
        defend_proba=float(defend_p),
        reduce_proba=float(reduce_p),
        confirm_proba=float(confirm_p),
        exit_hazard_score=float(hazard),
        target_position_multiplier=float(target_mult),
        stop_tighten_multiplier=float(stop_mult),
        approved=not veto,
        selected_features_ready=ready,
        veto_reasons=veto,
        debug={
            'exit_models_loaded': sorted(list(EXIT_MODELS.keys())),
            'exit_selected_feature_count': len(EXIT_SELECTED_FEATURES),
            'configured_thresholds': {'defend': defend_th, 'reduce': reduce_th, 'confirm': confirm_th},
        },
    )


_base_refresh_model_runtime = refresh_model_runtime

def refresh_model_runtime() -> Path:  # type: ignore[override]
    path = _base_refresh_model_runtime()
    _load_exit_artifacts()
    try:
        payload = json.loads(RUNTIME_PATH.read_text(encoding='utf-8')) if RUNTIME_PATH.exists() else {}
        payload.update({
            'exit_model_workflow_enabled': bool(ENABLE_EXIT_MODEL_WORKFLOW),
            'exit_model_primary': bool(EXIT_MODEL_PRIMARY),
            'exit_selected_feature_count': len(EXIT_SELECTED_FEATURES),
            'exit_selected_features_ready': _exit_selected_features_ready(),
            'exit_models_loaded': sorted(list(EXIT_MODELS.keys())),
        })
        RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        pass
    return path


refresh_model_runtime()
