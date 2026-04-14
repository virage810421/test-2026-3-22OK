# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

try:
    from fts_runtime_diagnostics import record_issue, write_summary as write_runtime_diagnostics_summary
except Exception:  # runtime diagnostics  # pragma: no cover
    def record_issue(*args, **kwargs):
        return {}
    def write_runtime_diagnostics_summary(*args, **kwargs):
        return None

from config import PARAMS
try:
    from fts_config import PATHS, CONFIG  # type: ignore
except Exception:  # runtime diagnostics  # pragma: no cover
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
ENABLE_BOOTSTRAP = bool(PARAMS.get('ENABLE_DIRECTIONAL_ARTIFACT_BOOTSTRAP', False)) and bool(PARAMS.get('DIRECTIONAL_BOOTSTRAP_FORCE_SHARED', False))
ALLOW_HEURISTIC_FALLBACK = bool(getattr(CONFIG, 'allow_heuristic_model_fallback', False))
DIRECTIONAL_LIVE_ENABLED = bool(getattr(CONFIG, 'enable_directional_features_in_live', True)) and (not bool(getattr(CONFIG, 'force_shared_feature_universe', False)))
ALLOW_SHARED_FALLBACK_PER_LANE = bool(getattr(CONFIG, 'live_allow_directional_shared_fallback', False)) and not bool(PARAMS.get('DIRECTIONAL_REQUIRE_INDEPENDENT_LANE_MODELS', True))


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
    except Exception as exc:  # runtime diagnostics
        record_issue('model_layer', 'safe_float_cast', exc, severity='WARNING', fail_mode='fail_closed')
        return default


def _normalize_scope(value: Any) -> str:
    s = str(value or '').strip().upper()
    return s if s in {'LONG', 'SHORT', 'RANGE'} else 'SHARED'



def _lane_seed_from_shared(scope: str) -> None:
    scope = _normalize_scope(scope)
    if scope == 'SHARED':
        return
    # v89: lane models must be independently trained.  Shared seeding is allowed
    # only when explicitly re-enabled for research diagnostics.
    if bool(PARAMS.get('DIRECTIONAL_REQUIRE_INDEPENDENT_LANE_MODELS', True)):
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
        except Exception as exc:  # runtime diagnostics
            record_issue('model_layer', 'load_selected_features', exc, severity='ERROR', fail_mode='fail_closed')
            SELECTED_FEATURES = []
    for regime in ['趨勢多頭', '區間盤整', '趨勢空頭']:
        p = MODEL_DIR / f'model_{regime}.pkl'
        if p.exists():
            try:
                AI_MODELS[regime] = joblib.load(p)
            except Exception as exc:  # runtime diagnostics
                record_issue('model_layer', 'exit_artifact_candidate_scan_failed', exc, severity='ERROR', fail_mode='fail_closed')
    if ENABLE_DIRECTIONAL:
        for scope in ['LONG', 'SHORT', 'RANGE']:
            sf = MODEL_DIR / f'selected_features_{scope.lower()}.pkl'
            if sf.exists():
                try:
                    DIRECTIONAL_FEATURES[scope] = [str(x) for x in joblib.load(sf) if str(x).strip()]
                except Exception as exc:  # runtime diagnostics
                    record_issue('model_layer', 'load_directional_selected_features', exc, severity='ERROR', fail_mode='fail_closed')
                    DIRECTIONAL_FEATURES[scope] = []
            for regime in ['趨勢多頭', '區間盤整', '趨勢空頭']:
                p = MODEL_DIR / f'model_{scope.lower()}_{regime}.pkl'
                if p.exists():
                    try:
                        DIRECTIONAL_MODELS[scope][regime] = joblib.load(p)
                    except Exception as exc:  # runtime diagnostics
                        record_issue('model_layer', 'artifact_probe_failed', exc, severity='ERROR', fail_mode='fail_closed')
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


def _refresh_model_runtime_base() -> Path:
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
        'directional_require_independent_lane_models': bool(PARAMS.get('DIRECTIONAL_REQUIRE_INDEPENDENT_LANE_MODELS', True)),
        'allow_shared_fallback_per_lane': bool(ALLOW_SHARED_FALLBACK_PER_LANE),
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
    if scope != 'SHARED' and (regime not in models or not selected_features_ready(scope)):
        return {'proba': 0.01}, 'directional_lane_model_unavailable_fail_closed'
    if regime in models and selected_features_ready(scope):
        try:
            from fts_feature_service import FeatureService
            features_dict = FeatureService().extract_ai_features(latest_row, history_df=None)
            X = {f: _safe_float(features_dict.get(f, latest_row.get(f, 0.0)), 0.0) for f in selected}
            proba = float(models[regime].predict_proba(pd.DataFrame([X]))[0][1])
            return {'proba': max(0.01, min(0.99, proba)), **X}, ('ai_model_directional' if scope != 'SHARED' and scope in DIRECTIONAL_MODELS and DIRECTIONAL_MODELS.get(scope) else ('ai_model_bootstrapped_from_shared' if scope != 'SHARED' else 'ai_model'))
        except Exception as exc:  # runtime diagnostics
            record_issue('model_layer', 'runtime_model_diagnostic_failed', exc, severity='ERROR', fail_mode='fail_closed')
    return {'proba': 0.01 if bool(PARAMS.get('SIGNAL_PATH_FAIL_CLOSED', True)) else max(0.01, min(0.99, signal_conf))}, ('model_unavailable_fail_closed' if bool(PARAMS.get('SIGNAL_PATH_FAIL_CLOSED', True)) else source)


def evaluate_model_signal(latest_row, regime: str, min_proba: float = 0.5, base_multiplier: float = 1.0, direction_scope: str = 'SHARED') -> ModelDecision:
    if not AI_MODELS and not SELECTED_FEATURES:
        refresh_model_runtime()

    direction_scope = _normalize_scope(direction_scope)
    expected_return = _safe_float(latest_row.get('Expected_Return', latest_row.get('Heuristic_EV', latest_row.get('Live_EV', latest_row.get('Realized_EV', 0.0)))), 0.0)
    ev_source = str(latest_row.get('EV_Source', 'unknown'))
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
    if 'fail_closed' in str(model_source) or 'unavailable' in str(model_source):
        veto_reasons.append(str(model_source))
    if direction_scope != 'SHARED' and bool(PARAMS.get('DIRECTIONAL_REQUIRE_INDEPENDENT_LANE_MODELS', True)):
        if direction_scope not in DIRECTIONAL_MODELS or regime not in DIRECTIONAL_MODELS.get(direction_scope, {}):
            veto_reasons.append(f'independent_directional_model_missing:{direction_scope}:{regime}')
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
    live_min_ev = float(PARAMS.get('MODEL_LAYER_MIN_EXPECTED_RETURN', PARAMS.get('LIVE_MIN_EXPECTED_RETURN', -0.0015)))
    ev_min_sample = int(PARAMS.get('LIVE_EV_MIN_SAMPLE_FOR_HARD_BLOCK', PARAMS.get('MIN_SIGNAL_SAMPLE_SIZE', 8)))
    if expected_return < live_min_ev and sample_size >= ev_min_sample:
        veto_reasons.append(f'expected_return_below_threshold:{expected_return:.4f}<{live_min_ev:.4f}')

    entry_readiness = _safe_float(latest_row.get('Entry_Readiness', 0.0), 0.0)
    breakout_risk = _safe_float(latest_row.get('Breakout_Risk_Next3', 0.0), 0.0)
    reversal_risk = _safe_float(latest_row.get('Reversal_Risk_Next3', 0.0), 0.0)
    exit_hazard = _safe_float(latest_row.get('Exit_Hazard_Score', 0.0), 0.0)
    ev_boost = 1.15 if expected_return > 0.015 else 1.05 if expected_return > 0.005 else 1.0
    sample_boost = 1.10 if sample_size >= 20 else 1.03 if sample_size >= 10 else 1.0
    readiness_boost = 1.08 if entry_readiness >= 0.60 else 1.02 if entry_readiness >= 0.35 else 1.0
    risk_penalty = 0.82 if max(breakout_risk, reversal_risk, exit_hazard) >= 0.80 else 0.92 if max(breakout_risk, reversal_risk, exit_hazard) >= 0.60 else 1.0
    conviction = max(0.0, min(proba * (float(base_multiplier) * 2.0) * ev_boost * sample_boost * readiness_boost * risk_penalty, 2.5))
    approved = len(veto_reasons) == 0

    decision = ModelDecision(
        regime=str(regime),
        model_source=model_source,
        proba=proba,
        realized_ev=expected_return,
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
        debug={'selected_count': len(_selected_features_for_scope(direction_scope)), 'allow_heuristic_model_fallback': bool(ALLOW_HEURISTIC_FALLBACK), 'Regime_Label': latest_row.get('Regime_Label', regime), 'Transition_Label': latest_row.get('Transition_Label', ''), 'Entry_State': entry_state, 'PreEntry_Score': latest_row.get('PreEntry_Score'), 'Confirm_Entry_Score': latest_row.get('Confirm_Entry_Score'), 'Entry_Readiness': entry_readiness, 'Breakout_Risk_Next3': breakout_risk, 'Reversal_Risk_Next3': reversal_risk, 'Exit_Hazard_Score': exit_hazard, 'Expected_Return': expected_return, 'EV_Source': ev_source, 'Next_Regime_Prob_Bull': latest_row.get('Next_Regime_Prob_Bull'), 'Next_Regime_Prob_Bear': latest_row.get('Next_Regime_Prob_Bear'), 'Next_Regime_Prob_Range': latest_row.get('Next_Regime_Prob_Range')},
    )
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_PATH.write_text(json.dumps(decision.as_dict(), ensure_ascii=False, indent=2), encoding='utf-8')
    return decision


# -----------------------------------------------------------------------------
# Exit AI model workflow (single official implementation)
# -----------------------------------------------------------------------------
EXIT_MODELS: dict[str, Any] = {}
EXIT_SELECTED_FEATURES: list[str] = []
EXIT_LOAD_ERRORS: dict[str, str] = {}
EXIT_MODEL_FILES = {
    'DEFEND': lambda: str(getattr(CONFIG, 'exit_defend_model_filename', 'exit_model_defend.pkl')),
    'REDUCE': lambda: str(getattr(CONFIG, 'exit_reduce_model_filename', 'exit_model_reduce.pkl')),
    'CONFIRM': lambda: str(getattr(CONFIG, 'exit_confirm_model_filename', 'exit_model_confirm.pkl')),
}
EXIT_REQUIRED_KEYS = ('DEFEND', 'REDUCE', 'CONFIRM')
ENABLE_EXIT_MODEL_WORKFLOW = bool(getattr(CONFIG, 'enable_exit_model_workflow', True)) and bool(PARAMS.get('ENABLE_EXIT_MODEL_WORKFLOW', True))
EXIT_MODEL_PRIMARY = bool(getattr(CONFIG, 'exit_model_primary', True)) and bool(PARAMS.get('EXIT_MODEL_PRIMARY', True))
EXIT_MODEL_MIN_FEATURES = int(getattr(CONFIG, 'exit_model_min_features', PARAMS.get('EXIT_MODEL_MIN_FEATURES', 6)))
# Hard-block default: hazard fallback is opt-in only.
EXIT_FALLBACK_TO_HAZARD = bool(getattr(CONFIG, 'exit_model_fallback_to_hazard', False)) and bool(PARAMS.get('EXIT_MODEL_FALLBACK_TO_HAZARD', False))
EXIT_REQUIRE_ALL_ARTIFACTS = bool(getattr(CONFIG, 'exit_model_require_all_artifacts', True)) and bool(PARAMS.get('EXIT_MODEL_REQUIRE_ALL_ARTIFACTS', True))
EXIT_HARD_BLOCK_IF_MISSING = bool(getattr(CONFIG, 'exit_model_hard_block_if_missing', True)) and bool(PARAMS.get('EXIT_MODEL_HARD_BLOCK_IF_MISSING', True))
EXIT_STATUS_PATH = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / str(getattr(CONFIG, 'exit_model_runtime_status_filename', PARAMS.get('EXIT_MODEL_RUNTIME_STATUS_PATH', 'exit_model_status.json')).split('/')[-1])


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


def _exit_required_model_files() -> dict[str, str]:
    return {key: str(MODEL_DIR / EXIT_MODEL_FILES[key]()) for key in EXIT_REQUIRED_KEYS}


def _load_exit_artifacts() -> None:
    """Load exit AI artifacts once, with explicit missing/load-error status.

    This is the only official exit artifact loader.  It does not silently use the
    hazard fallback; fallback requires EXIT_MODEL_FALLBACK_TO_HAZARD=True.
    """
    global EXIT_MODELS, EXIT_SELECTED_FEATURES, EXIT_LOAD_ERRORS
    EXIT_MODELS = {}
    EXIT_SELECTED_FEATURES = []
    EXIT_LOAD_ERRORS = {}
    if not ENABLE_EXIT_MODEL_WORKFLOW:
        EXIT_LOAD_ERRORS['workflow'] = 'disabled'
        return

    sf = MODEL_DIR / str(getattr(CONFIG, 'exit_selected_features_filename', 'selected_features_exit.pkl'))
    if sf.exists():
        try:
            EXIT_SELECTED_FEATURES = [str(x) for x in joblib.load(sf) if str(x).strip()]
            EXIT_SELECTED_FEATURES = list(dict.fromkeys(EXIT_SELECTED_FEATURES))
        except Exception as exc:  # runtime diagnostics
            EXIT_SELECTED_FEATURES = []
            EXIT_LOAD_ERRORS['selected_features_exit'] = f'load_error:{type(exc).__name__}:{exc}'
            record_issue('model_layer', 'exit_selected_features_load_failed', exc, severity='ERROR', fail_mode='fail_closed')
    else:
        EXIT_LOAD_ERRORS['selected_features_exit'] = 'missing'

    for key, fn in EXIT_MODEL_FILES.items():
        p = MODEL_DIR / fn()
        if p.exists():
            try:
                EXIT_MODELS[key] = joblib.load(p)
            except Exception as exc:  # runtime diagnostics
                EXIT_LOAD_ERRORS[key] = f'load_error:{type(exc).__name__}:{exc}'
                record_issue('model_layer', f'exit_model_{key.lower()}_load_failed', exc, severity='ERROR', fail_mode='fail_closed')
        else:
            EXIT_LOAD_ERRORS[key] = 'missing'


def _exit_selected_features_ready() -> bool:
    return len(EXIT_SELECTED_FEATURES) >= max(1, EXIT_MODEL_MIN_FEATURES)


def _exit_features_from_row(row: Any) -> dict[str, float]:
    if not EXIT_SELECTED_FEATURES:
        return {}
    try:
        from fts_feature_service import FeatureService
        features_dict = FeatureService().extract_ai_features(row, history_df=None)
    except Exception as exc:  # runtime diagnostics
        record_issue('model_layer', 'exit_feature_extract_failed', exc, severity='ERROR', fail_mode='fail_closed')
        features_dict = {}
    return {
        f: _safe_float(features_dict.get(f, row.get(f, 0.0) if hasattr(row, 'get') else 0.0), 0.0)
        for f in EXIT_SELECTED_FEATURES
    }


def _exit_artifact_status() -> dict[str, Any]:
    if not EXIT_MODELS and not EXIT_SELECTED_FEATURES and not EXIT_LOAD_ERRORS:
        _load_exit_artifacts()
    loaded = sorted(list(EXIT_MODELS.keys()))
    missing_models = [key for key in EXIT_REQUIRED_KEYS if key not in EXIT_MODELS]
    selected_ready = _exit_selected_features_ready()
    complete = bool(ENABLE_EXIT_MODEL_WORKFLOW and selected_ready and not missing_models)
    if EXIT_REQUIRE_ALL_ARTIFACTS:
        complete = bool(complete and set(EXIT_REQUIRED_KEYS).issubset(set(EXIT_MODELS.keys())))
    return {
        'exit_model_workflow_enabled': bool(ENABLE_EXIT_MODEL_WORKFLOW),
        'exit_model_primary': bool(EXIT_MODEL_PRIMARY),
        'exit_model_fallback_to_hazard': bool(EXIT_FALLBACK_TO_HAZARD),
        'exit_fallback_disabled_by_default': not bool(EXIT_FALLBACK_TO_HAZARD),
        'exit_require_all_artifacts': bool(EXIT_REQUIRE_ALL_ARTIFACTS),
        'exit_hard_block_if_missing': bool(EXIT_HARD_BLOCK_IF_MISSING),
        'exit_models_required': list(EXIT_REQUIRED_KEYS),
        'exit_models_loaded': loaded,
        'exit_models_missing': missing_models,
        'exit_selected_feature_count': len(EXIT_SELECTED_FEATURES),
        'exit_selected_features_ready': bool(selected_ready),
        'exit_artifacts_complete': bool(complete),
        'exit_model_source': 'exit_ai_model' if complete else ('exit_hazard_fallback' if EXIT_FALLBACK_TO_HAZARD else 'exit_ai_model_unavailable'),
        'exit_load_errors': dict(EXIT_LOAD_ERRORS),
        'exit_required_model_files': _exit_required_model_files(),
        'exit_selected_features_file': str(MODEL_DIR / str(getattr(CONFIG, 'exit_selected_features_filename', 'selected_features_exit.pkl'))),
    }


def get_exit_model_runtime_status(refresh: bool = False) -> dict[str, Any]:
    if refresh or (not EXIT_MODELS and not EXIT_SELECTED_FEATURES and not EXIT_LOAD_ERRORS):
        _load_exit_artifacts()
    return _exit_artifact_status()


def _write_exit_runtime_status() -> dict[str, Any]:
    status = get_exit_model_runtime_status(refresh=False)
    try:
        EXIT_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        EXIT_STATUS_PATH.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as exc:  # runtime diagnostics
        record_issue('model_layer', 'exit_runtime_status_write_failed', exc, severity='WARNING', fail_mode='fail_closed')
    return status


def _predict_exit_proba(model_key: str, row: Any) -> tuple[float, str]:
    status = _exit_artifact_status()
    if ENABLE_EXIT_MODEL_WORKFLOW and EXIT_MODEL_PRIMARY and status.get('exit_artifacts_complete') and model_key in EXIT_MODELS:
        try:
            X = _exit_features_from_row(row)
            if len(X) >= max(1, EXIT_MODEL_MIN_FEATURES):
                proba = float(EXIT_MODELS[model_key].predict_proba(pd.DataFrame([X]))[0][1])
                return max(0.01, min(0.99, proba)), 'exit_ai_model'
            return 0.01, 'exit_selected_features_not_ready'
        except Exception as exc:  # runtime diagnostics
            EXIT_LOAD_ERRORS[f'{model_key}_predict'] = f'predict_error:{type(exc).__name__}:{exc}'
            record_issue('model_layer', f'exit_model_{model_key.lower()}_predict_failed', exc, severity='ERROR', fail_mode='fail_closed')
            if not EXIT_FALLBACK_TO_HAZARD:
                return 0.01, 'exit_ai_model_predict_error'

    hazard = _safe_float(row.get('Exit_Hazard_Score', 0.0), 0.0) if hasattr(row, 'get') else 0.0
    if EXIT_FALLBACK_TO_HAZARD:
        if model_key == 'DEFEND':
            return max(0.01, min(0.99, hazard * 0.85 + 0.10)), 'exit_hazard_fallback'
        if model_key == 'REDUCE':
            return max(0.01, min(0.99, hazard * 0.95)), 'exit_hazard_fallback'
        return max(0.01, min(0.99, hazard * 1.05 - 0.03)), 'exit_hazard_fallback'
    return 0.01, 'exit_ai_model_unavailable'


def evaluate_exit_signal(row: Any) -> ExitDecision:
    status = _exit_artifact_status()
    hazard = _safe_float(row.get('Exit_Hazard_Score', 0.0), 0.0) if hasattr(row, 'get') else 0.0
    defend_p, src_def = _predict_exit_proba('DEFEND', row)
    reduce_p, src_red = _predict_exit_proba('REDUCE', row)
    confirm_p, src_con = _predict_exit_proba('CONFIRM', row)

    sources = sorted({src_def, src_red, src_con})
    source = 'exit_ai_model' if all(src == 'exit_ai_model' for src in sources) and status.get('exit_artifacts_complete') else ','.join(sources)
    defend_th = float(PARAMS.get('EXIT_DEFEND_THRESHOLD', 0.58))
    reduce_th = float(PARAMS.get('EXIT_REDUCE_THRESHOLD', 0.62))
    confirm_th = float(PARAMS.get('EXIT_CONFIRM_THRESHOLD', 0.66))

    veto: list[str] = []
    if EXIT_MODEL_PRIMARY and EXIT_HARD_BLOCK_IF_MISSING and not status.get('exit_artifacts_complete') and not EXIT_FALLBACK_TO_HAZARD:
        veto.append('exit_ai_model_artifacts_incomplete')
        if not status.get('exit_selected_features_ready'):
            veto.append('exit_selected_features_not_ready')
        for m in status.get('exit_models_missing', []):
            veto.append(f'exit_model_missing_{m.lower()}')

    if veto:
        state, action = 'HOLD', 'EXIT_AI_MODEL_NOT_READY_REVIEW'
        target_mult, stop_mult = 1.0, 1.0
    elif confirm_p >= confirm_th:
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
        target_mult, stop_mult = 1.0, 1.0

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
        selected_features_ready=bool(status.get('exit_selected_features_ready')),
        veto_reasons=veto,
        debug={**status, 'configured_thresholds': {'defend': defend_th, 'reduce': reduce_th, 'confirm': confirm_th}},
    )


_base_refresh_model_runtime = _refresh_model_runtime_base


def refresh_model_runtime() -> Path:  # type: ignore[override]
    path = _base_refresh_model_runtime()
    _load_exit_artifacts()
    status = _write_exit_runtime_status()
    try:
        payload = json.loads(RUNTIME_PATH.read_text(encoding='utf-8')) if RUNTIME_PATH.exists() else {}
        payload.update(status)
        RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as exc:  # runtime diagnostics
        record_issue('model_layer', 'model_layer_runtime_status_update_failed', exc, severity='WARNING', fail_mode='fail_closed')
    return path


refresh_model_runtime()
