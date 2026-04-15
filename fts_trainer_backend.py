# -*- coding: utf-8 -*-
"""Safer trainer backend with diversified feature selection, promotion floors and out-of-time checks."""
from __future__ import annotations

import itertools
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import TimeSeriesSplit

from config import PARAMS
from model_governance import create_version_tag, get_best_version_entry, promote_best_version, restore_version, snapshot_current_models
from fts_data_quality_guard import validate_training_frame
from fts_feature_catalog import FEATURE_SPECS, PRIORITY_NEW_FEATURES_20

RUNTIME_PATH = Path('runtime') / 'trainer_backend_report.json'


META_DROP_COLS = [
    'Ticker', 'Ticker SYMBOL', 'Date', 'Setup', 'Setup_Tag', 'Regime', 'Regime_Source',
    'Regime_Direction_Score', 'Regime_Strength_Score', 'Regime_Environment_Score',
    'Regime_Composite_Score', 'Regime_Intensity',
    'Label', 'Label_Y', 'Target_Return', 'Future_Return_Pct', 'Entry_Price', 'Entry_Price_Basis',
    'Exit_Price', 'Entry_Date', 'Exit_Date', 'Direction', 'Stop_Hit', 'Hold_Days',
    'Touched_TP', 'Touched_SL', 'Label_Reason', 'Label_Exit_Type',
    'Favorable_Move_Pct', 'Adverse_Move_Pct', 'Max_Favorable_Excursion',
    'Max_Adverse_Excursion', 'Realized_Return_After_Cost', 'Mounted_Feature_Count',
    'Setup_Ready_Label', 'Trigger_Confirm_Label', 'Entry_State_At_Label', 'Early_Path_State_At_Label', 'Confirm_Path_State_At_Label',
    'Exit_Defend_Label', 'Exit_Reduce_Label', 'Exit_Confirm_Label', 'Exit_State_At_Label', 'Target_Position_At_Label',
    'Sample_Type', 'Target_Return_Unit', 'Is_Position_Day', 'Position_Day', 'Position_Age_Days',
    'Feature_Row_Index', 'Position_Date', 'Current_Close', 'Unrealized_Return', 'Unrealized_Return_Pct',
    'Remaining_Return_To_Planned_Exit', 'Remaining_Return_To_Planned_Exit_Pct',
    'Adverse_To_Date_Pct', 'Favorable_To_Date_Pct', 'Realized_Return_After_Cost_Pct'
]


LANES = ['LONG', 'SHORT', 'RANGE']
REGIMES = ['趨勢多頭', '區間盤整', '趨勢空頭']


def _infer_lane_from_row(row: pd.Series) -> str:
    text = ' '.join(str(row.get(k, '')) for k in ['Direction', 'Action', 'Golden_Type', 'Structure', 'Setup', 'Setup_Tag', 'Regime']).upper()
    if 'RANGE' in text or '區間' in text:
        return 'RANGE'
    if 'SHORT' in text or 'SELL' in text or '空' in text:
        return 'SHORT'
    return 'LONG'


def _build_lane_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=str)
    return df.apply(_infer_lane_from_row, axis=1)





def _validate_target_return_frame(df: pd.DataFrame) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    """Fail closed and normalize Target_Return to decimal return.

    Canonical unit: 0.032 means +3.2%.  Older generated files used 3.2 for
    +3.2%; those are detected and converted once, with an explicit report flag.
    """
    required = bool(PARAMS.get('MODEL_REQUIRE_TARGET_RETURN', True))
    min_valid_ratio = float(PARAMS.get('MODEL_TARGET_RETURN_MIN_VALID_RATIO', 0.80))
    legacy_autofix = bool(PARAMS.get('TARGET_RETURN_LEGACY_PERCENT_AUTOFIX', True))
    abs_max_decimal = float(PARAMS.get('TARGET_RETURN_ABS_MAX_DECIMAL', 0.80))
    if 'Target_Return' not in df.columns:
        report = {
            'status': 'blocked' if required else 'warning',
            'reason': 'target_return_missing',
            'message': '正式訓練禁止用 Label_Y 產生 ±0.05 假 Target_Return；請由未來報酬/成本/滑價標籤器產生 Target_Return。',
            'required': required,
            'target_return_unit': 'decimal_return',
        }
        return (None if required else df), report
    s = pd.to_numeric(df['Target_Return'], errors='coerce')
    finite_mask = np.isfinite(s.astype(float))
    valid_ratio = float(finite_mask.mean()) if len(s) else 0.0
    nonzero_count = int((s[finite_mask].abs() > 1e-12).sum()) if finite_mask.any() else 0
    converted_from_legacy_percent = False
    raw_abs_p95 = float(s[finite_mask].abs().quantile(0.95)) if finite_mask.any() else 0.0
    raw_abs_max = float(s[finite_mask].abs().max()) if finite_mask.any() else 0.0

    # Legacy detector: normal daily/holding-period decimal returns rarely have
    # p95 > 0.80.  Old builder wrote percent points, so 3.2 meant +3.2%.
    if legacy_autofix and raw_abs_p95 > abs_max_decimal and raw_abs_max <= 100.0:
        s = s / 100.0
        converted_from_legacy_percent = True

    norm_abs_p95 = float(s[finite_mask].abs().quantile(0.95)) if finite_mask.any() else 0.0
    norm_abs_max = float(s[finite_mask].abs().max()) if finite_mask.any() else 0.0
    report = {
        'status': 'ok',
        'required': required,
        'rows_before': int(len(df)),
        'valid_ratio': valid_ratio,
        'nonzero_count': nonzero_count,
        'min_valid_ratio': min_valid_ratio,
        'target_return_unit': 'decimal_return',
        'legacy_percent_autofix_enabled': legacy_autofix,
        'converted_from_legacy_percent': converted_from_legacy_percent,
        'raw_abs_p95': raw_abs_p95,
        'raw_abs_max': raw_abs_max,
        'normalized_abs_p95': norm_abs_p95,
        'normalized_abs_max': norm_abs_max,
    }
    if valid_ratio < min_valid_ratio:
        report.update({'status': 'blocked', 'reason': 'target_return_valid_ratio_below_floor'})
        return None, report
    if nonzero_count <= 0:
        report.update({'status': 'blocked', 'reason': 'target_return_all_zero_or_missing'})
        return None, report
    if bool(PARAMS.get('TARGET_RETURN_BLOCK_IF_IMPLAUSIBLE', True)) and norm_abs_p95 > abs_max_decimal:
        report.update({'status': 'blocked', 'reason': 'target_return_implausible_after_unit_normalization'})
        return None, report
    cleaned = df.loc[finite_mask].copy()
    cleaned['Target_Return'] = s.loc[finite_mask].astype(float)
    cleaned['Target_Return_Unit'] = 'decimal_return'
    if 'Future_Return_Pct' not in cleaned.columns:
        cleaned['Future_Return_Pct'] = (cleaned['Target_Return'] * 100.0).round(4)
    report['rows_after'] = int(len(cleaned))
    return cleaned, report



def _emit_blocked_training_report(payload: dict[str, Any], *, reason: str = '', category: str = '') -> tuple[Path, dict[str, Any]]:
    payload = dict(payload)
    if reason and not payload.get('reason'):
        payload['reason'] = reason
    if category and not payload.get('blocked_reason_category'):
        payload['blocked_reason_category'] = category
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    _write_model_live_signal_gate(payload, promotion_ready=False, promoted=False, reason=payload.get('reason') or payload.get('blocked_reason_category') or 'training_blocked')
    return RUNTIME_PATH, payload


def _write_model_live_signal_gate(report: dict[str, Any], promotion_ready: bool, promoted: bool, reason: str = '') -> dict[str, Any]:
    gate = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'allow_live_signal': bool(promotion_ready and promoted),
        'promotion_ready': bool(promotion_ready),
        'promoted_current_candidate': bool(promoted),
        'block_live_on_unpromoted': bool(PARAMS.get('MODEL_BLOCK_LIVE_ON_UNPROMOTED', True)),
        'reason': reason or ('promoted' if promotion_ready and promoted else 'promotion_not_cleared'),
        'promotion': report.get('promotion', {}),
        'out_of_time': report.get('out_of_time', {}),
        'walk_forward_summary': report.get('walk_forward_summary', {}),
        'overall_score': report.get('overall_score', 0.0),
        'status': 'live_signal_allowed' if bool(promotion_ready and promoted) else 'live_signal_blocked',
    }
    Path('runtime').mkdir(parents=True, exist_ok=True)
    (Path('runtime') / 'model_live_signal_gate.json').write_text(json.dumps(gate, ensure_ascii=False, indent=2), encoding='utf-8')
    return gate

def _exit_selected_feature_pkl() -> Path:
    return Path('models') / 'selected_features_exit.pkl'


def _exit_model_pkl(label_key: str) -> Path:
    mapping = {
        'Exit_Defend_Label': 'exit_model_defend.pkl',
        'Exit_Reduce_Label': 'exit_model_reduce.pkl',
        'Exit_Confirm_Label': 'exit_model_confirm.pkl',
    }
    return Path('models') / mapping[label_key]


def _train_exit_models(train_df: pd.DataFrame, all_features: list[str], selected_features: list[str]) -> dict[str, Any]:
    report: dict[str, Any] = {'enabled': bool(PARAMS.get('ENABLE_EXIT_MODEL_WORKFLOW', True)), 'models': {}, 'selected_features_path': str(_exit_selected_feature_pkl())}
    if not report['enabled']:
        report['status'] = 'disabled'
        return report
    if train_df is None or train_df.empty:
        report.update({
            'status': 'blocked_missing_position_day_samples' if bool(PARAMS.get('EXIT_MODEL_REQUIRE_POSITION_DAY_SAMPLES', True)) else 'missing_exit_training_rows',
            'rows': 0,
            'requires_position_day_samples': bool(PARAMS.get('EXIT_MODEL_REQUIRE_POSITION_DAY_SAMPLES', True)),
        })
        return report
    if bool(PARAMS.get('EXIT_MODEL_REQUIRE_POSITION_DAY_SAMPLES', True)):
        sample_type = train_df.get('Sample_Type', pd.Series([''] * len(train_df), index=train_df.index)).astype(str).str.upper()
        is_position_day = sample_type.eq('POSITION_DAY') | pd.to_numeric(train_df.get('Is_Position_Day', 0), errors='coerce').fillna(0).astype(int).eq(1)
        min_rows = int(PARAMS.get('EXIT_MODEL_MIN_POSITION_DAY_ROWS', 80))
        if int(is_position_day.sum()) < min_rows:
            report.update({
                'status': 'blocked_position_day_rows_below_floor',
                'position_day_rows': int(is_position_day.sum()),
                'min_position_day_rows': min_rows,
                'requires_position_day_samples': True,
            })
            return report
        train_df = train_df.loc[is_position_day].copy()
    label_cols = ['Exit_Defend_Label', 'Exit_Reduce_Label', 'Exit_Confirm_Label']
    present = [c for c in label_cols if c in train_df.columns]
    if not present:
        report['status'] = 'missing_exit_labels'
        return report
    priority = [
        'Exit_Hazard_Score','Breakout_Risk_Next3','Reversal_Risk_Next3','Trend_Exhaustion_Score',
        'Trend_Confidence_Delta','Range_Confidence_Delta','Proba_Delta_3d','Entry_Readiness',
        'Next_Regime_Prob_Bull','Next_Regime_Prob_Bear','Next_Regime_Prob_Range',
        'Hysteresis_Switch_Armed','Hysteresis_Locked','ATR_Pct','ATR_Pct_Pctl','RealizedVol_20',
        'Foreign_Ratio_Delta_3d','Total_Ratio_Delta_3d','Score_Gap_Slope_3d'
    ]
    candidates = list(dict.fromkeys([f for f in priority + selected_features + all_features if f in train_df.columns and f not in META_DROP_COLS]))
    numeric_candidates: list[str] = []
    for f in candidates:
        vals = pd.to_numeric(train_df[f], errors='coerce')
        if vals.notna().sum() >= max(30, int(len(train_df) * 0.05)) and vals.fillna(0).nunique() > 1:
            numeric_candidates.append(f)
    min_features = int(PARAMS.get('EXIT_MODEL_MIN_FEATURES', 6))
    exit_selected = numeric_candidates[:max(min_features, min(24, len(numeric_candidates)))]
    if len(exit_selected) < min_features:
        report['status'] = 'insufficient_exit_features'
        report['selected_feature_count'] = len(exit_selected)
        return report
    joblib.dump(exit_selected, _exit_selected_feature_pkl())
    report['selected_feature_count'] = len(exit_selected)
    report['selected_features_preview'] = exit_selected[:20]
    X = train_df[exit_selected].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    saved = 0
    for label in label_cols:
        y = pd.to_numeric(train_df[label], errors='coerce').fillna(0).astype(int) if label in train_df.columns else pd.Series([0] * len(train_df), index=train_df.index, dtype=int)
        ratio = float(y.mean()) if len(y) else 0.0
        info = {'positive_ratio': ratio, 'rows': int(len(y)), 'path': str(_exit_model_pkl(label))}
        if label not in present:
            info.update({'status': 'SKIP', 'reason': 'label_missing'})
        elif y.nunique() < 2 or int(y.sum()) < int(PARAMS.get('EXIT_MODEL_MIN_POSITIVES', 20)):
            info.update({'status': 'SKIP', 'reason': 'label_single_class_or_too_few_positives'})
        else:
            model = _fit_model(X, y)
            joblib.dump(model, _exit_model_pkl(label))
            info.update({'status': 'SAVE', 'feature_count': int(len(exit_selected))})
            saved += 1
        report['models'][label] = info
    report['saved_model_count'] = saved
    report['status'] = 'trained' if saved else 'no_exit_models_saved'
    return report

def _directional_feature_pkl(lane: str) -> Path:
    return Path('models') / f'selected_features_{str(lane).lower()}.pkl'


def _directional_model_pkl(lane: str, regime: str) -> Path:
    return Path('models') / f'model_{str(lane).lower()}_{regime}.pkl'


def _delete_if_exists(path: Path) -> dict[str, Any]:
    info = {'path': str(path), 'existed': bool(path.exists()), 'removed': False}
    if path.exists():
        try:
            path.unlink()
            info['removed'] = True
        except Exception as exc:
            info['error'] = repr(exc)
    return info


def _purge_stale_lane_artifacts(lane: str, reason: str) -> dict[str, Any]:
    """Remove stale directional artifacts for a lane that did not train cleanly.

    This prevents an old selected_features_long.pkl / model_long_*.pkl from
    making readiness think a fresh independent lane model is available.
    """
    lane = str(lane).upper()
    removed = [_delete_if_exists(_directional_feature_pkl(lane))]
    for regime in REGIMES:
        removed.append(_delete_if_exists(_directional_model_pkl(lane, regime)))
    report = {
        'lane': lane,
        'reason': reason,
        'removed': removed,
        'removed_count': sum(1 for x in removed if x.get('removed')),
    }
    Path('runtime').mkdir(parents=True, exist_ok=True)
    audit_path = Path('runtime') / 'stale_lane_artifact_cleanup.json'
    history = []
    if audit_path.exists():
        try:
            old = json.loads(audit_path.read_text(encoding='utf-8'))
            history = list(old.get('history', [])) if isinstance(old, dict) else []
        except Exception:
            history = []
    history.append({**report, 'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
    audit_path.write_text(json.dumps({'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'history': history[-200:]}, ensure_ascii=False, indent=2), encoding='utf-8')
    return report


def _evaluate_alpha(signal: pd.Series, future_return: pd.Series) -> dict[str, float] | None:
    df = pd.DataFrame({'signal': pd.to_numeric(signal, errors='coerce'), 'ret': pd.to_numeric(future_return, errors='coerce')}).dropna()
    if len(df) < 40:
        return None
    direction = np.sign(df['signal']).replace(0, 1)
    realized = direction * df['ret']
    hit_rate = float((np.sign(realized) > 0).mean())
    avg_return = float(realized.mean())
    wins = realized[realized > 0]
    losses = realized[realized <= 0]
    profit_factor = float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 1e-12 else 99.9
    expectancy = float(realized.mean())
    corr = float(abs(pd.Series(signal).corr(pd.Series(future_return)))) if len(df) >= 10 else 0.0
    active_ratio = float((pd.Series(signal).abs() > 1e-12).mean())
    score = expectancy * 100 + hit_rate * 10 + min(profit_factor, 5.0) + corr * 2.0 + active_ratio
    return {
        'hit_rate': hit_rate,
        'avg_return': avg_return,
        'profit_factor': profit_factor,
        'expectancy': expectancy,
        'score': score,
        'corr_abs': corr,
        'active_ratio': active_ratio,
    }



def _run_feature_review_gate_before_training() -> dict[str, Any]:
    try:
        from fts_feature_review_service import FeatureReviewService
        path, payload = FeatureReviewService().build()
        return {'status': 'ok', 'path': str(path), 'payload_status': payload.get('status') if isinstance(payload, dict) else None}
    except Exception as exc:
        return {'status': 'error', 'error': repr(exc)}


def _load_feature_review_policy() -> dict[str, Any]:
    for path in [Path('models')/'approved_features_review.json', Path('runtime')/'feature_review_report.json']:
        if path.exists():
            try:
                data=json.loads(path.read_text(encoding='utf-8'))
                if isinstance(data, dict): data['_policy_path']=str(path); return data
            except Exception: pass
    return {'_policy_path':'','approved_features':[],'rejected_features':[],'noise_watchlist':[]}


def _apply_feature_review_policy(all_features: list[str], df: pd.DataFrame) -> tuple[list[str], dict[str, Any]]:
    policy = _load_feature_review_policy()
    enforce = bool(PARAMS.get('MODEL_ENFORCE_FEATURE_REVIEW', True))
    approved = {str(x) for x in policy.get('approved_features', []) if str(x)}
    rejected = {str(x) for x in policy.get('rejected_features', []) if str(x)}
    watch = {str(x) for x in policy.get('noise_watchlist', []) if str(x)}
    before = list(dict.fromkeys(all_features))
    policy_status = str(policy.get('status') or '')
    if not enforce:
        return before, {'status': 'feature_review_not_enforced', 'policy_path': policy.get('_policy_path', ''), 'input_count': len(before), 'output_count': len(before)}
    # v92：若特徵審核沒有正式完成，正式訓練 fail-closed，避免又回到噪音特徵全收。
    if policy.get('fail_closed') or policy_status.startswith(('blocked', 'waiting')) or not policy.get('_policy_path'):
        return [], {
            'status': 'feature_review_fail_closed',
            'policy_status': policy_status or 'missing_policy',
            'policy_path': policy.get('_policy_path', ''),
            'input_count': len(before),
            'output_count': 0,
            'approved_count': len(approved),
            'rejected_count': len(rejected),
            'noise_watchlist_count': len(watch),
            'hard_blocks_noise_features': True,
            'reason': 'feature_review_not_ready_or_training_data_missing',
        }
    if approved:
        allowed = approved - rejected - watch
        after = [f for f in before if f in allowed]
        mode = 'approved_features_only'
    else:
        banned = rejected | watch
        after = [f for f in before if f not in banned]
        mode = 'reject_and_noise_exclusion_only'
    after = [f for f in after if f in df.columns and f not in META_DROP_COLS]
    return after, {
        'status': 'feature_review_enforced',
        'mode': mode,
        'policy_status': policy_status,
        'policy_path': policy.get('_policy_path', ''),
        'input_count': len(before),
        'output_count': len(after),
        'approved_count': len(approved),
        'rejected_count': len(rejected),
        'noise_watchlist_count': len(watch),
        'dropped_by_review_count': len(before) - len(after),
        'dropped_by_review_preview': [f for f in before if f not in set(after)][:80],
        'hard_blocks_noise_features': True,
        'train_live_parity_enforced': bool(policy.get('train_live_parity_enforced', False)),
    }


def _build_feature_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    raw_features = [c for c in df.columns if c not in META_DROP_COLS]
    old_features: list[str] = []
    pkl = Path('models') / 'selected_features.pkl'
    if pkl.exists():
        try:
            old_features = list(joblib.load(pkl))
        except Exception:
            old_features = []
    return list(dict.fromkeys(raw_features + old_features)), META_DROP_COLS


def _materialize_interactions(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    out = df.copy()
    for feature in list(features):
        if '_X_' not in feature or feature in out.columns:
            continue
        parts = [p for p in feature.split('_X_') if p]
        if all(p in out.columns for p in parts):
            vals = pd.to_numeric(out[parts[0]], errors='coerce').fillna(0.0)
            for p in parts[1:]:
                vals = vals * pd.to_numeric(out[p], errors='coerce').fillna(0.0)
            out[feature] = vals
    return out


def _chronological_split(df: pd.DataFrame, oot_ratio: float = 0.20) -> tuple[pd.DataFrame, pd.DataFrame]:
    n = len(df)
    oot_n = max(int(n * oot_ratio), 60)
    oot_n = min(oot_n, max(n // 3, 1))
    if n <= oot_n + 80:
        return df.iloc[:-max(20, oot_n // 2)].copy(), df.iloc[-max(20, oot_n // 2):].copy()
    return df.iloc[:-oot_n].copy(), df.iloc[-oot_n:].copy()


def _candidate_bucket(name: str) -> str:
    return FEATURE_SPECS.get(name).bucket if name in FEATURE_SPECS else ('mounted' if str(name).startswith('MOUNT__') else 'other')


def _select_features_train_only(train_df: pd.DataFrame, all_features: list[str]) -> tuple[list[str], list[dict[str, Any]]]:
    future_return = pd.to_numeric(train_df['Target_Return'], errors='coerce').fillna(0.0)
    ranked: list[dict[str, Any]] = []
    for col in all_features:
        if col not in train_df.columns or col in META_DROP_COLS:
            continue
        signal = pd.to_numeric(train_df[col], errors='coerce').fillna(0.0)
        if signal.nunique() <= 1:
            continue
        res = _evaluate_alpha(signal, future_return)
        if not res:
            continue
        ranked.append({
            'feature': col,
            'bucket': _candidate_bucket(col),
            'priority': int(col in PRIORITY_NEW_FEATURES_20),
            **res,
        })
    ranked.sort(key=lambda x: (x['score'], x['priority'], x['profit_factor'], x['corr_abs']), reverse=True)

    min_count = int(PARAMS.get('MODEL_MIN_SELECTED_FEATURES', 8))
    max_count = int(PARAMS.get('MODEL_MAX_SELECTED_FEATURES', 18))
    seed_limit = int(PARAMS.get('MODEL_SEED_FEATURE_LIMIT', 12))

    chosen: list[str] = []
    used_buckets: set[str] = set()
    # Phase 1: quality-first with bucket diversification
    for item in ranked:
        if item['feature'] in chosen:
            continue
        if item['expectancy'] <= 0 and item['profit_factor'] < 1.0 and item['corr_abs'] < 0.03:
            continue
        bucket = item['bucket']
        if bucket not in used_buckets or len(chosen) < min_count // 2:
            chosen.append(item['feature'])
            used_buckets.add(bucket)
        if len(chosen) >= max(min_count, 6):
            break

    # Phase 2: fill from top-ranked features regardless of bucket until min count reached
    if len(chosen) < min_count:
        for item in ranked:
            if item['feature'] not in chosen:
                chosen.append(item['feature'])
            if len(chosen) >= min_count:
                break

    # Phase 3: backfill with strategic priority features if still too few
    if len(chosen) < min_count:
        for feat in PRIORITY_NEW_FEATURES_20:
            if feat in train_df.columns and feat not in chosen:
                chosen.append(feat)
            if len(chosen) >= min_count:
                break

    # Phase 4: absolute fallback from raw features
    if len(chosen) < min_count:
        for feat in all_features:
            if feat in train_df.columns and feat not in chosen and feat not in META_DROP_COLS:
                chosen.append(feat)
            if len(chosen) >= min_count:
                break

    base_for_combo = chosen[:min(seed_limit, len(chosen))]
    combos: list[str] = []
    for r in [2, 3]:
        for combo in itertools.combinations(base_for_combo, r):
            name = '_X_'.join(combo)
            if name in train_df.columns:
                signal = pd.to_numeric(train_df[name], errors='coerce').fillna(0.0)
            else:
                if not all(p in train_df.columns for p in combo):
                    continue
                signal = train_df[list(combo)].apply(pd.to_numeric, errors='coerce').fillna(0.0).prod(axis=1)
            if int((signal != 0).sum()) < 20:
                continue
            res = _evaluate_alpha(signal, future_return)
            if res and ((res['expectancy'] > 0 and res['profit_factor'] >= 1.0) or (res['score'] > 6.0 and res['corr_abs'] >= 0.05)):
                combos.append(name)
        if len(combos) >= max(2, min_count // 3):
            break

    final = list(dict.fromkeys(chosen + combos))[:max_count]
    return final, ranked[:50]


def _fit_model(X_train: pd.DataFrame, y_train: pd.Series) -> RandomForestClassifier:
    model = RandomForestClassifier(
        n_estimators=int(PARAMS.get('MODEL_N_ESTIMATORS', 200)),
        max_depth=int(PARAMS.get('MODEL_MAX_DEPTH', 7)),
        random_state=42,
        class_weight='balanced',
        min_samples_leaf=int(PARAMS.get('MODEL_MIN_SAMPLES_LEAF', 3)),
    )
    model.fit(X_train, y_train)
    return model


def _evaluate_predictions(pred: np.ndarray, y_true: pd.Series, ret: pd.Series) -> dict[str, float]:
    strategy_returns = np.where(pred == 1, pd.to_numeric(ret, errors='coerce').fillna(0.0).values, 0.0)
    gross_profit = float(strategy_returns[strategy_returns > 0].sum()) if np.any(strategy_returns > 0) else 0.0
    gross_loss = float(abs(strategy_returns[strategy_returns < 0].sum())) if np.any(strategy_returns < 0) else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 1e-12 else 99.9
    return {
        'hit_rate': float(np.mean(strategy_returns > 0)) if len(strategy_returns) else 0.0,
        'avg_return': float(np.mean(strategy_returns)) if len(strategy_returns) else 0.0,
        'pred_accuracy': float(accuracy_score(y_true, pred)) if len(y_true) else 0.0,
        'profit_factor': float(profit_factor),
        'coverage': float(np.mean(pred == 1)) if len(pred) else 0.0,
    }


def _purged_walk_forward(X: pd.DataFrame, y: pd.Series, target_return: pd.Series, gap: int = 5, splits: int = 5) -> tuple[list[dict[str, float]], dict[str, Any]]:
    tscv = TimeSeriesSplit(n_splits=max(splits, 3))
    results: list[dict[str, float]] = []
    effective_splits = 0
    for train_idx, test_idx in tscv.split(X):
        if gap > 0 and len(train_idx) > gap:
            train_idx = train_idx[:-gap]
        if len(train_idx) < 80 or len(test_idx) < 20:
            continue
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        ret_test = target_return.iloc[test_idx]
        if y_train.nunique() < 2 or y_test.nunique() < 2:
            continue
        model = _fit_model(X_train, y_train)
        pred = model.predict(X_test)
        metrics = _evaluate_predictions(pred, y_test, ret_test)
        results.append(metrics)
        effective_splits += 1
    summary = {
        'ret_mean': float(np.mean([r['avg_return'] for r in results])) if results else 0.0,
        'hit_rate_mean': float(np.mean([r['hit_rate'] for r in results])) if results else 0.0,
        'pf_mean': float(np.mean([r['profit_factor'] for r in results])) if results else 0.0,
        'coverage_mean': float(np.mean([r['coverage'] for r in results])) if results else 0.0,
        'effective_splits': effective_splits,
        'score': float(100 * np.mean([r['avg_return'] for r in results])) + float(np.mean([r['profit_factor'] for r in results])) if results else 0.0,
    }
    return results, summary



def _sample_type_series(df: pd.DataFrame) -> pd.Series:
    if 'Sample_Type' not in df.columns:
        return pd.Series(['ENTRY_SIGNAL'] * len(df), index=df.index, dtype=object)
    return df['Sample_Type'].astype(str).str.upper().replace({'': 'ENTRY_SIGNAL'})


def _split_entry_and_position_day_frames(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    st = _sample_type_series(df)
    position_mask = st.eq('POSITION_DAY') | pd.to_numeric(df.get('Is_Position_Day', 0), errors='coerce').fillna(0).astype(int).eq(1)
    entry_df = df.loc[~position_mask].copy()
    position_df = df.loc[position_mask].copy()
    if entry_df.empty and position_df.empty:
        entry_df = df.copy()
    if entry_df.empty:
        entry_df = df.copy()
    report = {
        'entry_signal_rows': int(len(entry_df)),
        'position_day_rows': int(len(position_df)),
        'position_day_required_for_exit': bool(PARAMS.get('EXIT_MODEL_REQUIRE_POSITION_DAY_SAMPLES', True)),
        'sample_type_counts': {str(k): int(v) for k, v in st.value_counts(dropna=False).to_dict().items()},
    }
    return entry_df, position_df, report

def train_models() -> tuple[Path, dict[str, Any]]:
    dataset_path = Path('data/ml_training_data.csv')
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not dataset_path.exists():
        payload = {'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'status': 'dataset_missing', 'dataset_path': str(dataset_path)}
        return _emit_blocked_training_report(payload, reason='training_dataset_missing', category='missing_market_data')

    pretrain_version = create_version_tag('pretrain')
    snapshot_current_models(pretrain_version, note='重訓前自動備份（任務收尾版）')

    try:
        if dataset_path.stat().st_size == 0:
            payload = {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'dataset_empty',
                'dataset_path': str(dataset_path),
                'reason': 'csv_file_is_zero_bytes',
            }
            return _emit_blocked_training_report(payload, reason='csv_file_is_zero_bytes', category='missing_market_data')
        df = pd.read_csv(dataset_path)
    except EmptyDataError:
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'dataset_empty',
            'dataset_path': str(dataset_path),
            'reason': 'no_columns_to_parse_from_file',
        }
        return _emit_blocked_training_report(payload, reason='no_columns_to_parse_from_file', category='missing_market_data')

    if df.empty or len(df.columns) == 0:
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'dataset_empty',
            'dataset_path': str(dataset_path),
            'reason': 'parsed_dataframe_is_empty',
        }
        return _emit_blocked_training_report(payload, reason='parsed_dataframe_is_empty', category='missing_market_data')

    df, quality_report = validate_training_frame(df, min_rows=max(80, int(PARAMS.get('MODEL_MIN_TRAIN_ROWS', 80))))
    if quality_report.get('status') == 'blocked':
        payload = {'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'status': 'blocked_by_data_quality', 'dataset_path': str(dataset_path), 'quality_report': quality_report}
        q_reason = str((quality_report or {}).get('reason') or (quality_report or {}).get('status_detail') or '')
        if 'label' in q_reason.lower():
            return _emit_blocked_training_report(payload, reason=q_reason or 'label_missing', category='missing_label')
        if 'row' in q_reason.lower() or 'sample' in q_reason.lower():
            return _emit_blocked_training_report(payload, reason=q_reason or 'insufficient_rows', category='insufficient_samples')
        return _emit_blocked_training_report(payload, reason=q_reason or 'data_quality_blocked', category='missing_market_data')
    if 'Date' in df.columns:
        df = df.sort_values('Date').reset_index(drop=True)
    df, target_return_report = _validate_target_return_frame(df)
    if df is None:
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'blocked_by_target_return',
            'dataset_path': str(dataset_path),
            'target_return_report': target_return_report,
        }
        return _emit_blocked_training_report(payload, reason=target_return_report.get('reason', 'target_return_blocked'), category='invalid_target_return')
    if len(df) < max(80, int(PARAMS.get('MODEL_MIN_TRAIN_ROWS', 80))):
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'blocked_by_target_return_row_count',
            'dataset_path': str(dataset_path),
            'target_return_report': target_return_report,
            'rows_after_target_return_clean': int(len(df)),
        }
        return _emit_blocked_training_report(payload, reason='target_return_clean_rows_below_floor', category='insufficient_samples')
    df['Label_Y'] = pd.to_numeric(df['Label_Y'], errors='coerce').fillna(0).astype(int)
    raw_df_for_exit = df.copy()
    entry_df, position_day_df, sample_type_report = _split_entry_and_position_day_frames(df)
    df = entry_df.copy()
    if len(df) < max(80, int(PARAMS.get('MODEL_MIN_TRAIN_ROWS', 80))):
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'blocked_by_entry_signal_row_count',
            'dataset_path': str(dataset_path),
            'target_return_report': target_return_report,
            'sample_type_report': sample_type_report,
            'rows_after_entry_filter': int(len(df)),
        }
        return _emit_blocked_training_report(payload, reason='entry_signal_rows_below_floor', category='insufficient_samples')

    feature_review_build = _run_feature_review_gate_before_training()
    all_features, _ = _build_feature_columns(df)
    all_features, feature_review_gate = _apply_feature_review_policy(all_features, df)
    min_feature_floor = int(PARAMS.get('MODEL_MIN_SELECTED_FEATURES', 8))
    if len(all_features) < min_feature_floor:
        payload = {'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'status': 'blocked_by_feature_review_gate', 'dataset_path': str(dataset_path), 'feature_review_build': feature_review_build, 'feature_review_gate': feature_review_gate, 'min_feature_floor': min_feature_floor}
        return _emit_blocked_training_report(payload, reason='feature_review_gate_selected_too_few_features', category='insufficient_features_after_review')
    df = _materialize_interactions(df, all_features)
    train_df, oot_df = _chronological_split(df, oot_ratio=float(PARAMS.get('OOT_RATIO', 0.20)))
    selected_features, ranked_features = _select_features_train_only(train_df, all_features)
    train_df = _materialize_interactions(train_df, selected_features)
    oot_df = _materialize_interactions(oot_df, selected_features)
    selected_features = [f for f in selected_features if f in train_df.columns and f in oot_df.columns]

    X_train = train_df[selected_features].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    y_train = train_df['Label_Y']
    X_oot = oot_df[selected_features].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    y_oot = oot_df['Label_Y']
    ret_oot = oot_df['Target_Return']

    wf_results, wf_summary = _purged_walk_forward(
        X_train,
        y_train,
        train_df['Target_Return'],
        gap=int(PARAMS.get('WF_GAP', 5)),
        splits=int(PARAMS.get('WF_SPLITS', 5)),
    )

    feature_to_sample_ratio = float(len(selected_features) / max(len(train_df), 1))
    model = _fit_model(X_train, y_train) if y_train.nunique() >= 2 and len(X_train) >= 80 and len(selected_features) >= int(PARAMS.get('MODEL_MIN_SELECTED_FEATURES', 8)) else None
    oot_metrics = {'hit_rate': 0.0, 'avg_return': 0.0, 'pred_accuracy': 0.0, 'profit_factor': 0.0, 'coverage': 0.0}
    if model is not None and len(X_oot) > 0 and y_oot.nunique() >= 1:
        pred_oot = model.predict(X_oot)
        oot_metrics = _evaluate_predictions(pred_oot, y_oot, ret_oot)

    overfit_gap = float(max(0.0, wf_summary.get('ret_mean', 0.0) - oot_metrics.get('avg_return', 0.0)))
    leakage_guards = {
        'feature_selection_train_only': True,
        'purged_walk_forward': True,
        'out_of_time_holdout': True,
    }
    warnings: list[str] = []
    if len(selected_features) < int(PARAMS.get('MODEL_MIN_SELECTED_FEATURES', 8)):
        warnings.append('selected_features_below_minimum')
    if oot_metrics.get('profit_factor', 0.0) < float(PARAMS.get('MODEL_MIN_OOT_PF', 1.0)):
        warnings.append('oot_profit_factor_below_floor')
    if oot_metrics.get('hit_rate', 0.0) < float(PARAMS.get('MODEL_MIN_OOT_HIT_RATE', 0.45)):
        warnings.append('oot_hit_rate_below_floor')
    if wf_summary.get('effective_splits', 0) < 3:
        warnings.append('insufficient_walk_forward_splits')

    report = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'trained' if model is not None else 'blocked',
        'blocked_reason_category': '' if model is not None else 'insufficient_samples',
        'dataset_path': str(dataset_path),
        'rows_total': int(len(df)),
        'rows_train': int(len(train_df)),
        'rows_out_of_time': int(len(oot_df)),
        'selected_features_count': int(len(selected_features)),
        'selected_features': selected_features,
        'feature_to_sample_ratio': feature_to_sample_ratio,
        'walk_forward_summary': wf_summary,
        'walk_forward_results': wf_results[:10],
        'out_of_time': oot_metrics,
        'overfit_gap': overfit_gap,
        'leakage_guards': leakage_guards,
        'class_balance_train': float(y_train.mean()) if len(y_train) else 0.0,
        'class_balance_oot': float(y_oot.mean()) if len(y_oot) else 0.0,
        'feature_selection_preview': ranked_features[:20],
        'warnings': warnings,
        'target_return_report': target_return_report,
        'sample_type_report': sample_type_report,
        'feature_review_build': feature_review_build,
        'feature_review_gate': feature_review_gate,
    }

    os.makedirs('models', exist_ok=True)
    joblib.dump(selected_features, 'models/selected_features.pkl')

    regimes = list(REGIMES)
    metrics_by_regime: dict[str, Any] = {}
    save_count = 0
    for regime in regimes:
        regime_df = train_df[train_df['Regime'] == regime].copy() if 'Regime' in train_df.columns else pd.DataFrame()
        model_path = Path(f'models/model_{regime}.pkl')
        if len(regime_df) < int(PARAMS.get('MODEL_MIN_REGIME_SAMPLES', 50)):
            metrics_by_regime[regime] = {'status': 'SKIP', 'reason': '樣本不足'}
            continue
        safe_features = [f for f in selected_features if f in regime_df.columns]
        if len(safe_features) < max(4, int(PARAMS.get('MODEL_MIN_SELECTED_FEATURES', 8)) // 2):
            metrics_by_regime[regime] = {'status': 'SKIP', 'reason': '無足夠可用特徵'}
            continue
        X_reg = regime_df[safe_features].replace([np.inf, -np.inf], np.nan).fillna(0.0)
        y_reg = regime_df['Label_Y']
        if y_reg.nunique() < 2:
            metrics_by_regime[regime] = {'status': 'SKIP', 'reason': '標籤單一'}
            continue
        reg_model = _fit_model(X_reg, y_reg)
        joblib.dump(reg_model, model_path)
        metrics_by_regime[regime] = {'status': 'SAVE', 'rows': int(len(regime_df)), 'feature_count': int(len(safe_features))}
        save_count += 1

    lane_series = _build_lane_series(train_df)
    lane_artifacts: dict[str, Any] = {}
    min_selected = int(PARAMS.get('MODEL_MIN_SELECTED_FEATURES', 8))
    for lane in LANES:
        lane_mask = lane_series == lane
        lane_df = train_df.loc[lane_mask].copy()
        lane_report: dict[str, Any] = {
            'rows': int(len(lane_df)),
            'selected_features_path': str(_directional_feature_pkl(lane)),
            'selection_source': 'not_selected',
            'independent_lane_required': bool(PARAMS.get('DIRECTIONAL_REQUIRE_INDEPENDENT_LANE_MODELS', True)),
            'selected_feature_count': 0,
            'selected_features_preview': [],
            'ranked_preview': [],
            'models': {},
        }
        lane_selected = list(selected_features)
        lane_ranked: list[dict[str, Any]] = []
        if len(lane_df) >= max(60, min_selected * 8) and lane_df['Label_Y'].nunique() >= 2:
            lane_features, lane_ranked = _select_features_train_only(lane_df, all_features)
            lane_df = _materialize_interactions(lane_df, lane_features)
            lane_features = [f for f in lane_features if f in lane_df.columns]
            if len(lane_features) >= max(4, min_selected // 2):
                lane_selected = lane_features
                lane_report['selection_source'] = 'lane_train_only'
            else:
                lane_report['selection_source'] = 'blocked_low_feature_count'
        else:
            lane_report['selection_source'] = 'blocked_low_samples'
        if lane_report.get('selection_source') != 'lane_train_only':
            lane_selected = []
        lane_selected = list(dict.fromkeys([f for f in lane_selected if f in train_df.columns]))
        if lane_selected:
            joblib.dump(lane_selected, _directional_feature_pkl(lane))
        lane_report['selected_feature_count'] = int(len(lane_selected))
        lane_report['selected_features_preview'] = lane_selected[:20]
        lane_report['ranked_preview'] = lane_ranked[:15]

        lane_save_count = 0
        for regime in REGIMES:
            lane_reg_df = train_df.loc[lane_mask & (train_df['Regime'] == regime)].copy() if 'Regime' in train_df.columns else pd.DataFrame()
            model_path = _directional_model_pkl(lane, regime)
            if len(lane_reg_df) >= int(PARAMS.get('MODEL_MIN_REGIME_SAMPLES', 50)) and lane_reg_df['Label_Y'].nunique() >= 2:
                safe_features = [f for f in lane_selected if f in lane_reg_df.columns]
                if len(safe_features) >= max(4, min_selected // 2):
                    X_lane = lane_reg_df[safe_features].replace([np.inf, -np.inf], np.nan).fillna(0.0)
                    y_lane = lane_reg_df['Label_Y']
                    lane_model = _fit_model(X_lane, y_lane)
                    joblib.dump(lane_model, model_path)
                    lane_report['models'][regime] = {'status': 'SAVE', 'rows': int(len(lane_reg_df)), 'feature_count': int(len(safe_features)), 'path': str(model_path)}
                    lane_save_count += 1
                    continue
            lane_report['models'][regime] = {
                'status': 'SKIP',
                'reason': 'independent_lane_model_not_trained_shared_fallback_forbidden',
                'rows': int(len(lane_reg_df)),
                'path': str(model_path),
            }
        lane_report['saved_model_count'] = lane_save_count
        if bool(PARAMS.get('DIRECTIONAL_REQUIRE_INDEPENDENT_LANE_MODELS', True)) and (
            lane_save_count <= 0 or lane_report.get('selection_source') != 'lane_train_only'
        ):
            lane_report['stale_artifact_cleanup'] = _purge_stale_lane_artifacts(
                lane,
                reason='independent_lane_training_failed_or_not_selected',
            )
        lane_artifacts[lane] = lane_report

    exit_training_df = position_day_df.copy() if not position_day_df.empty else pd.DataFrame()
    if not exit_training_df.empty:
        exit_training_df = _materialize_interactions(exit_training_df, selected_features)
    exit_model_artifacts = _train_exit_models(exit_training_df, all_features, selected_features)

    exit_model_artifacts['training_source'] = 'position_day_samples' if not position_day_df.empty else 'missing_position_day_samples'
    exit_model_artifacts['position_day_rows_available'] = int(len(position_day_df))
    exit_model_artifacts['entry_signal_rows_used_for_entry_models'] = int(len(train_df) + len(oot_df))

    overall_score = float(oot_metrics['avg_return'] * 100 + min(oot_metrics['profit_factor'], 5.0) + oot_metrics['hit_rate'] * 10)
    version_tag = create_version_tag('trained')
    snapshot_current_models(version_tag, metrics={'overall_score': round(overall_score, 4), 'out_of_time': oot_metrics, 'walk_forward': wf_summary, 'feature_count': len(selected_features), 'directional_lane_feature_counts': {k: v.get('selected_feature_count', 0) for k, v in lane_artifacts.items()}, 'directional_lane_model_counts': {k: v.get('saved_model_count', 0) for k, v in lane_artifacts.items()}}, note='任務收尾版訓練完成快照')

    promotion_floors = {
        'min_selected_features': int(PARAMS.get('MODEL_MIN_SELECTED_FEATURES', 8)),
        'min_overall_score': float(PARAMS.get('MODEL_MIN_PROMOTION_SCORE', 2.0)),
        'min_oot_profit_factor': float(PARAMS.get('MODEL_MIN_OOT_PF', 1.15)),
        'min_oot_hit_rate': float(PARAMS.get('MODEL_MIN_OOT_HIT_RATE', 0.52)),
        'min_wf_effective_splits': int(PARAMS.get('MODEL_MIN_WF_EFFECTIVE_SPLITS', 3)),
        'min_wf_ret_mean': float(PARAMS.get('MODEL_MIN_WF_RET_MEAN', 0.0)),
    }
    promotion_failures = []
    if save_count <= 0:
        promotion_failures.append('no_regime_models_saved')
    if len(selected_features) < promotion_floors['min_selected_features']:
        promotion_failures.append('selected_features_below_floor')
    if overall_score < promotion_floors['min_overall_score']:
        promotion_failures.append('overall_score_below_floor')
    if oot_metrics.get('profit_factor', 0.0) < promotion_floors['min_oot_profit_factor']:
        promotion_failures.append('oot_profit_factor_below_floor')
    if oot_metrics.get('hit_rate', 0.0) < promotion_floors['min_oot_hit_rate']:
        promotion_failures.append('oot_hit_rate_below_floor')
    if int(wf_summary.get('effective_splits', 0) or 0) < promotion_floors['min_wf_effective_splits']:
        promotion_failures.append('walk_forward_effective_splits_below_floor')
    if float(wf_summary.get('ret_mean', 0.0) or 0.0) < promotion_floors['min_wf_ret_mean']:
        promotion_failures.append('walk_forward_return_below_floor')
    if bool(PARAMS.get('DIRECTIONAL_REQUIRE_INDEPENDENT_LANE_MODELS', True)):
        missing_independent = []
        for lane, lane_payload in lane_artifacts.items():
            if lane_payload.get('saved_model_count', 0) <= 0 or lane_payload.get('selection_source') != 'lane_train_only':
                missing_independent.append(lane)
        if missing_independent:
            promotion_failures.append('independent_directional_lane_models_missing:' + ','.join(missing_independent))
    promotion_ready = len(promotion_failures) == 0
    best_entry = get_best_version_entry()
    best_score = float(best_entry.get('metrics', {}).get('overall_score', -1e18)) if best_entry else -1e18

    promoted_current_candidate = False
    if promotion_ready and overall_score > best_score:
        promote_best_version(version_tag)
        promoted_current_candidate = True
        report['promotion'] = {'status': 'promoted_best', 'version': version_tag}
    elif save_count == 0:
        restore_version(pretrain_version)
        report['promotion'] = {'status': 'restored_pretrain', 'version': pretrain_version, 'reason': 'no_regime_models_saved'}
    elif not promotion_ready:
        restore_version(pretrain_version)
        report['promotion'] = {'status': 'restored_pretrain', 'version': pretrain_version, 'reason': 'promotion_floor_not_met', 'failures': promotion_failures}
    else:
        restore_version(pretrain_version)
        report['promotion'] = {'status': 'restored_pretrain', 'version': pretrain_version, 'reason': 'candidate_not_better_than_best', 'best_score': best_score}

    advanced_feature_candidates = [
        'Score_Gap_Slope_3d','ADX_Delta_3d','MACD_Hist_Delta_3d','RSI_Reclaim_Speed','BB_Squeeze_Release',
        'ATR_Expansion_Start','Volume_Z20_Delta','Foreign_Ratio_Delta_3d','Total_Ratio_Delta_3d','Bull_Emerging_Score',
        'Bear_Emerging_Score','Range_Compression_Score','Breakout_Readiness','Trend_Exhaustion_Score','Entry_Readiness',
        'Breakout_Risk_Next3','Reversal_Risk_Next3','Exit_Hazard_Score','Proba_Delta_3d','Trend_Confidence_Delta','Range_Confidence_Delta',
        'Regime_Confidence','Next_Regime_Prob_Bull','Next_Regime_Prob_Bear','Next_Regime_Prob_Range'
    ]
    report['advanced_feature_candidates_present'] = [c for c in advanced_feature_candidates if c in train_df.columns]
    report['advanced_feature_selected'] = [c for c in selected_features if c in advanced_feature_candidates]
    if 'Setup_Ready_Label' in df.columns:
        report['setup_ready_positive_ratio'] = float(pd.to_numeric(df['Setup_Ready_Label'], errors='coerce').fillna(0).mean())
    if 'Trigger_Confirm_Label' in df.columns:
        report['trigger_confirm_positive_ratio'] = float(pd.to_numeric(df['Trigger_Confirm_Label'], errors='coerce').fillna(0).mean())
    report['regimes'] = metrics_by_regime
    report['directional_lane_artifacts'] = lane_artifacts
    report['exit_model_artifacts'] = exit_model_artifacts
    for _exit_label in ['Exit_Defend_Label','Exit_Reduce_Label','Exit_Confirm_Label']:
        if _exit_label in df.columns:
            report[_exit_label + '_positive_ratio'] = float(pd.to_numeric(df[_exit_label], errors='coerce').fillna(0).mean())
    report['overall_score'] = overall_score
    report['promotion_floors'] = promotion_floors
    report['promotion_failures'] = promotion_failures
    report['promotion_ready'] = promotion_ready
    report['model_live_signal_gate'] = _write_model_live_signal_gate(report, promotion_ready=promotion_ready, promoted=promoted_current_candidate)
    RUNTIME_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return RUNTIME_PATH, report


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    path, payload = train_models()
    print(f'🧠 已輸出 trainer backend report：{path}')
    print(payload.get('status'))
