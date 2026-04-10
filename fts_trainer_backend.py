# -*- coding: utf-8 -*-
"""Safer trainer backend with train-only feature selection, purged walk-forward and out-of-time holdout."""
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
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import TimeSeriesSplit

from config import PARAMS
from model_governance import create_version_tag, get_best_version_entry, promote_best_version, restore_version, snapshot_current_models
from fts_data_quality_guard import validate_training_frame

RUNTIME_PATH = Path('runtime') / 'trainer_backend_report.json'


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
    score = expectancy * 100 + hit_rate * 10 + min(profit_factor, 5.0)
    return {'hit_rate': hit_rate, 'avg_return': avg_return, 'profit_factor': profit_factor, 'expectancy': expectancy, 'score': score}


def _build_feature_columns(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    drop_cols = ['Ticker', 'Ticker SYMBOL', 'Date', 'Setup', 'Setup_Tag', 'Regime', 'Label', 'Label_Y', 'Target_Return', 'Future_Return_Pct', 'Entry_Price', 'Entry_Price_Basis', 'Exit_Price', 'Entry_Date', 'Exit_Date', 'Direction', 'Stop_Hit', 'Hold_Days', 'Touched_TP', 'Touched_SL', 'Label_Reason', 'Label_Exit_Type', 'Favorable_Move_Pct', 'Adverse_Move_Pct', 'Max_Favorable_Excursion', 'Max_Adverse_Excursion', 'Realized_Return_After_Cost']
    raw_features = [c for c in df.columns if c not in drop_cols]
    old_features: list[str] = []
    pkl = Path('models') / 'selected_features.pkl'
    if pkl.exists():
        try:
            old_features = list(joblib.load(pkl))
        except Exception:
            old_features = []
    return list(dict.fromkeys(raw_features + old_features)), drop_cols


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


def _select_features_train_only(train_df: pd.DataFrame, all_features: list[str]) -> list[str]:
    future_return = pd.to_numeric(train_df['Target_Return'], errors='coerce').fillna(0.0)
    qualified: list[str] = []
    for col in all_features:
        if col not in train_df.columns:
            continue
        signal = pd.to_numeric(train_df[col], errors='coerce').fillna(0.0)
        if signal.nunique() <= 1:
            continue
        res = _evaluate_alpha(signal, future_return)
        if not res:
            continue
        if res['expectancy'] > 0 and res['hit_rate'] >= 0.50:
            qualified.append(col)
    if not qualified:
        qualified = [f for f in all_features if f in train_df.columns]
    seed_limit = int(PARAMS.get('MODEL_SEED_FEATURE_LIMIT', 12))
    seed_features = qualified[:seed_limit]
    combos: list[str] = []
    for r in [2, 3]:
        for combo in itertools.combinations(seed_features, r):
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
            if res and res['expectancy'] > 0 and res['hit_rate'] >= 0.55:
                combos.append(name)
    return list(dict.fromkeys(qualified + combos))


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


def train_models() -> tuple[Path, dict[str, Any]]:
    dataset_path = Path('data/ml_training_data.csv')
    RUNTIME_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not dataset_path.exists():
        payload = {'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'status': 'dataset_missing', 'dataset_path': str(dataset_path)}
        RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return RUNTIME_PATH, payload

    pretrain_version = create_version_tag('pretrain')
    snapshot_current_models(pretrain_version, note='重訓前自動備份（防過擬合版）')

    df = pd.read_csv(dataset_path)
    df, quality_report = validate_training_frame(df, min_rows=max(80, int(PARAMS.get('MODEL_MIN_TRAIN_ROWS', 80))))
    if quality_report.get('status') == 'blocked':
        payload = {'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'status': 'blocked_by_data_quality', 'dataset_path': str(dataset_path), 'quality_report': quality_report}
        RUNTIME_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return RUNTIME_PATH, payload
    if 'Date' in df.columns:
        df = df.sort_values('Date').reset_index(drop=True)
    if 'Target_Return' not in df.columns:
        df['Target_Return'] = np.where(pd.to_numeric(df.get('Label_Y', 0), errors='coerce').fillna(0).astype(int) == 1, 0.05, -0.05)
    else:
        df['Target_Return'] = pd.to_numeric(df['Target_Return'], errors='coerce').fillna(0.0)
    df['Label_Y'] = pd.to_numeric(df['Label_Y'], errors='coerce').fillna(0).astype(int)

    all_features, _ = _build_feature_columns(df)
    df = _materialize_interactions(df, all_features)
    train_df, oot_df = _chronological_split(df, oot_ratio=float(PARAMS.get('OOT_RATIO', 0.20)))
    selected_features = _select_features_train_only(train_df, all_features)
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
    model = _fit_model(X_train, y_train) if y_train.nunique() >= 2 and len(X_train) >= 80 else None
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
    report = {
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'trained' if model is not None else 'blocked',
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
    }

    os.makedirs('models', exist_ok=True)
    joblib.dump(selected_features, 'models/selected_features.pkl')

    regimes = ['趨勢多頭', '區間盤整', '趨勢空頭']
    metrics_by_regime: dict[str, Any] = {}
    save_count = 0
    for regime in regimes:
        regime_df = train_df[train_df['Regime'] == regime].copy() if 'Regime' in train_df.columns else pd.DataFrame()
        model_path = Path(f'models/model_{regime}.pkl')
        if len(regime_df) < int(PARAMS.get('MODEL_MIN_REGIME_SAMPLES', 50)):
            metrics_by_regime[regime] = {'status': 'SKIP', 'reason': '樣本不足'}
            continue
        safe_features = [f for f in selected_features if f in regime_df.columns]
        if not safe_features:
            metrics_by_regime[regime] = {'status': 'SKIP', 'reason': '無可用特徵'}
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

    overall_score = float(oot_metrics['avg_return'] * 100 + min(oot_metrics['profit_factor'], 5.0) + oot_metrics['hit_rate'] * 10)
    version_tag = create_version_tag('trained')
    snapshot_current_models(version_tag, metrics={'overall_score': round(overall_score, 4), 'out_of_time': oot_metrics, 'walk_forward': wf_summary, 'feature_count': len(selected_features)}, note='防過擬合版訓練完成快照')
    best_entry = get_best_version_entry()
    best_score = float(best_entry.get('metrics', {}).get('overall_score', -1e18)) if best_entry else -1e18
    if overall_score > best_score and save_count > 0:
        promote_best_version(version_tag)
        report['promotion'] = {'status': 'promoted_best', 'version': version_tag}
    elif save_count == 0:
        restore_version(pretrain_version)
        report['promotion'] = {'status': 'restored_pretrain', 'version': pretrain_version}
    else:
        report['promotion'] = {'status': 'kept_current', 'version': version_tag}

    report['regimes'] = metrics_by_regime
    RUNTIME_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return RUNTIME_PATH, report


if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    path, payload = train_models()
    print(f'🧠 已輸出 trainer backend report：{path}')
    print(payload.get('status'))
