# -*- coding: utf-8 -*-
"""Level-1 compatibility bridge for ml_trainer.

保留 advanced_chart(1) 時代的函式名稱；主訓練主線改由 fts_trainer_backend 提供。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit

import fts_trainer_backend as _backend

BRIDGE_LEVEL = 'level_1'
BRIDGE_TARGET = 'fts_trainer_backend.train_models'
LEGACY_SOURCE = 'advanced_chart(1).zip::ml_trainer.py'


def evaluate_alpha_full(signal, future_return):
    return _backend._evaluate_alpha(pd.Series(signal), pd.Series(future_return))


def walk_forward_analysis(X, y, target_return, p=None):
    p = p or getattr(_backend, 'PARAMS', {})
    splits = int(p.get('WF_SPLITS', 5))
    tscv = TimeSeriesSplit(n_splits=splits)
    results = []
    X = pd.DataFrame(X).reset_index(drop=True)
    y = pd.Series(y).reset_index(drop=True)
    target_return = pd.Series(target_return).reset_index(drop=True)
    for train_idx, test_idx in tscv.split(X):
        X_train, X_test = X.iloc[train_idx].copy(), X.iloc[test_idx].copy()
        y_train, y_test = y.iloc[train_idx].copy(), y.iloc[test_idx].copy()
        ret_test = target_return.iloc[test_idx].copy()
        if y_train.nunique() < 2 or y_test.nunique() < 2:
            continue
        model = _backend._fit_model(X_train.fillna(0), y_train.astype(int))
        pred = pd.Series(model.predict(X_test.fillna(0)), index=X_test.index)
        pred_proba = model.predict_proba(X_test.fillna(0))[:, 1] if hasattr(model, 'predict_proba') else np.array([0.5] * len(X_test))
        strategy_returns = np.where(pred == 1, ret_test.values, 0.0)
        gross_profit = strategy_returns[strategy_returns > 0].sum() if np.any(strategy_returns > 0) else 0.0
        gross_loss = abs(strategy_returns[strategy_returns < 0].sum()) if np.any(strategy_returns < 0) else 0.0
        results.append({
            'hit_rate': float(np.mean(strategy_returns > 0)) if len(strategy_returns) else 0.0,
            'return': float(np.mean(strategy_returns)) if len(strategy_returns) else 0.0,
            'profit_factor': float(gross_profit / gross_loss) if gross_loss > 0 else 99.9,
            'pred_accuracy': float(np.mean(pred.values == y_test.values)) if len(y_test) else 0.0,
            'sharpe_like': float(np.mean(strategy_returns) / np.std(strategy_returns)) if len(strategy_returns) and np.std(strategy_returns) > 0 else 0.0,
            'coverage': float(np.mean(pred == 1)) if len(pred) else 0.0,
            'mean_proba': float(np.mean(pred_proba)) if len(pred_proba) else 0.5,
        })
    return results


def evaluate_stability(results):
    if not results:
        return {
            'ret_mean': 0.0,
            'consistency': 0.0,
            'pf_mean': 0.0,
            'sharpe_mean': 0.0,
            'coverage_mean': 0.0,
        }
    returns = [r.get('return', 0.0) for r in results]
    pfs = [r.get('profit_factor', 0.0) for r in results]
    sharpes = [r.get('sharpe_like', 0.0) for r in results]
    coverages = [r.get('coverage', 0.0) for r in results]
    return {
        'ret_mean': float(np.mean(returns)),
        'consistency': float(np.mean([r > 0 for r in returns])),
        'pf_mean': float(np.mean(pfs)),
        'sharpe_mean': float(np.mean(sharpes)),
        'coverage_mean': float(np.mean(coverages)),
    }


def train_models():
    return _backend.train_models()


if __name__ == '__main__':
    train_models()
