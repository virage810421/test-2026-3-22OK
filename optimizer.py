# -*- coding: utf-8 -*-
from __future__ import annotations

import random
from copy import deepcopy
from typing import Any

import numpy as np

from config import PARAMS as BASE_PARAMS, TRAINING_POOL
from screening import inspect_stock
from kline_cache import get_smart_klines
from param_storage import save_candidate_params
from fts_research_lab import ResearchLab
from fts_utils import now_str

_LAB = ResearchLab()


PARAM_SPACE = {
    'RSI_PERIOD': [10, 14, 18],
    'MACD_FAST': [10, 12, 15],
    'MACD_SLOW': [20, 26, 30],
    'ADX_TREND_THRESHOLD': [18, 20, 25],
    'VOL_BREAKOUT_MULTIPLIER': [1.05, 1.1, 1.2],
    'TRIGGER_SCORE': [2, 3],
    'TP_BASE_PCT': [0.08, 0.10, 0.12],
    'SL_MIN_PCT': [0.025, 0.03, 0.04],
}


def generate_random_params(base: dict[str, Any] | None = None) -> dict[str, Any]:
    p = deepcopy(base or BASE_PARAMS)
    for k, vals in PARAM_SPACE.items():
        p[k] = random.choice(vals)
    return p


def _split_df(df, split_ratio: float):
    split_idx = max(80, int(len(df) * split_ratio))
    return df.iloc[:split_idx].copy(), df.iloc[split_idx:].copy()


def _eval_result(res: dict[str, Any] | None) -> dict[str, float]:
    if not res:
        return {'ev': 0.0, 'wr': 0.0, 'ret': 0.0, 'kelly': 0.0, 'samples': 0.0}
    return {
        'ev': float(res.get('期望值', 0.0) or 0.0),
        'wr': float(res.get('系統勝率(%)', 0.0) or 0.0),
        'ret': float(res.get('累計報酬率(%)', 0.0) or 0.0),
        'kelly': float(res.get('Kelly建議倉位', 0.0) or 0.0),
        'samples': float(res.get('歷史訊號樣本數', 0.0) or 0.0),
    }


def run_walk_forward_optimization(iterations: int = 50, split_ratio: float = 0.7, ticker_list=None):
    targets = list(ticker_list or TRAINING_POOL)
    history = get_smart_klines(targets, period='2y')
    results = []
    for _ in range(int(iterations)):
        params = generate_random_params()
        train_scores = []
        test_scores = []
        for ticker in targets:
            df = history.get(ticker)
            if df is None or len(df) < 140:
                continue
            train_df, test_df = _split_df(df, split_ratio)
            train_res = _eval_result(inspect_stock(ticker, preloaded_df=train_df, p=params))
            test_res = _eval_result(inspect_stock(ticker, preloaded_df=test_df, p=params))
            train_scores.append(train_res)
            test_scores.append(test_res)
        if not train_scores:
            continue
        train_ev = float(np.mean([x['ev'] for x in train_scores]))
        train_wr = float(np.mean([x['wr'] for x in train_scores]))
        train_ret = float(np.mean([x['ret'] for x in train_scores]))
        test_ev = float(np.mean([x['ev'] for x in test_scores])) if test_scores else 0.0
        test_wr = float(np.mean([x['wr'] for x in test_scores])) if test_scores else 0.0
        test_ret = float(np.mean([x['ret'] for x in test_scores])) if test_scores else 0.0
        stability_penalty = abs(train_ev - test_ev) + abs(train_wr - test_wr) * 0.02
        score = train_ev + test_ev + (train_wr + test_wr) * 0.02 + (train_ret + test_ret) * 0.005 - stability_penalty
        results.append({
            'Params': params,
            'Train_EV': round(train_ev, 4),
            'Train_WinRate': round(train_wr, 2),
            'Train_TotalReturn': round(train_ret, 2),
            'Test_EV': round(test_ev, 4),
            'Test_WinRate': round(test_wr, 2),
            'Test_TotalReturn': round(test_ret, 2),
            'StabilityPenalty': round(stability_penalty, 4),
            'Score': round(score, 4),
        })
    if not results:
        return None
    best = sorted(results, key=lambda x: x['Score'], reverse=True)[0]
    save_candidate_params('walk_forward_default', best['Params'], metrics={k: v for k, v in best.items() if k != 'Params'}, source_module='optimizer.py', note='research-only walk-forward candidate')
    _LAB.write_json_artifact('optimizer_runs', f'walk_forward_best_{now_str().replace(":","").replace("-","").replace("T","_")}.json', best)
    return best


if __name__ == '__main__':
    print(run_walk_forward_optimization())
