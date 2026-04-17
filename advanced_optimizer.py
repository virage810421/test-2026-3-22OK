# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import random
from copy import deepcopy
from typing import Any

import numpy as np

from config import PARAMS, TRAINING_POOL
from fts_service_api import inspect_stock
from fts_market_data_service import MarketDataService

_MARKET_DATA_SERVICE = MarketDataService()


def get_smart_klines(ticker_list, period: str = '2y'):
    """主線 K 線入口：舊 kline_cache.py 已退役，直接走 MarketDataService。"""
    return _MARKET_DATA_SERVICE.get_smart_klines(ticker_list, period=period)
from param_storage import save_candidate_params
from fts_research_lab import ResearchLab
from fts_utils import now_str

try:
    from sklearn.gaussian_process import GaussianProcessRegressor
    from sklearn.gaussian_process.kernels import Matern, WhiteKernel, ConstantKernel
except ImportError:  # pragma: no cover
    GaussianProcessRegressor = None
    Matern = WhiteKernel = ConstantKernel = None

_LAB = ResearchLab()


def _candidate_space() -> dict[str, list[Any]]:
    return {
        'RSI_PERIOD': [10, 14, 18, 20],
        'MACD_FAST': [10, 12, 15],
        'MACD_SLOW': [20, 26, 30],
        'ADX_TREND_THRESHOLD': [18, 20, 25],
        'VOL_BREAKOUT_MULTIPLIER': [1.05, 1.1, 1.2],
        'TRIGGER_SCORE': [2, 3],
        'TP_BASE_PCT': [0.08, 0.10, 0.12, 0.15],
        'SL_MIN_PCT': [0.025, 0.03, 0.04],
        'SL_MAX_PCT': [0.06, 0.08, 0.10],
    }


def _vectorize(params: dict[str, Any], space: dict[str, list[Any]]) -> list[float]:
    vec = []
    for key, vals in space.items():
        v = params.get(key, vals[0])
        try:
            idx = vals.index(v)
        except ValueError:
            idx = 0
        except TypeError:
            idx = 0
        denom = max(len(vals) - 1, 1)
        vec.append(idx / denom)
    return vec


def _sample_random(space: dict[str, list[Any]]) -> dict[str, Any]:
    return {k: random.choice(v) for k, v in space.items()}


def _mutate_near(best: dict[str, Any], space: dict[str, list[Any]]) -> dict[str, Any]:
    out = dict(best)
    keys = list(space.keys())
    for key in random.sample(keys, k=max(1, len(keys) // 3)):
        vals = space[key]
        try:
            idx = vals.index(out.get(key, vals[0]))
        except ValueError:
            idx = 0
        except TypeError:
            idx = 0
        shift = random.choice([-1, 1])
        idx = max(0, min(len(vals) - 1, idx + shift))
        out[key] = vals[idx]
    return out


def _expected_improvement(mu: float, sigma: float, best: float, xi: float = 0.01) -> float:
    if sigma <= 1e-9:
        return 0.0
    z = (mu - best - xi) / sigma
    # Normal CDF/PDF without scipy
    cdf = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
    pdf = math.exp(-0.5 * z * z) / math.sqrt(2.0 * math.pi)
    return max(0.0, (mu - best - xi) * cdf + sigma * pdf)


def is_dominated(a: dict[str, float], b: dict[str, float]) -> bool:
    better_or_equal = (
        b.get('Test_EV', 0.0) >= a.get('Test_EV', 0.0)
        and b.get('Test_WinRate', 0.0) >= a.get('Test_WinRate', 0.0)
        and b.get('Test_TotalReturn', 0.0) >= a.get('Test_TotalReturn', 0.0)
    )
    strictly_better = (
        b.get('Test_EV', 0.0) > a.get('Test_EV', 0.0)
        or b.get('Test_WinRate', 0.0) > a.get('Test_WinRate', 0.0)
        or b.get('Test_TotalReturn', 0.0) > a.get('Test_TotalReturn', 0.0)
    )
    return better_or_equal and strictly_better


def get_pareto_frontier(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    frontier = []
    for row in results:
        if any(is_dominated(row, other) for other in results if other is not row):
            continue
        frontier.append(row)
    return frontier


def evaluate_params(params: dict[str, Any], histories: dict[str, Any], targets: list[str]) -> dict[str, Any]:
    metrics = []
    for ticker in targets:
        df = histories.get(ticker)
        if df is None or len(df) < 120:
            continue
        split_idx = max(80, int(len(df) * 0.7))
        test_df = df.iloc[split_idx:].copy()
        res = inspect_stock(ticker, preloaded_df=test_df, p=params)
        if not res:
            continue
        metrics.append({
            'ev': float(res.get('期望值', 0.0) or 0.0),
            'wr': float(res.get('系統勝率(%)', 0.0) or 0.0),
            'ret': float(res.get('累計報酬率(%)', 0.0) or 0.0),
            'kelly': float(res.get('Kelly建議倉位', 0.0) or 0.0),
        })
    if not metrics:
        return {'Params': params, 'Test_EV': 0.0, 'Test_WinRate': 0.0, 'Test_TotalReturn': 0.0, 'Kelly': 0.0, 'Composite': 0.0}
    out = {
        'Params': params,
        'Test_EV': round(float(np.mean([m['ev'] for m in metrics])), 4),
        'Test_WinRate': round(float(np.mean([m['wr'] for m in metrics])), 2),
        'Test_TotalReturn': round(float(np.mean([m['ret'] for m in metrics])), 2),
        'Kelly': round(float(np.mean([m['kelly'] for m in metrics])), 4),
    }
    out['Composite'] = round(out['Test_EV'] + out['Test_WinRate'] * 0.02 + out['Test_TotalReturn'] * 0.005 + out['Kelly'] * 10.0, 4)
    return out


def run_bayesian_optimization(n_iter: int = 30, split_ratio: float = 0.7, ticker_list=None):
    targets = list(ticker_list or TRAINING_POOL)
    histories = get_smart_klines(targets, period='2y')
    space = _candidate_space()
    results = []
    vectors: list[list[float]] = []
    scores: list[float] = []

    warmup = min(8, max(4, n_iter // 4))
    gp = None
    if GaussianProcessRegressor is not None:
        kernel = ConstantKernel(1.0) * Matern(length_scale=1.0, nu=1.5) + WhiteKernel(noise_level=1e-5)
        gp = GaussianProcessRegressor(kernel=kernel, alpha=1e-5, random_state=42, normalize_y=True)

    for i in range(int(n_iter)):
        if i < warmup or not results:
            candidate = _sample_random(space)
        else:
            best_params = max(results, key=lambda x: x.get('Composite', -1e18))['Params']
            proposals = [_mutate_near(best_params, space) for _ in range(20)] + [_sample_random(space) for _ in range(20)]
            if gp is not None and len(vectors) >= warmup:
                gp.fit(np.asarray(vectors), np.asarray(scores))
                scored = []
                best_score = max(scores) if scores else 0.0
                for p in proposals:
                    x = np.asarray([_vectorize(p, space)])
                    mu, sigma = gp.predict(x, return_std=True)
                    ei = _expected_improvement(float(mu[0]), float(sigma[0]), float(best_score))
                    scored.append((ei, p))
                candidate = max(scored, key=lambda x: x[0])[1]
            else:
                candidate = random.choice(proposals)
        params = deepcopy(PARAMS)
        params.update(candidate)
        evaluated = evaluate_params(params, histories, targets)
        results.append(evaluated)
        vectors.append(_vectorize(candidate, space))
        scores.append(float(evaluated.get('Composite', 0.0) or 0.0))

    if not results:
        return None
    frontier = get_pareto_frontier(results)
    best = sorted(frontier or results, key=lambda x: x.get('Composite', 0.0), reverse=True)[0]
    strategy_keys = set(_candidate_space().keys())
    saved = save_candidate_params(
        'strategy_signal::default',
        {k: best['Params'].get(k) for k in strategy_keys if k in best['Params']},
        metrics={k: v for k, v in best.items() if k != 'Params'},
        source_module='advanced_optimizer.py',
        note='strategy-signal research-only Bayesian/GP Pareto candidate; AI judge required',
    )
    try:
        from fts_candidate_ai_judge import judge_candidate_by_id
        best['AI_Judge'] = judge_candidate_by_id(saved.get('candidate_id'))
    except Exception as exc:
        best['AI_Judge'] = {'status': 'auto_judge_failed', 'error': repr(exc)}
    _LAB.write_json_artifact('optimizer_runs', f'pareto_gp_best_{now_str().replace(":","").replace("-","").replace(" ","_")}.json', {'best': best, 'frontier': frontier, 'iterations': n_iter})
    return best


if __name__ == '__main__':
    print(run_bayesian_optimization())
