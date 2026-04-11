# -*- coding: utf-8 -*-
from __future__ import annotations

from config import WATCH_LIST
from sector_classifier import get_stock_sector
from param_storage import save_candidate_params

USE_ADVANCED_BAYES = False

if USE_ADVANCED_BAYES:
    from advanced_optimizer import run_bayesian_optimization as run_engine
else:
    from optimizer import run_walk_forward_optimization as run_engine


def start_automated_training():
    sector_groups = {}
    for ticker in WATCH_LIST:
        sector = get_stock_sector(ticker) if 'get_stock_sector' in globals() else 'Unknown'
        sector_groups.setdefault(sector or 'Unknown', []).append(ticker)
    results = {}
    for sector, tickers in sector_groups.items():
        result = run_engine(n_iter=20, ticker_list=tickers) if USE_ADVANCED_BAYES else run_engine(iterations=30, ticker_list=tickers)
        if result and result.get('Params'):
            save_candidate_params(scope_name=f'sector::{sector}', best_params=result['Params'], metrics={k: v for k, v in result.items() if k != 'Params'}, source_module='auto_optimizer.py', note='sector candidate only')
            results[sector] = result
    return results


if __name__ == '__main__':
    print(start_automated_training())
