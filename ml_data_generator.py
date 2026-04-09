# -*- coding: utf-8 -*-
"""Compatibility wrapper.
v83 收編後，舊門牌 `ml_data_generator.py` 仍可執行；
但真正的訓練資料 builder 已搬到 `fts_training_data_builder.py`。
"""
from fts_training_data_builder import *  # noqa: F401,F403

if __name__ == "__main__":
    tickers = get_dynamic_watchlist()
    generate_ml_dataset(tickers)
