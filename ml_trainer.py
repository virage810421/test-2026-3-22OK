# -*- coding: utf-8 -*-
"""Compatibility wrapper.
v83 起，ml_trainer.py 保留舊入口，但實際訓練邏輯已收編到 fts_trainer_backend.py，
供正式交易主控版直接呼叫。
"""
from fts_trainer_backend import *  # noqa: F401,F403

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    train_models()
