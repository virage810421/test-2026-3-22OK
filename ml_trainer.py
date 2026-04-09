# -*- coding: utf-8 -*-
"""Compatibility wrapper.
`ml_trainer.py` 仍保留舊門牌，避免你原本用這支啟動訓練時斷線；
但主線已改走 `fts_trainer_backend.py`。

這就是「保留舊門牌，但新大樓已經搬走」：
- 舊檔名還在，舊指令還能跑
- 真正負責訓練的邏輯，已搬到新 service / backend
"""
from fts_trainer_backend import *  # noqa: F401,F403

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    train_models()
