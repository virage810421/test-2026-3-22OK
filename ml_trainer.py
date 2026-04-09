# -*- coding: utf-8 -*-
from __future__ import annotations

from fts_trainer_backend import *  # type: ignore

if __name__ == '__main__':
    try:
        train_models()  # type: ignore
    except NameError:
        print('⚠️ fts_trainer_backend 未提供 train_models()，請檢查後端。')
