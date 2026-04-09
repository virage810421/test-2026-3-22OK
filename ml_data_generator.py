# -*- coding: utf-8 -*-
from __future__ import annotations

from fts_training_data_builder import generate_ml_dataset, get_dynamic_watchlist

if __name__ == '__main__':
    generate_ml_dataset(get_dynamic_watchlist())
