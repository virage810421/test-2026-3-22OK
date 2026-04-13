# -*- coding: utf-8 -*-
"""Legacy facade for ml_data_generator."""
from __future__ import annotations

import warnings

from fts_training_data_builder import generate_ml_dataset as _generate_ml_dataset
from fts_training_data_builder import get_dynamic_watchlist as _get_dynamic_watchlist

LEGACY_FACADE = True
SERVICE_ENTRYPOINT = 'fts_training_data_builder.generate_ml_dataset'


def get_dynamic_watchlist():
    warnings.warn('ml_data_generator.py 已退役為 legacy facade；新主線請改用 fts_training_data_builder。', DeprecationWarning, stacklevel=2)
    return _get_dynamic_watchlist()


def generate_ml_dataset(tickers=None):
    warnings.warn('ml_data_generator.py 已退役為 legacy facade；新主線請改用 fts_training_data_builder。', DeprecationWarning, stacklevel=2)
    return _generate_ml_dataset(tickers=tickers)


if __name__ == '__main__':
    generate_ml_dataset(get_dynamic_watchlist())
