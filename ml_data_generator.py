# -*- coding: utf-8 -*-
"""Level-1 compatibility bridge for ml_data_generator.

舊入口保留，資料生成主線改由 fts_training_data_builder 提供。
"""
from __future__ import annotations

from fts_training_data_builder import generate_ml_dataset as _generate_ml_dataset
from fts_training_data_builder import get_dynamic_watchlist as _get_dynamic_watchlist

BRIDGE_LEVEL = 'level_1'
BRIDGE_TARGET = 'fts_training_data_builder.generate_ml_dataset'
LEGACY_SOURCE = 'advanced_chart(1).zip::ml_data_generator.py'


def get_dynamic_watchlist():
    return _get_dynamic_watchlist()


def _signal_flags(setup_tag: str):
    tag = str(setup_tag).strip()
    is_short = ('空' in tag) or ('SHORT' in tag.upper())
    is_long = ('多' in tag) or ('LONG' in tag.upper())
    return is_long, is_short


def generate_ml_dataset(tickers=None):
    return _generate_ml_dataset(tickers=tickers)


if __name__ == '__main__':
    generate_ml_dataset(get_dynamic_watchlist())
