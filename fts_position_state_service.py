# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

import pandas as pd

from fts_sector_service import SectorService
from fts_utils import safe_float


class PositionStateService:
    def __init__(self):
        self.sectors = SectorService()

    @staticmethod
    def read_active_positions_csv(path) -> pd.DataFrame:
        try:
            return pd.read_csv(path, encoding='utf-8-sig')
        except Exception:
            try:
                return pd.read_csv(path)
            except Exception:
                return pd.DataFrame()

    def current_portfolio_state(self, active_df: pd.DataFrame, total_nav: float) -> dict[str, Any]:
        state = {
            'total_alloc': 0.0,
            'sector_alloc': {},
            'sector_count': {},
            'direction_alloc': {'LONG': 0.0, 'SHORT': 0.0},
        }
        if active_df is None or active_df.empty or total_nav <= 0:
            return state
        for _, pos in active_df.iterrows():
            ticker = str(pos.get('Ticker SYMBOL', pos.get('Ticker', ''))).strip()
            invested = safe_float(pos.get('投入資金', pos.get('invested', 0.0)), 0.0)
            if invested <= 0:
                continue
            alloc = invested / total_nav
            direction = 'SHORT' if ('空' in str(pos.get('方向', pos.get('Direction', '')))) else 'LONG'
            sector = self.sectors.get_stock_sector(ticker)
            state['total_alloc'] += alloc
            state['sector_alloc'][sector] = state['sector_alloc'].get(sector, 0.0) + alloc
            state['sector_count'][sector] = state['sector_count'].get(sector, 0) + 1
            state['direction_alloc'][direction] = state['direction_alloc'].get(direction, 0.0) + alloc
        return state
