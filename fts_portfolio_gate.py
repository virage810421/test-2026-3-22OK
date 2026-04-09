# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_sector_service import SectorService
from fts_utils import safe_float


def passes_portfolio_gate(row: dict[str, Any], total_nav: float, portfolio_state: dict[str, Any], params: dict[str, Any] | None = None) -> tuple[bool, str]:
    params = params or {}
    if total_nav <= 0:
        return False, '總資產異常'
    direction = 'SHORT' if ('空' in str(row.get('Direction', ''))) else 'LONG'
    ticker = str(row.get('Ticker', row.get('Ticker SYMBOL', ''))).strip()
    sector = SectorService().get_stock_sector(ticker)
    requested_alloc = safe_float(row.get('Kelly_Pos', 0.0), 0.0)

    max_sector_positions = int(params.get('PORT_MAX_SECTOR_POSITIONS', 2))
    max_sector_alloc = float(params.get('PORT_MAX_SECTOR_ALLOC', 0.35))
    max_total_alloc = float(params.get('PORT_MAX_TOTAL_ALLOC', 0.60))
    max_direction_alloc = float(params.get('PORT_MAX_DIRECTION_ALLOC', 0.45))
    max_single_pos = float(params.get('PORT_MAX_SINGLE_POS', 0.12))
    min_position = float(params.get('PORT_MIN_POSITION', 0.01))

    if requested_alloc < min_position:
        return False, '倉位低於最小門檻'
    if requested_alloc > max_single_pos:
        return False, '單筆倉位超過上限'
    current_total = portfolio_state.get('total_alloc', 0.0)
    current_sector_alloc = portfolio_state.get('sector_alloc', {}).get(sector, 0.0)
    current_sector_count = portfolio_state.get('sector_count', {}).get(sector, 0)
    current_direction_alloc = portfolio_state.get('direction_alloc', {}).get(direction, 0.0)
    if current_sector_count >= max_sector_positions:
        return False, f'{sector} 產業持倉數已達上限'
    if current_total + requested_alloc > max_total_alloc:
        return False, '總配置上限不足'
    if current_sector_alloc + requested_alloc > max_sector_alloc:
        return False, f'{sector} 產業資金占比將超限'
    if current_direction_alloc + requested_alloc > max_direction_alloc:
        return False, f'{direction} 方向曝險將超限'
    return True, '通過組合閘門'
