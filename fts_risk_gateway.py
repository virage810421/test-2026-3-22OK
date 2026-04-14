# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 2 files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_risk_gateway.py
# ==============================================================================
import json
from dataclasses import dataclass
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log, safe_float, safe_int


@dataclass
class RiskCheckResult:
    approved: bool
    reason: str = ''
    projected_position: int = 0
    order_value: float = 0.0


class RiskGateway:
    MODULE_VERSION = 'v83_risk_gateway'

    def __init__(
        self,
        max_single_order_value: float | None = None,
        max_symbol_abs_position: int = 5000,
        allow_short: bool = False,
        block_duplicate_signal_id: bool = True,
        min_cash_buffer: float = 50_000,
    ):
        self.runtime_path = PATHS.runtime_dir / 'risk_gateway_service.json'
        self.max_single_order_value = float(max_single_order_value or CONFIG.max_order_notional)
        self.max_symbol_abs_position = int(max_symbol_abs_position)
        self.allow_short = bool(allow_short)
        self.block_duplicate_signal_id = bool(block_duplicate_signal_id)
        self.min_cash_buffer = float(min_cash_buffer)
        self.used_signal_ids: set[str] = set()

    @staticmethod
    def _direction(order: dict[str, Any]) -> str:
        side = str(order.get('side', order.get('Action', 'BUY'))).upper().strip()
        if 'SHORT' in side:
            return 'SHORT'
        if side in {'SELL', 'EXIT'}:
            return 'SELL'
        if 'COVER' in side:
            return 'COVER'
        return 'BUY'

    def validate(self, order: dict[str, Any], ref_price: float, cash: float, current_positions: dict[str, int] | None = None) -> RiskCheckResult:
        current_positions = current_positions or {}
        symbol = str(order.get('symbol', order.get('Ticker', order.get('ticker', '')))).strip()
        qty = safe_int(order.get('quantity', order.get('qty', order.get('Target_Qty', 0))), 0)
        signal_id = str(order.get('signal_id', order.get('Client_Order_ID', ''))).strip()
        side = self._direction(order)

        if ref_price <= 0:
            return RiskCheckResult(False, '參考價格無效')
        if qty <= 0:
            return RiskCheckResult(False, '數量必須 > 0')
        if self.block_duplicate_signal_id and signal_id and signal_id in self.used_signal_ids:
            return RiskCheckResult(False, f'重複 signal_id：{signal_id}')

        order_value = qty * ref_price
        if order_value > self.max_single_order_value:
            return RiskCheckResult(False, f'單筆委託金額超限：{order_value:,.0f}', order_value=order_value)

        current_pos = safe_int(current_positions.get(symbol, 0), 0)
        projected = current_pos
        if side == 'BUY':
            projected = current_pos + qty
        elif side in {'SELL', 'SHORT'}:
            projected = current_pos - qty
        elif side == 'COVER':
            projected = current_pos + qty

        if abs(projected) > self.max_symbol_abs_position:
            return RiskCheckResult(False, f'單檔絕對持倉超限：目前 {current_pos}，送單後 {projected}，上限 {self.max_symbol_abs_position}', projected_position=projected, order_value=order_value)
        if side == 'SHORT' and not self.allow_short:
            return RiskCheckResult(False, '系統未開啟放空權限', projected_position=projected, order_value=order_value)
        if side in {'BUY', 'COVER'} and float(cash) - order_value < self.min_cash_buffer:
            return RiskCheckResult(False, '下單後現金緩衝不足', projected_position=projected, order_value=order_value)
        return RiskCheckResult(True, '', projected_position=projected, order_value=order_value)

    def register_signal(self, signal_id: str) -> None:
        if signal_id:
            self.used_signal_ids.add(signal_id)

    def build_summary(self) -> tuple[Any, dict[str, Any]]:
        sample = self.validate({'symbol': '2330.TW', 'quantity': 1000, 'side': 'BUY', 'signal_id': 'probe-001'}, ref_price=100.0, cash=500000.0, current_positions={})
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'sample_approved': sample.approved,
            'limits': {
                'max_single_order_value': self.max_single_order_value,
                'max_symbol_abs_position': self.max_symbol_abs_position,
                'allow_short': self.allow_short,
                'min_cash_buffer': self.min_cash_buffer,
            },
            'status': 'wave2_risk_gateway_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🚧 risk gateway ready: {self.runtime_path}')
        return self.runtime_path, payload


# ==============================================================================
# Merged from: fts_portfolio_gate.py
# ==============================================================================
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
