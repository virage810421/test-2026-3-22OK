# -*- coding: utf-8 -*-
from __future__ import annotations

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
