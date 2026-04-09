# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import defaultdict
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, pct, normalize_key, write_json


def _index_by(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    out = {}
    for row in rows or []:
        out[normalize_key(row.get(key))] = row
    return out


def _to_position_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in rows or []:
        out[normalize_key(row.get('ticker'))] = row
    return out


class ReconciliationEngine:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'reconciliation_engine.json'

    def reconcile(
        self,
        local_orders: list[dict[str, Any]],
        broker_orders: list[dict[str, Any]],
        local_fills: list[dict[str, Any]],
        broker_fills: list[dict[str, Any]],
        local_positions: list[dict[str, Any]],
        broker_positions: list[dict[str, Any]],
        local_cash: float,
        broker_cash: float,
    ) -> tuple[Any, dict[str, Any]]:
        local_order_map = _index_by(local_orders, 'order_id')
        broker_order_map = _index_by(broker_orders, 'order_id')
        local_fill_map = _index_by(local_fills, 'fill_id')
        broker_fill_map = _index_by(broker_fills, 'fill_id')
        local_pos_map = _to_position_map(local_positions)
        broker_pos_map = _to_position_map(broker_positions)

        order_mismatches = []
        for order_id, local in local_order_map.items():
            broker = broker_order_map.get(order_id)
            if broker is None:
                order_mismatches.append({'order_id': order_id, 'type': 'missing_at_broker', 'local': local})
                continue
            if normalize_key(local.get('status')) != normalize_key(broker.get('status')):
                order_mismatches.append({'order_id': order_id, 'type': 'status_mismatch', 'local_status': local.get('status'), 'broker_status': broker.get('status')})
            if int(local.get('qty', 0) or 0) != int(broker.get('qty', 0) or 0):
                order_mismatches.append({'order_id': order_id, 'type': 'qty_mismatch', 'local_qty': local.get('qty'), 'broker_qty': broker.get('qty')})
        orphan_broker_orders = [row for oid, row in broker_order_map.items() if oid not in local_order_map]

        fill_mismatches = []
        for fill_id, local in local_fill_map.items():
            broker = broker_fill_map.get(fill_id)
            if broker is None:
                fill_mismatches.append({'fill_id': fill_id, 'type': 'missing_at_broker', 'local': local})
                continue
            if float(local.get('fill_qty', 0) or 0) != float(broker.get('fill_qty', 0) or 0):
                fill_mismatches.append({'fill_id': fill_id, 'type': 'qty_mismatch', 'local_qty': local.get('fill_qty'), 'broker_qty': broker.get('fill_qty')})
            if round(float(local.get('fill_price', 0) or 0), 4) != round(float(broker.get('fill_price', 0) or 0), 4):
                fill_mismatches.append({'fill_id': fill_id, 'type': 'price_mismatch', 'local_price': local.get('fill_price'), 'broker_price': broker.get('fill_price')})
        orphan_broker_fills = [row for fid, row in broker_fill_map.items() if fid not in local_fill_map]

        position_mismatches = []
        all_tickers = sorted(set(local_pos_map) | set(broker_pos_map))
        for ticker in all_tickers:
            local = local_pos_map.get(ticker)
            broker = broker_pos_map.get(ticker)
            if local is None or broker is None:
                position_mismatches.append({'ticker': ticker, 'type': 'position_missing_on_one_side', 'local': local, 'broker': broker})
                continue
            if int(local.get('qty', 0) or 0) != int(broker.get('qty', 0) or 0):
                position_mismatches.append({'ticker': ticker, 'type': 'qty_mismatch', 'local_qty': local.get('qty'), 'broker_qty': broker.get('qty')})
            if round(float(local.get('avg_cost', 0) or 0), 4) != round(float(broker.get('avg_cost', 0) or 0), 4):
                position_mismatches.append({'ticker': ticker, 'type': 'avg_cost_mismatch', 'local_avg_cost': local.get('avg_cost'), 'broker_avg_cost': broker.get('avg_cost')})

        cash_diff = round(float(local_cash or 0) - float(broker_cash or 0), 4)
        cash_check = {'local_cash': float(local_cash or 0), 'broker_cash': float(broker_cash or 0), 'cash_diff': cash_diff, 'matched': abs(cash_diff) < 0.01}

        status_counts = defaultdict(int)
        for row in broker_orders:
            status_counts[normalize_key(row.get('status'))] += 1

        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'summary': {
                'local_orders': len(local_orders or []),
                'broker_orders': len(broker_orders or []),
                'local_fills': len(local_fills or []),
                'broker_fills': len(broker_fills or []),
                'local_positions': len(local_positions or []),
                'broker_positions': len(broker_positions or []),
                'order_mismatch_count': len(order_mismatches),
                'fill_mismatch_count': len(fill_mismatches),
                'position_mismatch_count': len(position_mismatches),
                'orphan_broker_orders': len(orphan_broker_orders),
                'orphan_broker_fills': len(orphan_broker_fills),
                'cash_matched': cash_check['matched'],
                'all_green': all([
                    len(order_mismatches) == 0,
                    len(fill_mismatches) == 0,
                    len(position_mismatches) == 0,
                    len(orphan_broker_orders) == 0,
                    len(orphan_broker_fills) == 0,
                    cash_check['matched'],
                ]),
                'broker_status_mix': dict(status_counts),
            },
            'cash_check': cash_check,
            'order_mismatches': order_mismatches[:200],
            'fill_mismatches': fill_mismatches[:200],
            'position_mismatches': position_mismatches[:200],
            'orphan_broker_orders': orphan_broker_orders[:100],
            'orphan_broker_fills': orphan_broker_fills[:100],
            'status': 'reconciliation_green' if len(order_mismatches) == len(fill_mismatches) == len(position_mismatches) == 0 and cash_check['matched'] else 'reconciliation_break',
        }
        write_json(self.path, payload)
        log(f"🧮 已輸出 reconciliation engine：{self.path} | all_green={payload['summary']['all_green']}")
        return self.path, payload
