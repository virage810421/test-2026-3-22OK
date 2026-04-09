# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import defaultdict
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, normalize_key, write_json
from fts_utils import log


def _first_non_empty(row: dict[str, Any], keys: list[str]) -> str:
    for k in keys:
        v = normalize_key(row.get(k))
        if v:
            return v
    return ''


def _index_by_multi(rows: list[dict[str, Any]], keys: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        key = _first_non_empty(row, keys)
        if not key:
            continue
        current = out.get(key)
        if current is None:
            out[key] = row
            continue
        # prefer latest updated row if possible
        curr_ts = str(current.get('updated_at') or current.get('time') or '')
        new_ts = str(row.get('updated_at') or row.get('time') or '')
        if new_ts >= curr_ts:
            out[key] = row
    return out


def _to_position_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {}
    for row in rows or []:
        ticker = normalize_key(row.get('ticker') or row.get('股票代號'))
        if ticker:
            out[ticker] = row
    return out


def _float(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def _int(v: Any) -> int:
    try:
        return int(float(v or 0))
    except Exception:
        return 0


def _approx(a: float, b: float, tol: float = 1e-4) -> bool:
    return abs(a - b) <= tol


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
        local_order_map = _index_by_multi(local_orders, ['order_id', 'client_order_id', 'broker_order_id', '委託單號'])
        broker_order_map = _index_by_multi(broker_orders, ['order_id', 'client_order_id', 'broker_order_id', '委託單號'])
        local_fill_map = _index_by_multi(local_fills, ['fill_id', 'trade_id', '成交編號'])
        broker_fill_map = _index_by_multi(broker_fills, ['fill_id', 'trade_id', '成交編號'])
        local_pos_map = _to_position_map(local_positions)
        broker_pos_map = _to_position_map(broker_positions)

        order_mismatches = []
        for order_id, local in local_order_map.items():
            broker = broker_order_map.get(order_id)
            if broker is None:
                order_mismatches.append({'order_id': order_id, 'type': 'missing_at_broker', 'local': local})
                continue
            if normalize_key(local.get('status') or local.get('委託狀態')) != normalize_key(broker.get('status') or broker.get('委託狀態')):
                order_mismatches.append({'order_id': order_id, 'type': 'status_mismatch', 'local_status': local.get('status') or local.get('委託狀態'), 'broker_status': broker.get('status') or broker.get('委託狀態')})
            if _int(local.get('qty') or local.get('委託股數')) != _int(broker.get('qty') or broker.get('委託股數')):
                order_mismatches.append({'order_id': order_id, 'type': 'qty_mismatch', 'local_qty': local.get('qty') or local.get('委託股數'), 'broker_qty': broker.get('qty') or broker.get('委託股數')})
        orphan_broker_orders = [row for oid, row in broker_order_map.items() if oid not in local_order_map]

        fill_mismatches = []
        for fill_id, local in local_fill_map.items():
            broker = broker_fill_map.get(fill_id)
            if broker is None:
                fill_mismatches.append({'fill_id': fill_id, 'type': 'missing_at_broker', 'local': local})
                continue
            if _float(local.get('fill_qty') or local.get('成交股數')) != _float(broker.get('fill_qty') or broker.get('成交股數')):
                fill_mismatches.append({'fill_id': fill_id, 'type': 'qty_mismatch', 'local_qty': local.get('fill_qty') or local.get('成交股數'), 'broker_qty': broker.get('fill_qty') or broker.get('成交股數')})
            if not _approx(_float(local.get('fill_price') or local.get('成交價格')), _float(broker.get('fill_price') or broker.get('成交價格')), 1e-3):
                fill_mismatches.append({'fill_id': fill_id, 'type': 'price_mismatch', 'local_price': local.get('fill_price') or local.get('成交價格'), 'broker_price': broker.get('fill_price') or broker.get('成交價格')})
        orphan_broker_fills = [row for fid, row in broker_fill_map.items() if fid not in local_fill_map]

        position_mismatches = []
        corporate_action_suspects = []
        all_tickers = sorted(set(local_pos_map) | set(broker_pos_map))
        for ticker in all_tickers:
            local = local_pos_map.get(ticker)
            broker = broker_pos_map.get(ticker)
            if local is None or broker is None:
                position_mismatches.append({'ticker': ticker, 'type': 'position_missing_on_one_side', 'local': local, 'broker': broker})
                continue
            lqty = _int(local.get('qty') or local.get('持股數量'))
            bqty = _int(broker.get('qty') or broker.get('持股數量'))
            lcost = _float(local.get('avg_cost') or local.get('庫存均價'))
            bcost = _float(broker.get('avg_cost') or broker.get('庫存均價'))
            if lqty != bqty:
                position_mismatches.append({'ticker': ticker, 'type': 'qty_mismatch', 'local_qty': lqty, 'broker_qty': bqty})
            if not _approx(lcost, bcost, 1e-3):
                position_mismatches.append({'ticker': ticker, 'type': 'avg_cost_mismatch', 'local_avg_cost': lcost, 'broker_avg_cost': bcost})
            if lqty > 0 and bqty > 0:
                qty_ratio = max(lqty, bqty) / max(min(lqty, bqty), 1)
                cost_ratio = max(lcost, bcost) / max(min(lcost, bcost), 1e-8) if lcost > 0 and bcost > 0 else 0.0
                if qty_ratio in (2.0, 3.0) or abs(qty_ratio - 2.0) < 0.05 or abs(qty_ratio - 0.5) < 0.05:
                    if cost_ratio > 1.8 or cost_ratio < 0.55:
                        corporate_action_suspects.append({'ticker': ticker, 'local_qty': lqty, 'broker_qty': bqty, 'local_avg_cost': lcost, 'broker_avg_cost': bcost, 'suspect': 'possible_split_or_reverse_split'})

        cash_diff = round(_float(local_cash) - _float(broker_cash), 4)
        cash_check = {'local_cash': _float(local_cash), 'broker_cash': _float(broker_cash), 'cash_diff': cash_diff, 'matched': abs(cash_diff) < 0.01}

        status_counts = defaultdict(int)
        for row in broker_orders or []:
            status_counts[normalize_key(row.get('status') or row.get('委託狀態'))] += 1

        repair_actions = []
        if orphan_broker_orders or orphan_broker_fills:
            repair_actions.append('fetch_today_broker_snapshots_and_merge_orphans')
        if corporate_action_suspects:
            repair_actions.append('apply_corporate_action_position_rebuild')
        if position_mismatches:
            repair_actions.append('rebuild_position_snapshot_from_fills')
        if not cash_check['matched']:
            repair_actions.append('replay_fees_taxes_and_cash_ledger')

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
                'corporate_action_suspect_count': len(corporate_action_suspects),
                'orphan_broker_orders': len(orphan_broker_orders),
                'orphan_broker_fills': len(orphan_broker_fills),
                'cash_matched': cash_check['matched'],
                'all_green': all([
                    len(order_mismatches) == 0,
                    len(fill_mismatches) == 0,
                    len(position_mismatches) == 0,
                    len(orphan_broker_orders) == 0,
                    len(orphan_broker_fills) == 0,
                    len(corporate_action_suspects) == 0,
                    cash_check['matched'],
                ]),
                'broker_status_mix': dict(status_counts),
            },
            'cash_check': cash_check,
            'order_mismatches': order_mismatches[:200],
            'fill_mismatches': fill_mismatches[:200],
            'position_mismatches': position_mismatches[:200],
            'corporate_action_suspects': corporate_action_suspects[:100],
            'orphan_broker_orders': orphan_broker_orders[:100],
            'orphan_broker_fills': orphan_broker_fills[:100],
            'repair_actions': repair_actions,
            'status': 'reconciliation_green' if len(repair_actions) == 0 else 'reconciliation_repair_needed',
        }
        write_json(self.path, payload)
        log(f"🧮 已輸出 reconciliation engine：{self.path} | all_green={payload['summary']['all_green']}")
        return self.path, payload
