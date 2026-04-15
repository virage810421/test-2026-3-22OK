# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 4 files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_reconciliation_engine.py
# ==============================================================================
from collections import defaultdict
from typing import Any

from fts_prelive_runtime import PATHS, now_str, normalize_key, write_json
from fts_repair_workflow_engine import RepairWorkflowEngine


def _lane(row: dict[str, Any]) -> str:
    return normalize_key(row.get('direction_bucket') or row.get('approved_pool_type') or row.get('lane') or 'UNKNOWN') or 'UNKNOWN'


def _key(row: dict[str, Any]) -> str:
    return str(row.get('order_id') or row.get('client_order_id') or row.get('broker_order_id') or '').strip()


class ReconciliationEngine:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'reconciliation_engine.json'

    @staticmethod
    def _position_key(row: dict[str, Any]) -> str:
        return normalize_key(row.get('ticker') or row.get('Ticker') or row.get('Ticker SYMBOL') or row.get('symbol') or '')

    @staticmethod
    def _shares(row: dict[str, Any]) -> int:
        for key in ('shares', 'qty', 'quantity', 'position_qty', '進場股數', '持股數'):
            value = row.get(key)
            if value in (None, ''):
                continue
            try:
                return int(float(value))
            except Exception:
                continue
        return 0

    def reconcile(self, local_orders, broker_orders, local_fills, broker_fills, local_positions, broker_positions, local_cash, broker_cash):
        local_map = {_key(r): r for r in (local_orders or []) if _key(r)}
        broker_map = {_key(r): r for r in (broker_orders or []) if _key(r)}
        local_fill_map = {_key(r): r for r in (local_fills or []) if _key(r)}
        broker_fill_map = {_key(r): r for r in (broker_fills or []) if _key(r)}
        local_pos_map = {self._position_key(r): r for r in (local_positions or []) if self._position_key(r)}
        broker_pos_map = {self._position_key(r): r for r in (broker_positions or []) if self._position_key(r)}
        issues = []
        lane_issue_breakdown = defaultdict(int)

        def add_issue(issue_type: str, row: dict[str, Any] | None = None, **extra: Any) -> None:
            lane = _lane(row or {})
            issue = {'type': issue_type, 'lane': lane}
            if row is not None:
                oid = _key(row)
                if oid:
                    issue['order_id'] = oid
            issue.update(extra)
            issues.append(issue)
            lane_issue_breakdown[lane] += 1

        for oid, row in local_map.items():
            if oid not in broker_map:
                add_issue('missing_at_broker', row)
                continue
            broker_row = broker_map[oid]
            if normalize_key(str(row.get('status', ''))) != normalize_key(str(broker_row.get('status', ''))):
                add_issue('status_mismatch', row, local_status=row.get('status'), broker_status=broker_row.get('status'))

        for oid, row in broker_map.items():
            if oid not in local_map:
                add_issue('orphan_broker_order', row)

        for oid, row in local_fill_map.items():
            if oid not in broker_fill_map:
                add_issue('missing_fill_at_broker', row)
        for oid, row in broker_fill_map.items():
            if oid not in local_fill_map:
                add_issue('orphan_broker_fill', row)

        position_issues = []
        for key, row in local_pos_map.items():
            if key not in broker_pos_map:
                position_issues.append({'ticker': key, 'type': 'missing_position_at_broker', 'local_shares': self._shares(row), 'broker_shares': 0})
                add_issue('missing_position_at_broker', row, ticker=key, local_shares=self._shares(row), broker_shares=0)
                continue
            local_shares = self._shares(row)
            broker_shares = self._shares(broker_pos_map[key])
            if local_shares != broker_shares:
                position_issues.append({'ticker': key, 'type': 'position_qty_mismatch', 'local_shares': local_shares, 'broker_shares': broker_shares})
                add_issue('position_qty_mismatch', row, ticker=key, local_shares=local_shares, broker_shares=broker_shares)
        for key, row in broker_pos_map.items():
            if key not in local_pos_map:
                position_issues.append({'ticker': key, 'type': 'orphan_broker_position', 'local_shares': 0, 'broker_shares': self._shares(row)})
                add_issue('orphan_broker_position', row, ticker=key, local_shares=0, broker_shares=self._shares(row))

        cash_diff = float((broker_cash or 0) - (local_cash or 0))
        cash_ok = abs(cash_diff) <= 1e-6
        order_ok = not any(x['type'] in {'missing_at_broker', 'status_mismatch', 'orphan_broker_order'} for x in issues)
        fill_ok = not any(x['type'] in {'missing_fill_at_broker', 'orphan_broker_fill'} for x in issues)
        position_ok = len(position_issues) == 0
        all_green = bool(order_ok and fill_ok and position_ok and cash_ok)
        status = 'reconciled' if all_green else ('reconciliation_partial' if (local_map or broker_map or local_pos_map or broker_pos_map or local_fill_map or broker_fill_map) else 'reconciliation_waiting_for_inputs')
        payload = {
            'generated_at': now_str(),
            'status': status,
            'all_green': all_green,
            'summary': {
                'all_green': all_green,
                'order_ok': order_ok,
                'fill_ok': fill_ok,
                'position_ok': position_ok,
                'cash_ok': cash_ok,
            },
            'order_issue_count': sum(1 for x in issues if x['type'] in {'missing_at_broker', 'status_mismatch', 'orphan_broker_order'}),
            'fill_issue_count': sum(1 for x in issues if x['type'] in {'missing_fill_at_broker', 'orphan_broker_fill'}),
            'position_issue_count': len(position_issues),
            'issue_count': len(issues),
            'issues': issues,
            'position_issues': position_issues,
            'lane_issue_breakdown': dict(lane_issue_breakdown),
            'cash_local': float(local_cash or 0),
            'cash_broker': float(broker_cash or 0),
            'cash_diff': cash_diff,
            'directional_order_mix': dict((lane, c) for lane, c in lane_issue_breakdown.items()),
            'directional_fill_mix': {},
        }
        repair_path, repair_payload = RepairWorkflowEngine().execute(payload)
        payload['repair_workflow'] = {'path': repair_path, 'payload': repair_payload}
        payload['directional_repair_actions'] = repair_payload.get('executed_actions', [])
        write_json(self.path, payload)
        return str(self.path), payload


# ==============================================================================
# Merged from: fts_recovery_engine.py
# ==============================================================================
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, write_json


class RecoveryEngine:
    """Create crash-safe snapshots and rebuild a recovery plan before broker go-live."""

    def __init__(self):
        self.snapshot_path = PATHS.state_dir / 'engine_state.json'
        self.plan_path = PATHS.runtime_dir / 'recovery_plan.json'

    def create_snapshot(
        self,
        cash: float,
        positions: list[dict[str, Any]],
        open_orders: list[dict[str, Any]],
        recent_fills: list[dict[str, Any]],
        kill_switch_state: dict[str, Any] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        payload = {
            'saved_at': now_str(),
            'system_name': CONFIG.system_name,
            'cash': float(cash or 0),
            'positions': positions or [],
            'open_orders': open_orders or [],
            'recent_fills': recent_fills or [],
            'kill_switch_state': kill_switch_state or {},
            'meta': meta or {},
        }
        write_json(self.snapshot_path, payload)
        log(f'💾 已輸出 recovery snapshot：{self.snapshot_path}')
        return self.snapshot_path, payload

    def load_snapshot(self) -> dict[str, Any] | None:
        return load_json(self.snapshot_path, None)

    def build_recovery_plan(
        self,
        broker_snapshot: dict[str, Any] | None = None,
        retry_queue_summary: dict[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        state = self.load_snapshot() or {}
        broker_snapshot = broker_snapshot or {}
        retry_queue_summary = retry_queue_summary or {}
        actions = []
        failures = []
        if not state:
            failures.append('missing_local_snapshot')
        else:
            actions.extend([
                'restore_cash_from_snapshot',
                'restore_positions_from_snapshot',
                'rebuild_open_order_state_machine',
                'replay_recent_fills_for_pnl_consistency',
                'restore_kill_switch_state',
            ])
        if broker_snapshot:
            actions.extend([
                'fetch_live_open_orders_from_broker',
                'fetch_today_fills_from_broker',
                'reconcile_snapshot_vs_broker',
                'repair_orphan_orders_if_any',
            ])
        if retry_queue_summary.get('total', 0) > 0:
            actions.append('drain_retry_queue_before_new_orders')
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'snapshot_found': bool(state),
            'broker_snapshot_found': bool(broker_snapshot),
            'retry_queue_total': int(retry_queue_summary.get('total', 0) or 0),
            'actions': actions,
            'failures': failures,
            'ready_to_recover': len(failures) == 0,
            'status': 'recovery_ready' if len(failures) == 0 else 'recovery_blocked',
        }
        write_json(self.plan_path, payload)
        log(f'♻️ 已輸出 recovery plan：{self.plan_path}')
        return self.plan_path, payload


# ==============================================================================
# Merged from: fts_retry_queue.py
# ==============================================================================
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class RetryQueueManager:
    def __init__(self):
        self.path = PATHS.runtime_dir / "retry_queue.json"

    def _load(self):
        if not self.path.exists():
            return {"generated_at": now_str(), "items": []}
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, payload):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def add_failed_tasks(self, failed_tasks):
        if not getattr(CONFIG, "enable_retry_queue", False):
            return []
        payload = self._load()
        items = payload.get("items", [])
        for task in failed_tasks:
            key = f"{task.get('stage')}::{task.get('name')}::{task.get('script')}"
            existing = next((x for x in items if x.get("key") == key), None)
            if existing:
                existing["last_failed_at"] = now_str()
                existing["attempts"] = int(existing.get("attempts", 0)) + 1
                existing["status"] = "pending_retry"
            else:
                row = {
                    "key": key,
                    "stage": task.get("stage"),
                    "name": task.get("name"),
                    "script": task.get("script"),
                    "required": task.get("required", False),
                    "attempts": 1,
                    "first_failed_at": now_str(),
                    "last_failed_at": now_str(),
                    "status": "pending_retry",
                }
                items.append(row)
        payload["generated_at"] = now_str()
        payload["items"] = items
        self._save(payload)
        log(f"🧯 已更新 retry queue：{self.path} | total={len(items)}")
        return items

    def summarize(self):
        payload = self._load()
        items = payload.get("items", [])
        required_items = [x for x in items if x.get("required")]
        optional_items = [x for x in items if not x.get("required")]
        return {
            "total": len(items),
            "required": len(required_items),
            "optional": len(optional_items),
            "items": items,
        }

    def list_retryable_items(self):
        summary = self.summarize()
        max_attempts = getattr(CONFIG, "max_retry_attempts", 3)
        retryable = [x for x in summary["items"] if int(x.get("attempts", 0)) < max_attempts and x.get("status") == "pending_retry"]
        return retryable

    def mark_success(self, key: str):
        payload = self._load()
        changed = False
        for item in payload.get("items", []):
            if item.get("key") == key:
                item["status"] = "resolved"
                item["resolved_at"] = now_str()
                changed = True
        if changed:
            payload["generated_at"] = now_str()
            self._save(payload)

    def validate_required_queue(self):
        summary = self.summarize()
        pending_required = [x for x in summary["items"] if x.get("required") and x.get("status") == "pending_retry"]
        if getattr(CONFIG, "fail_on_retry_queue_required_items", True) and pending_required:
            raise RuntimeError(f"retry queue 內仍有必要任務待補跑: {len(pending_required)} 筆")
        return summary


# ==============================================================================
# Merged from: fts_execution.py
# ==============================================================================
from fts_models import Order, OrderSide, OrderStatus, TradeSignal
from fts_utils import now_str, round_price
from fts_config import CONFIG
import uuid

class ReconciliationService:
    def summarize(self, orders, fills):
        return {"orders_count": len(orders), "fills_count": len(fills), "filled_order_ids": sorted(list({f.order_id for f in fills}))}

class PositionMonitor:
    def generate_exit_signals(self, broker):
        signals = []
        positions = broker.get_positions()
        for ticker, pos in positions.items():
            last_px = broker.last_prices.get(ticker, pos.avg_cost)
            trailing_stop = round_price(max(pos.stop_loss_price, pos.highest_price * (1 - CONFIG.trailing_stop_pct)))
            bars_held = max(0, CONFIG.current_bar_index - pos.entry_bar)

            if CONFIG.enable_bracket_exit:
                if not pos.partial_tp_done and pos.take_profit_price > 0 and last_px >= pos.take_profit_price:
                    sell_qty = max(CONFIG.lot_size, int(pos.qty * CONFIG.partial_take_profit_ratio) // CONFIG.lot_size * CONFIG.lot_size)
                    sell_qty = min(sell_qty, pos.qty)
                    signals.append(TradeSignal(
                        ticker=ticker, action="SELL", reference_price=last_px, target_qty=sell_qty,
                        score=99, ai_confidence=1.0, strategy_name="PartialTakeProfitExit",
                        reason="觸發分批停利", regime="Exit"
                    ))
                    pos.partial_tp_done = True
                    if CONFIG.break_even_after_partial_tp:
                        pos.stop_loss_price = max(pos.stop_loss_price, pos.avg_cost)
                        pos.lifecycle_note = "分批停利後保本停損"
                elif last_px <= trailing_stop:
                    signals.append(TradeSignal(
                        ticker=ticker, action="SELL", reference_price=last_px, target_qty=pos.qty,
                        score=99, ai_confidence=1.0, strategy_name="TrailingStopExit",
                        reason="觸發移動停損", regime="Exit"
                    ))
                    pos.cooldown_until = CONFIG.current_bar_index + CONFIG.position_cooldown_bars
                    pos.lifecycle_note = "移動停損出場"
                elif bars_held >= CONFIG.max_holding_bars:
                    signals.append(TradeSignal(
                        ticker=ticker, action="SELL", reference_price=last_px, target_qty=pos.qty,
                        score=99, ai_confidence=1.0, strategy_name="MaxHoldingExit",
                        reason="超過最大持有bar", regime="Exit"
                    ))
                    pos.cooldown_until = CONFIG.current_bar_index + CONFIG.position_cooldown_bars
                    pos.lifecycle_note = "時間出場"
        return signals

class ExecutionEngine:
    def __init__(self, broker, logger=None):
        self.broker = broker
        self.logger = logger
        self.reconciliation = ReconciliationService()
        self.monitor = PositionMonitor()

    def signal_to_order(self, s):
        side = OrderSide.BUY if s.action == "BUY" else OrderSide.SELL
        t = now_str()
        note_parts = [s.reason]
        if s.regime: note_parts.append(f"regime={s.regime}")
        return Order(str(uuid.uuid4()), s.ticker, side, s.target_qty, s.reference_price, s.reference_price,
                     OrderStatus.NEW, s.strategy_name, s.score, s.ai_confidence, s.industry,
                     t, t, " | ".join([x for x in note_parts if x]), s.model_name, s.model_version, s.regime)

    def execute(self, signals):
        auto_exit_signals = self.monitor.generate_exit_signals(self.broker)
        merged_signals = auto_exit_signals + signals

        submitted = filled = rejected = partially_filled = cancelled = 0
        orders = []; all_fills = []
        for s in merged_signals:
            self.broker.update_market_price(s.ticker, s.reference_price)
            order = self.signal_to_order(s); orders.append(order)
            if self.logger: self.logger.insert_order(order)
            order.status = OrderStatus.PENDING_SUBMIT; order.updated_at = now_str()
            if self.logger: self.logger.update_order_status(order.order_id, order.status, order.updated_at)
            order, fills = self.broker.place_order(order); all_fills.extend(fills)
            if order.status == OrderStatus.PARTIALLY_FILLED: partially_filled += 1
            elif order.status == OrderStatus.FILLED: filled += 1
            elif order.status == OrderStatus.REJECTED: rejected += 1
            elif order.status == OrderStatus.CANCELLED: cancelled += 1
            submitted += 1
            if self.logger:
                self.logger.update_order_status(order.order_id, order.status, order.updated_at, order.note)
                for f in fills: self.logger.insert_fill(f)
        recon = self.reconciliation.summarize(orders, all_fills)
        return {
            "submitted": submitted,
            "filled": filled,
            "partially_filled": partially_filled,
            "rejected": rejected,
            "cancelled": cancelled,
            "fills_count": len(all_fills),
            "auto_exit_signals": len(auto_exit_signals),
            "reconciliation": recon,
        }
