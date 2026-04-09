# -*- coding: utf-8 -*-
from __future__ import annotations

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
