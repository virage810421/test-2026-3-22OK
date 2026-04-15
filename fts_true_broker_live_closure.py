# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str
from fts_exception_policy import record_diagnostic
from fts_broker_api_adapter import ConfigurableBrokerAdapter
from fts_callback_ingestion_service import CallbackIngestionService
from fts_execution_ledger import ExecutionLedger
from fts_execution_runtime import ReconciliationEngine


class TrueBrokerLiveClosureService:
    MODULE_VERSION = 'v84_true_broker_live_closure_service'

    def __init__(self) -> None:
        self.summary_path = PATHS.runtime_dir / 'true_broker_live_closure.json'
        self.snapshot_path = PATHS.runtime_dir / 'broker_live_runtime_snapshot.json'
        self.local_summary_path = PATHS.runtime_dir / 'local_execution_evidence.json'
        self.adapter = ConfigurableBrokerAdapter()

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _read_jsonl(path: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not path.exists():
            return rows
        try:
            for line in path.read_text(encoding='utf-8').splitlines():
                if not line.strip():
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
        except Exception:
            return rows
        return rows

    @staticmethod
    def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not path.exists():
            return rows
        try:
            with path.open('r', encoding='utf-8-sig', newline='') as fh:
                for row in csv.DictReader(fh):
                    if isinstance(row, dict):
                        rows.append(dict(row))
        except Exception:
            return rows
        return rows

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ''):
                return default
            return float(value)
        except Exception:
            return default

    def _load_local_orders(self) -> tuple[str | None, list[dict[str, Any]]]:
        events = self._read_jsonl(PATHS.runtime_dir / 'execution_ledger_events.jsonl')
        rows: list[dict[str, Any]] = []
        for env in events:
            payload = env.get('payload') if isinstance(env.get('payload'), dict) else {}
            if str(env.get('event_type', '')).lower() in {'submission_event', 'callback_event'} and payload:
                rows.append(dict(payload))
        if rows:
            return str(PATHS.runtime_dir / 'execution_ledger_events.jsonl'), rows
        for cand in [PATHS.data_dir / 'approved_executable_orders.csv', PATHS.base_dir / 'approved_executable_orders.csv']:
            csv_rows = self._read_csv_rows(cand)
            if csv_rows:
                return str(cand), csv_rows
        return None, []

    def _load_local_fills(self) -> tuple[str | None, list[dict[str, Any]]]:
        events = self._read_jsonl(PATHS.runtime_dir / 'execution_ledger_events.jsonl')
        rows: list[dict[str, Any]] = []
        for env in events:
            payload = env.get('payload') if isinstance(env.get('payload'), dict) else {}
            if str(env.get('event_type', '')).lower() == 'fill_event' and payload:
                rows.append(dict(payload))
        if rows:
            return str(PATHS.runtime_dir / 'execution_ledger_events.jsonl'), rows
        return None, []

    def _load_local_positions(self) -> tuple[str | None, list[dict[str, Any]]]:
        for cand in [PATHS.runtime_dir / 'restart_recovery_snapshot.json', PATHS.state_dir / 'engine_state.json']:
            payload = self._read_json(cand)
            rows = payload.get('positions') if isinstance(payload.get('positions'), list) else []
            if rows:
                return str(cand), [x for x in rows if isinstance(x, dict)]
        for cand in [PATHS.data_dir / 'active_positions.csv', PATHS.base_dir / 'active_positions.csv']:
            rows = self._read_csv_rows(cand)
            if rows:
                return str(cand), rows
        return None, []

    def _load_local_cash(self) -> tuple[str | None, float | None]:
        for cand in [PATHS.runtime_dir / 'restart_recovery_snapshot.json', PATHS.state_dir / 'engine_state.json', PATHS.runtime_dir / 'execution_release_manifest.json']:
            payload = self._read_json(cand)
            for key in ('cash', 'cash_available', 'broker_cash', 'local_cash'):
                if key in payload:
                    return str(cand), self._safe_float(payload.get(key), None)
        return None, None

    @staticmethod
    def _combine_broker_orders(open_orders: list[dict[str, Any]], callbacks: list[dict[str, Any]], fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for row in open_orders or []:
            key = str(row.get('client_order_id') or row.get('broker_order_id') or row.get('order_id') or '').strip()
            if key:
                out[key] = dict(row)
        for row in callbacks or []:
            key = str(row.get('client_order_id') or row.get('broker_order_id') or '').strip()
            if not key:
                continue
            base = out.get(key, {})
            base.update({'client_order_id': row.get('client_order_id'), 'broker_order_id': row.get('broker_order_id'), 'status': row.get('status'), 'symbol': row.get('symbol') or row.get('ticker')})
            out[key] = base
        for row in fills or []:
            key = str(row.get('client_order_id') or row.get('broker_order_id') or row.get('order_id') or '').strip()
            if not key:
                continue
            base = out.get(key, {})
            base.update({'client_order_id': row.get('client_order_id'), 'broker_order_id': row.get('broker_order_id'), 'status': row.get('status') or 'FILLED', 'symbol': row.get('symbol') or row.get('ticker'), 'filled_qty': row.get('filled_qty') or row.get('qty')})
            out[key] = base
        return list(out.values())

    def build(self) -> tuple[str, dict[str, Any]]:
        probe_path, probe = self.adapter.probe()
        capability = self.adapter.capability_report()
        connect = self.adapter.connect() if probe.get('ready_for_live_connect') else {'connected': False, 'status': 'connect_blocked_by_probe'}
        snapshot = {'orders': [], 'fills': [], 'positions': [], 'cash': {}, 'callbacks': []}
        callback_summary = {'status': 'callback_ingestion_not_run', 'ingested_count': 0}
        recon_payload: dict[str, Any] = {}
        ledger_summary = {'status': 'execution_ledger_not_built'}
        local_sources: dict[str, Any] = {}
        try:
            if probe.get('ready_for_live_connect') and connect.get('connected'):
                snapshot = self.adapter.export_broker_snapshot() if callable(getattr(self.adapter, 'export_broker_snapshot', None)) else {}
                snapshot.setdefault('orders', self.adapter.get_open_orders())
                snapshot.setdefault('fills', self.adapter.get_fills())
                snapshot.setdefault('positions', self.adapter.get_positions())
                snapshot.setdefault('cash', self.adapter.get_cash())
                snapshot['callbacks'] = self.adapter.poll_callbacks() if callable(getattr(self.adapter, 'poll_callbacks', None)) else []
                self.snapshot_path.write_text(json.dumps({'generated_at': now_str(), **snapshot}, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
                _, callback_summary = CallbackIngestionService().ingest(snapshot.get('callbacks', []), broker=str(capability.get('provider_name') or 'CONFIGURABLE_REAL_ADAPTER'), account_id=str((self.adapter._config.get('auth', {}) or {}).get('account_id', '')), fanout_ledger=True)
                _, ledger_summary = ExecutionLedger().build_summary()
                local_orders_src, local_orders = self._load_local_orders()
                local_fills_src, local_fills = self._load_local_fills()
                local_positions_src, local_positions = self._load_local_positions()
                local_cash_src, local_cash = self._load_local_cash()
                local_sources = {
                    'local_orders': local_orders_src,
                    'local_fills': local_fills_src,
                    'local_positions': local_positions_src,
                    'local_cash': local_cash_src,
                }
                broker_orders = self._combine_broker_orders(snapshot.get('orders', []), callback_summary.get('latest_callbacks', []) or [], snapshot.get('fills', []))
                _recon_path, recon_payload = ReconciliationEngine().reconcile(
                    local_orders,
                    broker_orders,
                    local_fills,
                    snapshot.get('fills', []),
                    local_positions,
                    snapshot.get('positions', []),
                    local_cash,
                    self._safe_float((snapshot.get('cash') or {}).get('cash_available') or (snapshot.get('cash') or {}).get('cash'), 0.0),
                )
        except Exception as exc:
            record_diagnostic('true_broker_live_closure', 'build_failed', exc, severity='error', fail_closed=True)
            recon_payload = {'status': 'true_broker_live_closure_error', 'error': repr(exc)}
        kill_switch_state = self._read_json(PATHS.runtime_dir / 'kill_switch_state.json')
        recon_green = bool(recon_payload.get('all_green')) if isinstance(recon_payload, dict) else False
        if not recon_green and isinstance(recon_payload, dict):
            recon_green = bool(recon_payload.get('status') in {'reconciled', 'ok'} and int(recon_payload.get('order_issue_count', 0) or 0) == 0 and abs(self._safe_float(recon_payload.get('cash_diff'), 0.0)) <= float(getattr(CONFIG, 'true_broker_cash_tolerance', 1.0)))
        checks = {
            'adapter_ready': bool(probe.get('ready_for_live_connect')),
            'api_connected': bool(connect.get('connected')),
            'callback_evidence': int(callback_summary.get('ingested_count', 0) or 0) > 0,
            'ledger_evidence': bool((ledger_summary.get('lane_event_counts') or {}) or (ledger_summary.get('lane_order_counts') or {})),
            'reconcile_green': recon_green,
            'kill_switch_ready': bool(kill_switch_state),
            'activity_observed': bool(snapshot.get('orders') or snapshot.get('fills') or callback_summary.get('ingested_count', 0)),
        }
        status = 'true_broker_live_closure_ready' if all(checks.values()) else 'true_broker_live_closure_partial'
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'status': status,
            'checks': checks,
            'probe_path': str(probe_path),
            'probe': probe,
            'connect': connect,
            'capability': capability,
            'snapshot_path': str(self.snapshot_path) if self.snapshot_path.exists() else '',
            'callback_summary': callback_summary,
            'ledger_summary': ledger_summary,
            'reconciliation': recon_payload,
            'local_sources': local_sources,
            'production_rule': '真券商 live 閉環必須同時拿到 adapter/connect、callback、ledger、reconcile、kill-switch 的可驗證證據。',
        }
        self.summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        local_payload = {
            'generated_at': now_str(),
            'status': 'local_execution_evidence_ready' if local_sources else 'local_execution_evidence_missing',
            'sources': local_sources,
        }
        self.local_summary_path.write_text(json.dumps(local_payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        return str(self.summary_path), payload
