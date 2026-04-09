# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json


class EODCloseBookBuilder:
    def build(self) -> tuple[str, dict[str, Any]]:
        recon = load_json(PATHS.runtime_dir / 'reconciliation_engine.json', {}) or {}
        attrib = load_json(PATHS.runtime_dir / 'performance_attribution.json', {}) or {}
        daily_ops = load_json(PATHS.runtime_dir / 'daily_ops_summary.json', {}) or {}
        phase2 = load_json(PATHS.runtime_dir / 'phase2_mock_real_broker.json', {}) or {}
        incident = load_json(PATHS.runtime_dir / 'intraday_incident_guard.json', {}) or {}
        execution = load_json(PATHS.runtime_dir / 'decision_execution_bridge.json', {}) or {}
        callback_store = load_json(PATHS.runtime_dir / 'callback_event_store_summary.json', {}) or {}

        recon_green = bool(recon.get('all_green', recon.get('summary', {}).get('all_green', False)))
        payload = {
            'generated_at': now_str(),
            'status': 'closebook_ready' if recon_green else 'closebook_attention',
            'reconciliation_status': recon.get('status', 'missing'),
            'all_green': recon_green,
            'headline': {
                'payload_rows': execution.get('rows_total', 0),
                'orders_submitted': phase2.get('orders_submitted', 0),
                'orders_filled': phase2.get('orders_filled', 0),
                'fills_count': phase2.get('fills_count', 0),
                'callbacks_recorded': phase2.get('callbacks_recorded', 0),
                'incident_status': incident.get('status', 'missing'),
            },
            'pnl_summary': attrib.get('headline', {}),
            'callback_store': callback_store,
            'close_notes': (daily_ops.get('close_notes', []) or []) + phase2.get('notes', []),
            'next_day_resume_ready': recon_green and incident.get('status') != 'incident_guard_block',
        }
        path = PATHS.runtime_dir / 'eod_closebook.json'
        write_json(path, payload)
        return str(path), payload
