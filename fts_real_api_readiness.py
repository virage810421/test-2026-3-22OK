# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class RealAPIReadinessBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'real_api_readiness.json'

    @staticmethod
    def _read_json(path):
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def build(self):
        probe = self._read_json(PATHS.runtime_dir / 'broker_adapter_probe.json')
        connect = self._read_json(PATHS.runtime_dir / 'broker_adapter_last_connect.json')
        callback = self._read_json(PATHS.runtime_dir / 'broker_callback_ingestion_summary.json')
        ledger = self._read_json(PATHS.runtime_dir / 'execution_ledger_summary.json')
        recon = self._read_json(PATHS.runtime_dir / 'reconciliation_engine.json') or self._read_json(PATHS.runtime_dir / 'reconciliation_report.json')
        closure = self._read_json(PATHS.runtime_dir / 'true_broker_live_closure.json')
        callback_bound = int(callback.get('ingested_count', 0) or 0) > 0
        ledger_bound = bool((ledger.get('lane_event_counts') or {}) or (ledger.get('lane_order_counts') or {}))
        reconcile_bound = bool((closure.get('checks') or {}).get('reconcile_green')) or bool(recon.get('all_green'))
        api_bound = bool(probe.get('ready_for_live_connect')) and bool(connect.get('connected'))
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'target': 'real_broker_api_and_real_market_binding',
            'completed_now': [
                'adapter probe evidence' if probe else 'adapter probe missing',
                'real connect evidence' if connect else 'real connect missing',
                'callback ingestion evidence' if callback_bound else 'callback evidence missing',
                'execution ledger evidence' if ledger_bound else 'ledger evidence missing',
                'broker/live reconciliation evidence' if reconcile_bound else 'reconcile evidence missing',
            ],
            'api_bound': api_bound,
            'callback_bound': callback_bound,
            'ledger_bound': ledger_bound,
            'reconcile_bound': reconcile_bound,
            'kill_switch_bound': bool(getattr(CONFIG, 'enable_live_kill_switch', True) and self._read_json(PATHS.runtime_dir / 'kill_switch_state.json')),
            'true_broker_red_lights': {
                'api': 'GREEN' if api_bound else 'RED: no real adapter connect proof',
                'callback': 'GREEN' if callback_bound else 'RED: no callback evidence',
                'ledger': 'GREEN' if ledger_bound else 'RED: no ledger evidence',
                'reconcile': 'GREEN' if reconcile_bound else 'RED: no green reconciliation proof',
                'kill_switch': 'GREEN' if getattr(CONFIG, 'enable_live_kill_switch', True) else 'RED',
            },
            'closure_status': closure.get('status', ''),
            'status': 'real_api_bound_with_evidence' if api_bound and callback_bound and ledger_bound and reconcile_bound else 'five_red_lights_until_real_broker_bound',
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f"🛰️ 已輸出 real api readiness：{self.path}")
        return self.path, payload
