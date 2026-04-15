# -*- coding: utf-8 -*-
from __future__ import annotations

"""真券商 readiness 五紅燈 Gate v92.

注意：這不是把真券商完成；它把缺口變成不可繞過的正式紅燈：
API / callback / ledger / reconcile / kill switch，五項缺一不可。
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG


class TrueBrokerReadinessGate:
    MODULE_VERSION = 'v92_true_broker_five_red_lights_gate'

    def __init__(self) -> None:
        self.path = PATHS.runtime_dir / 'true_broker_readiness_gate.json'
        self.config_path = PATHS.base_dir / getattr(CONFIG, 'broker_adapter_config_filename', 'broker_adapter_config.json')

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
    def _file_has_lines(path: Path) -> bool:
        if not path.exists():
            return False
        try:
            return any(line.strip() for line in path.read_text(encoding='utf-8').splitlines())
        except Exception:
            return False

    def build(self) -> tuple[Path, dict[str, Any]]:
        cfg = self._read_json(self.config_path)
        adapter_enabled = bool(cfg.get('enabled', False))
        provider = str(cfg.get('provider_name') or '').strip()
        base_url = str(cfg.get('base_url') or '').strip()
        auth = cfg.get('auth', {}) if isinstance(cfg.get('auth', {}), dict) else {}
        real_api = self._read_json(PATHS.runtime_dir / 'real_api_readiness.json')
        probe = self._read_json(PATHS.runtime_dir / 'broker_adapter_probe.json')
        kill_state = self._read_json(PATHS.runtime_dir / 'kill_switch_state.json')
        recon = self._read_json(PATHS.runtime_dir / 'reconciliation_engine.json') or self._read_json(PATHS.runtime_dir / 'reconciliation_report.json')
        ledger_candidates = [
            PATHS.runtime_dir / 'execution_ledger.json',
            PATHS.runtime_dir / 'execution_fills.json',
            PATHS.runtime_dir / 'broker_ledger_snapshot.json',
            PATHS.data_dir / 'broker_ledger_snapshot.csv',
        ]
        callback_candidates = [
            PATHS.runtime_dir / 'broker_callback_events.jsonl',
            PATHS.runtime_dir / 'broker_callbacks.json',
            PATHS.runtime_dir / 'callback_ingestion_service.json',
        ]
        api_green = bool(adapter_enabled and provider and provider != 'fill_me_after_account_opened' and base_url and auth.get('api_key') and auth.get('account_id') and (probe.get('ready_for_live_connect') or real_api.get('api_bound')))
        callback_green = bool(real_api.get('callback_bound') or any(self._file_has_lines(p) or p.exists() for p in callback_candidates))
        ledger_green = bool(real_api.get('ledger_bound') or any(p.exists() for p in ledger_candidates))
        reconcile_green = bool(real_api.get('reconcile_bound') and recon) or bool(recon.get('status') in {'reconciled', 'ok', 'ready'})
        kill_switch_green = bool(getattr(CONFIG, 'enable_live_kill_switch', True) and kill_state)
        lights = {
            'api': api_green,
            'callback': callback_green,
            'ledger': ledger_green,
            'reconcile': reconcile_green,
            'kill_switch': kill_switch_green,
        }
        red = [k for k, ok in lights.items() if not ok]
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'module_version': self.MODULE_VERSION,
            'status': 'true_broker_ready' if not red else 'blocked_for_real_broker',
            'lights': lights,
            'red_lights': red,
            'green_count': 5 - len(red),
            'config_path': str(self.config_path),
            'evidence': {
                'adapter_enabled': adapter_enabled,
                'provider_name': provider,
                'base_url_present': bool(base_url),
                'auth_api_key_present': bool(auth.get('api_key')),
                'auth_account_id_present': bool(auth.get('account_id')),
                'probe_ready_for_live_connect': bool(probe.get('ready_for_live_connect')),
                'callback_files': [str(p) for p in callback_candidates if p.exists()],
                'ledger_files': [str(p) for p in ledger_candidates if p.exists()],
                'reconciliation_status': recon.get('status'),
                'kill_switch_state_present': bool(kill_state),
            },
            'production_rule': 'LIVE/真券商必須五燈全綠；沒有真實券商 API/callback/ledger/reconcile 實證時不可補分。',
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.path, payload


def main() -> int:
    path, payload = TrueBrokerReadinessGate().build()
    print(f'🏦 真券商 readiness gate：{path} | status={payload.get("status")} green={payload.get("green_count")}/5')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
