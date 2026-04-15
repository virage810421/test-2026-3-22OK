# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG


class TrueBrokerReadinessGate:
    MODULE_VERSION = 'v84_true_broker_readiness_gate_with_live_closure'

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

    def build(self) -> tuple[Path, dict[str, Any]]:
        cfg = self._read_json(self.config_path)
        closure = self._read_json(PATHS.runtime_dir / 'true_broker_live_closure.json')
        real_api = self._read_json(PATHS.runtime_dir / 'real_api_readiness.json')
        kill_state = self._read_json(PATHS.runtime_dir / 'kill_switch_state.json')
        lights = {
            'api': bool(real_api.get('api_bound')),
            'callback': bool(real_api.get('callback_bound')),
            'ledger': bool(real_api.get('ledger_bound')),
            'reconcile': bool(real_api.get('reconcile_bound')),
            'kill_switch': bool(real_api.get('kill_switch_bound')) and bool(kill_state),
        }
        red = [k for k, ok in lights.items() if not ok]
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'module_version': self.MODULE_VERSION,
            'status': 'true_broker_ready' if not red and closure.get('status') == 'true_broker_live_closure_ready' else 'blocked_for_real_broker',
            'lights': lights,
            'red_lights': red,
            'green_count': 5 - len(red),
            'config_path': str(self.config_path),
            'evidence': {
                'adapter_enabled': bool(cfg.get('enabled', False)),
                'provider_name': str(cfg.get('provider_name') or '').strip(),
                'base_url_present': bool(cfg.get('base_url')),
                'closure_status': closure.get('status'),
                'closure_checks': closure.get('checks', {}),
                'real_api_status': real_api.get('status'),
            },
            'production_rule': 'LIVE/真券商必須五燈全綠，且 true_broker_live_closure 必須 ready；沒有真實 API/callback/ledger/reconcile 證據時不可過關。',
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.path, payload


def main() -> int:
    path, payload = TrueBrokerReadinessGate().build()
    print(f'🏦 真券商 readiness gate：{path} | status={payload.get("status")} green={payload.get("green_count")}/5')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
