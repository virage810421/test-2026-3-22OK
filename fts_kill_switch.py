# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, write_json


class KillSwitchManager:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'kill_switch_state.json'

    def _load(self) -> dict[str, Any]:
        payload = load_json(self.path, None)
        if isinstance(payload, dict):
            return payload
        return {
            'generated_at': now_str(),
            'system': {'armed': False, 'reason': ''},
            'account': {'armed': False, 'reason': ''},
            'strategies': {},
            'symbols': {},
        }

    def ensure_default_state(self) -> dict[str, Any]:
        payload = self._load()
        if not self.path.exists():
            self.save(payload)
        return payload

    def save(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload['generated_at'] = now_str()
        write_json(self.path, payload)
        return payload

    def trigger(self, level: str, key: str = 'default', reason: str = '') -> dict[str, Any]:
        payload = self._load()
        if level in {'system', 'account'}:
            payload[level] = {'armed': True, 'reason': reason, 'key': key, 'updated_at': now_str()}
        elif level == 'strategy':
            payload['strategies'][key] = {'armed': True, 'reason': reason, 'updated_at': now_str()}
        elif level == 'symbol':
            payload['symbols'][key] = {'armed': True, 'reason': reason, 'updated_at': now_str()}
        else:
            raise ValueError(f'unsupported kill switch level: {level}')
        self.save(payload)
        log(f'🛑 kill switch 觸發 | level={level} | key={key} | reason={reason}')
        return payload

    def clear(self, level: str, key: str = 'default') -> dict[str, Any]:
        payload = self._load()
        if level in {'system', 'account'}:
            payload[level] = {'armed': False, 'reason': '', 'key': key, 'updated_at': now_str()}
        elif level == 'strategy':
            payload['strategies'].pop(key, None)
        elif level == 'symbol':
            payload['symbols'].pop(key, None)
        else:
            raise ValueError(f'unsupported kill switch level: {level}')
        self.save(payload)
        log(f'✅ kill switch 清除 | level={level} | key={key}')
        return payload

    def is_blocked(self, symbol: str | None = None, strategy: str | None = None) -> tuple[bool, list[str]]:
        payload = self._load()
        reasons = []
        if payload.get('system', {}).get('armed'):
            reasons.append(f"system:{payload['system'].get('reason', '')}")
        if payload.get('account', {}).get('armed'):
            reasons.append(f"account:{payload['account'].get('reason', '')}")
        if strategy and payload.get('strategies', {}).get(strategy, {}).get('armed'):
            reasons.append(f"strategy:{strategy}:{payload['strategies'][strategy].get('reason', '')}")
        if symbol and payload.get('symbols', {}).get(symbol, {}).get('armed'):
            reasons.append(f"symbol:{symbol}:{payload['symbols'][symbol].get('reason', '')}")
        return len(reasons) > 0, reasons
