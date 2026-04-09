# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_real_broker_adapter_blueprint import RealBrokerAdapterBlueprint
from fts_utils import now_str

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


@dataclass
class BrokerAdapterPaths:
    config_path: Path
    template_path: Path
    runtime_probe_path: Path


class ConfigurableBrokerAdapter(RealBrokerAdapterBlueprint):
    """可插式 Phase3 broker adapter。

    - 沒開戶前：產生 config template、驗 contract、可做 dry-run probe。
    - 開戶後：把 broker_adapter_config.json 填好，即可接 REST / polling 類型 API。
    - 若 config 仍是 template，會回傳 clear blocked status，而不是直接崩潰。
    """

    MODULE_VERSION = 'v83_configurable_broker_adapter'

    def __init__(self, config_path: Path | None = None):
        self.paths = BrokerAdapterPaths(
            config_path=config_path or (PATHS.base_dir / getattr(CONFIG, 'broker_adapter_config_filename', 'broker_adapter_config.json')),
            template_path=PATHS.runtime_dir / 'broker_adapter_config.template.json',
            runtime_probe_path=PATHS.runtime_dir / 'broker_adapter_probe.json',
        )
        self._config = self._load_config()
        self._session = requests.Session() if requests is not None else None

    # -----------------------------
    # config management
    # -----------------------------
    @staticmethod
    def default_template() -> dict[str, Any]:
        return {
            'generated_at': now_str(),
            'module_version': ConfigurableBrokerAdapter.MODULE_VERSION,
            'enabled': False,
            'provider_name': 'fill_me_after_account_opened',
            'transport': 'rest',
            'base_url': '',
            'auth': {
                'api_key': '',
                'api_secret': '',
                'account_id': '',
                'cert_or_token': '',
                'header_api_key': 'X-API-KEY',
                'header_bearer_prefix': 'Bearer ',
            },
            'timeouts': {'connect_seconds': 10, 'read_seconds': 20},
            'endpoints': {
                'connect': {'method': 'GET', 'path': '/v1/account/ping'},
                'place_order': {'method': 'POST', 'path': '/v1/orders'},
                'cancel_order': {'method': 'POST', 'path': '/v1/orders/{broker_order_id}/cancel'},
                'replace_order': {'method': 'POST', 'path': '/v1/orders/{broker_order_id}/replace'},
                'get_order_status': {'method': 'GET', 'path': '/v1/orders/{broker_order_id}'},
                'get_fills': {'method': 'GET', 'path': '/v1/fills'},
                'get_positions': {'method': 'GET', 'path': '/v1/positions'},
                'get_cash': {'method': 'GET', 'path': '/v1/account/cash'},
                'poll_callbacks': {'method': 'GET', 'path': '/v1/events'},
            },
            'field_mapping': {
                'order_status': 'status',
                'broker_order_id': 'broker_order_id',
                'client_order_id': 'client_order_id',
                'filled_qty': 'filled_qty',
                'remaining_qty': 'remaining_qty',
                'avg_fill_price': 'avg_fill_price',
                'reject_reason': 'reject_reason',
                'event_time': 'timestamp',
            },
            'payload_mapping': {
                'ticker': 'symbol',
                'side': 'side',
                'qty': 'qty',
                'price': 'price',
                'order_type': 'order_type',
                'time_in_force': 'time_in_force',
                'session': 'session',
                'client_order_id': 'client_order_id',
                'idempotency_key': 'idempotency_key',
            },
            'polling': {'enabled': True, 'use_since_cursor': True, 'cursor_field': 'cursor'},
        }

    def ensure_template_files(self) -> tuple[Path, Path]:
        template = self.default_template()
        self.paths.template_path.write_text(json.dumps(template, ensure_ascii=False, indent=2), encoding='utf-8')
        if not self.paths.config_path.exists():
            self.paths.config_path.write_text(json.dumps(template, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.paths.template_path, self.paths.config_path

    def _load_config(self) -> dict[str, Any]:
        self.ensure_template_files()
        try:
            return json.loads(self.paths.config_path.read_text(encoding='utf-8'))
        except Exception:
            return self.default_template()

    def _is_ready(self) -> tuple[bool, list[str]]:
        missing: list[str] = []
        enabled = bool(self._config.get('enabled'))
        if not enabled:
            missing.append('config_enabled_false')
        if not str(self._config.get('provider_name', '')).strip() or self._config.get('provider_name') == 'fill_me_after_account_opened':
            missing.append('provider_name')
        if not str(self._config.get('base_url', '')).strip():
            missing.append('base_url')
        auth = self._config.get('auth', {}) or {}
        for field in ['api_key', 'account_id']:
            if not str(auth.get(field, '')).strip():
                missing.append(f'auth.{field}')
        if requests is None:
            missing.append('requests_not_installed')
        return (len(missing) == 0), missing

    def probe(self) -> tuple[Path, dict[str, Any]]:
        ok, missing = self._is_ready()
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'config_path': str(self.paths.config_path),
            'template_path': str(self.paths.template_path),
            'provider_name': self._config.get('provider_name', ''),
            'transport': self._config.get('transport', 'rest'),
            'ready_for_live_connect': ok,
            'missing_items': missing,
            'status': 'broker_adapter_ready' if ok else 'broker_adapter_account_pending',
        }
        self.paths.runtime_probe_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.paths.runtime_probe_path, payload

    # -----------------------------
    # http helpers
    # -----------------------------
    def _headers(self) -> dict[str, str]:
        auth = self._config.get('auth', {}) or {}
        headers = {'Content-Type': 'application/json'}
        api_key = str(auth.get('api_key', '')).strip()
        secret = str(auth.get('api_secret', '')).strip()
        if api_key:
            headers[str(auth.get('header_api_key', 'X-API-KEY'))] = api_key
        if secret:
            headers['Authorization'] = f"{auth.get('header_bearer_prefix', 'Bearer ')}{secret}"
        return headers

    def _endpoint(self, name: str, **path_vars: Any) -> tuple[str, str]:
        endpoints = self._config.get('endpoints', {}) or {}
        spec = endpoints.get(name, {}) or {}
        method = str(spec.get('method', 'GET')).upper()
        path = str(spec.get('path', '/')).format(**path_vars)
        base_url = str(self._config.get('base_url', '')).rstrip('/')
        return method, f'{base_url}{path}'

    def _map_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        mapping = self._config.get('payload_mapping', {}) or {}
        out: dict[str, Any] = {}
        for src_key, dst_key in mapping.items():
            if src_key in payload:
                out[str(dst_key)] = payload[src_key]
        if 'client_order_id' not in out:
            out['client_order_id'] = payload.get('client_order_id') or f'CLIENT-{uuid.uuid4().hex[:10].upper()}'
        if 'idempotency_key' not in out:
            out['idempotency_key'] = payload.get('idempotency_key') or out['client_order_id']
        return out

    def _request(self, name: str, *, payload: dict[str, Any] | None = None, params: dict[str, Any] | None = None, **path_vars: Any) -> dict[str, Any]:
        ok, missing = self._is_ready()
        if not ok:
            return {'ok': False, 'status': 'adapter_not_ready', 'missing_items': missing}
        if self._session is None:
            return {'ok': False, 'status': 'requests_not_available'}
        method, url = self._endpoint(name, **path_vars)
        timeout_cfg = self._config.get('timeouts', {}) or {}
        timeout = (int(timeout_cfg.get('connect_seconds', 10)), int(timeout_cfg.get('read_seconds', 20)))
        response = self._session.request(method=method, url=url, headers=self._headers(), json=payload, params=params, timeout=timeout)
        body: Any
        try:
            body = response.json()
        except Exception:
            body = {'raw_text': response.text}
        return {
            'ok': response.ok,
            'http_status': response.status_code,
            'status': 'http_ok' if response.ok else 'http_error',
            'body': body,
            'requested_at': now_str(),
            'endpoint_name': name,
            'url': url,
        }

    # -----------------------------
    # blueprint implementation
    # -----------------------------
    def connect(self) -> dict[str, Any]:
        return self._request('connect')

    def refresh_auth(self) -> dict[str, Any]:
        return {'ok': True, 'status': 'auth_refresh_delegated_to_provider', 'requested_at': now_str()}

    def place_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._map_payload(payload)
        return self._request('place_order', payload=normalized)

    def cancel_order(self, broker_order_id: str) -> dict[str, Any]:
        return self._request('cancel_order', broker_order_id=broker_order_id)

    def replace_order(self, broker_order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._map_payload(payload)
        return self._request('replace_order', payload=normalized, broker_order_id=broker_order_id)

    def get_order_status(self, broker_order_id: str) -> dict[str, Any]:
        return self._request('get_order_status', broker_order_id=broker_order_id)

    def get_fills(self, trading_date: str | None = None) -> list[dict[str, Any]]:
        result = self._request('get_fills', params={'trading_date': trading_date} if trading_date else None)
        body = result.get('body', {})
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            return list(body.get('items', []))
        return []

    def get_positions(self) -> list[dict[str, Any]]:
        result = self._request('get_positions')
        body = result.get('body', {})
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            return list(body.get('items', []))
        return []

    def get_cash(self) -> dict[str, Any]:
        result = self._request('get_cash')
        body = result.get('body', {})
        if isinstance(body, dict):
            return body
        return {'raw': body}

    def disconnect(self) -> dict[str, Any]:
        return {'ok': True, 'status': 'disconnect_noop_for_rest_adapter', 'requested_at': now_str()}

    def poll_callbacks(self, cursor: str | None = None) -> dict[str, Any]:
        params = {'cursor': cursor} if cursor else None
        return self._request('poll_callbacks', params=params)
