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
from fts_exception_policy import record_diagnostic

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


@dataclass
class BrokerAdapterPaths:
    config_path: Path
    template_path: Path
    runtime_probe_path: Path
    runtime_last_connect_path: Path


class ConfigurableBrokerAdapter(RealBrokerAdapterBlueprint):
    MODULE_VERSION = 'v84_configurable_broker_adapter_live_closure'

    def __init__(self, config_path: Path | None = None):
        self.paths = BrokerAdapterPaths(
            config_path=config_path or (PATHS.base_dir / getattr(CONFIG, 'broker_adapter_config_filename', 'broker_adapter_config.json')),
            template_path=PATHS.runtime_dir / 'broker_adapter_config.template.json',
            runtime_probe_path=PATHS.runtime_dir / 'broker_adapter_probe.json',
            runtime_last_connect_path=PATHS.runtime_dir / 'broker_adapter_last_connect.json',
        )
        self._config = self._load_config()
        self._session = requests.Session() if requests is not None else None

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
                'get_open_orders': {'method': 'GET', 'path': '/v1/orders/open'},
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
        except Exception as exc:
            record_diagnostic('broker_adapter', 'load_config', exc, severity='warning', fail_closed=True)
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
        endpoints = self._config.get('endpoints', {}) or {}
        for key in ['connect', 'place_order', 'get_order_status', 'get_open_orders', 'get_fills', 'get_positions', 'get_cash', 'poll_callbacks']:
            if key not in endpoints:
                missing.append(f'endpoints.{key}')
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

    def capability_report(self) -> dict[str, Any]:
        ok, missing = self._is_ready()
        endpoints = self._config.get('endpoints', {}) or {}
        return {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'broker_kind': 'configurable_rest_adapter',
            'provider_name': self._config.get('provider_name', ''),
            'transport': self._config.get('transport', 'rest'),
            'supports_callbacks': 'poll_callbacks' in endpoints,
            'supports_open_orders': 'get_open_orders' in endpoints,
            'supports_replace': 'replace_order' in endpoints,
            'supports_cancel': 'cancel_order' in endpoints,
            'broker_bound': ok,
            'true_broker_ready': False,
            'missing_for_true_broker': missing,
            'status': 'adapter_capability_ready' if ok else 'adapter_capability_blocked',
        }

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
        try:
            body: Any = response.json()
        except Exception as exc:
            record_diagnostic('broker_adapter', 'parse_http_response_json', exc, severity='warning', fail_closed=True)
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

    @staticmethod
    def _extract_items(result: dict[str, Any], keys: tuple[str, ...] = ('items', 'rows', 'data', 'events', 'orders', 'fills', 'positions')) -> list[dict[str, Any]]:
        body = result.get('body', {}) if isinstance(result, dict) else {}
        if isinstance(body, list):
            return [x for x in body if isinstance(x, dict)]
        if isinstance(body, dict):
            for key in keys:
                value = body.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict)]
            if all(k in body for k in ('broker_order_id', 'status')) or all(k in body for k in ('cash_available', 'equity')):
                return [body]
        return []

    def connect(self) -> dict[str, Any]:
        result = self._request('connect')
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'provider_name': self._config.get('provider_name', ''),
            'connected': bool(result.get('ok')),
            'status': 'connected' if result.get('ok') else result.get('status', 'connect_failed'),
            'raw': result,
        }
        self.paths.runtime_last_connect_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return payload

    def refresh_auth(self) -> dict[str, Any]:
        return {'ok': True, 'status': 'auth_refresh_delegated_to_provider', 'requested_at': now_str()}

    def place_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request('place_order', payload=self._map_payload(payload))

    def cancel_order(self, broker_order_id: str) -> dict[str, Any]:
        return self._request('cancel_order', broker_order_id=broker_order_id)

    def replace_order(self, broker_order_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request('replace_order', payload=self._map_payload(payload), broker_order_id=broker_order_id)

    def get_order_status(self, broker_order_id: str) -> dict[str, Any]:
        result = self._request('get_order_status', broker_order_id=broker_order_id)
        rows = self._extract_items(result, keys=('items', 'rows', 'data', 'orders'))
        if rows:
            return rows[0]
        body = result.get('body', {}) if isinstance(result.get('body', {}), dict) else {}
        return body if isinstance(body, dict) else {'raw': body}

    def get_open_orders(self) -> list[dict[str, Any]]:
        return self._extract_items(self._request('get_open_orders'), keys=('items', 'rows', 'data', 'orders'))

    def get_fills(self, trading_date: str | None = None) -> list[dict[str, Any]]:
        return self._extract_items(self._request('get_fills', params={'trading_date': trading_date} if trading_date else None), keys=('items', 'rows', 'data', 'fills'))

    def get_positions(self) -> list[dict[str, Any]]:
        return self._extract_items(self._request('get_positions'), keys=('items', 'rows', 'data', 'positions'))

    def get_cash(self) -> dict[str, Any]:
        result = self._request('get_cash')
        body = result.get('body', {})
        if isinstance(body, list) and body and isinstance(body[0], dict):
            return body[0]
        return body if isinstance(body, dict) else {'raw': body}

    def disconnect(self) -> dict[str, Any]:
        return {'ok': True, 'status': 'disconnect_noop_for_rest_adapter', 'requested_at': now_str()}

    def poll_callbacks(self, cursor: str | None = None):
        params = {'cursor': cursor} if cursor else None
        return self._extract_items(self._request('poll_callbacks', params=params), keys=('items', 'rows', 'data', 'events', 'callbacks'))

    def export_broker_snapshot(self) -> dict[str, Any]:
        return {
            'generated_at': now_str(),
            'provider_name': self._config.get('provider_name', ''),
            'orders': self.get_open_orders(),
            'fills': self.get_fills(),
            'positions': self.get_positions(),
            'cash': self.get_cash(),
        }
