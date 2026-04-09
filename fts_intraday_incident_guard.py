# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, write_json


class IntradayIncidentGuard:
    def evaluate(self, *, broker_connected: bool, callback_lag_seconds: int, reject_rate: float, day_loss_pct: float, stale_price_symbols: list[str] | None = None, orphan_order_count: int = 0) -> tuple[str, dict[str, Any]]:
        stale_price_symbols = stale_price_symbols or []
        alerts = []
        severity = 'ok'
        if not broker_connected:
            alerts.append('broker_disconnected')
            severity = 'critical'
        if callback_lag_seconds > 30:
            alerts.append('callback_lag_too_high')
            severity = 'critical'
        elif callback_lag_seconds > 5:
            alerts.append('callback_lag_warning')
            severity = 'warn'
        if reject_rate > 0.20:
            alerts.append('reject_rate_too_high')
            severity = 'critical'
        elif reject_rate > 0.05:
            alerts.append('reject_rate_warning')
            severity = max(severity, 'warn')
        if day_loss_pct <= -0.03:
            alerts.append('daily_loss_limit_hit')
            severity = 'critical'
        elif day_loss_pct <= -0.015:
            alerts.append('daily_loss_warning')
            severity = max(severity, 'warn')
        if stale_price_symbols:
            alerts.append('stale_price_symbols_present')
            if severity == 'ok':
                severity = 'warn'
        if orphan_order_count > 0:
            alerts.append('orphan_orders_present')
            if severity == 'ok':
                severity = 'warn'
        payload = {
            'generated_at': now_str(),
            'status': 'incident_guard_block' if severity == 'critical' else ('incident_guard_warn' if severity == 'warn' else 'incident_guard_ok'),
            'severity': severity,
            'alerts': alerts,
            'metrics': {
                'broker_connected': broker_connected,
                'callback_lag_seconds': callback_lag_seconds,
                'reject_rate': round(float(reject_rate), 6),
                'day_loss_pct': round(float(day_loss_pct), 6),
                'stale_price_symbols': stale_price_symbols,
                'orphan_order_count': int(orphan_order_count),
            },
            'kill_switch_recommended': severity == 'critical',
        }
        path = PATHS.runtime_dir / 'intraday_incident_guard.json'
        write_json(path, payload)
        return str(path), payload
