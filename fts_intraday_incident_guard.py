# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, write_json


class IntradayIncidentGuard:
    def evaluate(self, *, broker_connected: bool, callback_lag_seconds: int, reject_rate: float, day_loss_pct: float) -> tuple[str, dict[str, Any]]:
        alerts = []
        if not broker_connected:
            alerts.append('broker_disconnected')
        if callback_lag_seconds > 30:
            alerts.append('callback_lag_too_high')
        if reject_rate > 0.20:
            alerts.append('reject_rate_too_high')
        if day_loss_pct <= -0.03:
            alerts.append('daily_loss_limit_hit')
        payload = {
            'generated_at': now_str(),
            'status': 'incident_guard_block' if alerts else 'incident_guard_ok',
            'alerts': alerts,
            'kill_switch_recommended': len(alerts) > 0,
        }
        path = PATHS.runtime_dir / 'intraday_incident_guard.json'
        write_json(path, payload)
        return str(path), payload
