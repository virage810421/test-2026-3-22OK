# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, write_json


class DailyOpsSummaryBuilder:
    def __init__(self):
        self.summary_path = PATHS.runtime_dir / 'daily_ops_summary.json'
        self.alerts_path = PATHS.runtime_dir / 'alerts.json'
        self.md_path = PATHS.runtime_dir / 'daily_ops_summary.md'

    def _flag_alerts(self, dashboard: dict[str, Any]):
        alerts = []
        heartbeat = dashboard.get('heartbeat', {})
        hb_stage = heartbeat.get('stage')
        if hb_stage == 'crash':
            alerts.append({'level': 'critical', 'type': 'heartbeat_crash', 'message': 'heartbeat 顯示上次執行發生 crash'})
        retry = dashboard.get('retry_queue_summary', {})
        if retry.get('pending_retry', 0) > 0:
            alerts.append({'level': 'warning', 'type': 'pending_retry', 'message': f"retry queue 尚有 {retry.get('pending_retry', 0)} 筆待補跑"})
        upstream_exec = dashboard.get('upstream_exec', {})
        if len(upstream_exec.get('failed', [])) > 0:
            alerts.append({'level': 'warning', 'type': 'upstream_failed', 'message': f"本輪上游任務失敗 {len(upstream_exec.get('failed', []))} 筆"})
        execution_result = dashboard.get('execution_result', {})
        if execution_result.get('rejected', 0) > 0:
            alerts.append({'level': 'info', 'type': 'rejected_orders', 'message': f"本輪有 {execution_result.get('rejected', 0)} 筆委託被拒"})
        return alerts

    def build(self, dashboard: dict[str, Any], candidates: list[dict[str, Any]] | None = None, blacklist: list[str] | None = None, risk_usage: dict[str, Any] | None = None, order_board: dict[str, Any] | None = None, close_notes: list[str] | None = None):
        candidates = candidates or []
        blacklist = blacklist or []
        risk_usage = risk_usage or {}
        order_board = order_board or {}
        close_notes = close_notes or []
        alerts = self._flag_alerts(dashboard)
        summary = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'mode': CONFIG.mode,
            'broker_type': CONFIG.broker_type,
            'headline': {
                'pending_retry': dashboard.get('retry_queue_summary', {}).get('pending_retry', 0),
                'signals': dashboard.get('execution_readiness', {}).get('total_signals', 0),
                'filled': dashboard.get('execution_result', {}).get('filled', 0),
                'partial': dashboard.get('execution_result', {}).get('partially_filled', 0),
                'positions': dashboard.get('positions_summary', {}).get('count', 0),
                'alerts': len(alerts),
            },
            'today_candidates': candidates[:50],
            'blacklist': blacklist[:50],
            'risk_usage': risk_usage,
            'order_board': order_board,
            'alerts': alerts,
            'close_notes': close_notes,
        }
        write_json(self.summary_path, summary)
        write_json(self.alerts_path, {'generated_at': now_str(), 'alerts': alerts})
        lines = [
            f'# Daily Ops Summary | {CONFIG.system_name}',
            '',
            f"- Signals: {summary['headline']['signals']}",
            f"- Filled: {summary['headline']['filled']}",
            f"- Partial: {summary['headline']['partial']}",
            f"- Positions: {summary['headline']['positions']}",
            f"- Alerts: {summary['headline']['alerts']}",
            '',
            '## 今日候選',
        ]
        for row in candidates[:20]:
            lines.append(f"- {row.get('ticker', row.get('Ticker', ''))} | score={row.get('score', row.get('Score', ''))} | regime={row.get('regime', row.get('Regime', ''))}")
        lines.append('')
        lines.append('## 禁買清單')
        for item in blacklist[:20]:
            lines.append(f'- {item}')
        lines.append('')
        lines.append('## 收盤檢討')
        for note in close_notes[:20]:
            lines.append(f'- {note}')
        self.md_path.write_text('\n'.join(lines), encoding='utf-8')
        log(f'📝 已輸出 daily ops summary：{self.summary_path}')
        log(f'🚨 已輸出 alerts：{self.alerts_path}')
        return self.summary_path, self.alerts_path, summary
