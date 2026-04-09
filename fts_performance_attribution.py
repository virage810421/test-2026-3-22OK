# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, safe_float, write_json


class PerformanceAttributionBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'performance_attribution.json'
        self.md_path = PATHS.runtime_dir / 'performance_attribution.md'

    def _bucket(self, trades: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
        buckets = defaultdict(lambda: {'count': 0, 'pnl': 0.0, 'win_count': 0})
        for row in trades:
            key = str(row.get(field, 'UNKNOWN') or 'UNKNOWN')
            pnl = safe_float(row.get('pnl', row.get('淨損益金額', 0.0)), 0.0)
            buckets[key]['count'] += 1
            buckets[key]['pnl'] += pnl
            buckets[key]['win_count'] += 1 if pnl > 0 else 0
        out = []
        for key, item in buckets.items():
            out.append({
                'bucket': key,
                'count': item['count'],
                'pnl': round(item['pnl'], 2),
                'win_rate': round(item['win_count'] / item['count'], 4) if item['count'] else 0.0,
            })
        return sorted(out, key=lambda x: (x['pnl'], x['count']), reverse=True)

    def build(self, trades: list[dict[str, Any]], rejected_orders: list[dict[str, Any]] | None = None) -> tuple[Any, dict[str, Any]]:
        rejected_orders = rejected_orders or []
        total_pnl = round(sum(safe_float(x.get('pnl', x.get('淨損益金額', 0.0)), 0.0) for x in trades), 2)
        total_slippage = round(sum(safe_float(x.get('slippage_cost', 0.0), 0.0) for x in trades), 2)
        total_commission = round(sum(safe_float(x.get('commission', 0.0), 0.0) for x in trades), 2)
        total_tax = round(sum(safe_float(x.get('tax', 0.0), 0.0) for x in trades), 2)
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'summary': {
                'trade_count': len(trades),
                'total_pnl': total_pnl,
                'total_slippage_cost': total_slippage,
                'total_commission': total_commission,
                'total_tax': total_tax,
                'reject_count': len(rejected_orders),
            },
            'by_regime': self._bucket(trades, 'regime'),
            'by_setup': self._bucket(trades, 'setup'),
            'by_industry': self._bucket(trades, 'industry'),
            'reject_reasons': self._bucket(rejected_orders, 'reason') if rejected_orders else [],
            'status': 'attribution_ready',
        }
        write_json(self.path, payload)
        lines = [
            '# Performance Attribution',
            '',
            f"- trade_count: {payload['summary']['trade_count']}",
            f"- total_pnl: {payload['summary']['total_pnl']}",
            f"- total_slippage_cost: {payload['summary']['total_slippage_cost']}",
            f"- total_commission: {payload['summary']['total_commission']}",
            f"- total_tax: {payload['summary']['total_tax']}",
            f"- reject_count: {payload['summary']['reject_count']}",
            '',
            '## By Regime',
        ]
        for row in payload['by_regime'][:20]:
            lines.append(f"- {row['bucket']} | count={row['count']} | pnl={row['pnl']} | win_rate={row['win_rate']}")
        lines.append('')
        lines.append('## By Setup')
        for row in payload['by_setup'][:20]:
            lines.append(f"- {row['bucket']} | count={row['count']} | pnl={row['pnl']} | win_rate={row['win_rate']}")
        lines.append('')
        lines.append('## By Industry')
        for row in payload['by_industry'][:20]:
            lines.append(f"- {row['bucket']} | count={row['count']} | pnl={row['pnl']} | win_rate={row['win_rate']}")
        self.md_path.write_text('\n'.join(lines), encoding='utf-8')
        log(f'📈 已輸出 performance attribution：{self.path}')
        return self.path, payload
