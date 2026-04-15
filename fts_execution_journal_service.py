# -*- coding: utf-8 -*-
from __future__ import annotations

"""交易員級 execution journal v92.

把訊號、被擋單、通過 Gate、paper order/fill、持倉生命週期建議都寫成 JSONL。
核心目的：每一筆「為什麼下 / 不下 / 減碼 / 出場」都可追。
"""

import json
from datetime import datetime, date
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS


def _json_safe(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, (list, dict)):
        return v
    try:
        import numpy as np  # type: ignore
        if isinstance(v, (np.integer, np.floating)):
            return float(v)
    except Exception:
        pass
    return str(v)


def append_execution_journal_event(event_type: str, **kwargs: Any) -> dict[str, Any]:
    """Append one normalized execution journal event.

    This helper is intentionally dependency-light so every order path can call it.
    It never raises; failing to journal returns ok=False.
    """
    try:
        PATHS.runtime_dir.mkdir(parents=True, exist_ok=True)
        path = PATHS.runtime_dir / 'execution_journal.jsonl'
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        today = date.today().isoformat()
        ticker = str(kwargs.get('ticker') or kwargs.get('symbol') or '').strip().upper()
        oid = str(kwargs.get('broker_order_id') or kwargs.get('order_id') or kwargs.get('client_order_id') or '')
        status = str(kwargs.get('status') or '')
        side = str(kwargs.get('side') or kwargs.get('action') or '')
        qty_raw = kwargs.get('qty', kwargs.get('target_qty', kwargs.get('filled_qty', 0)))
        try:
            qty = int(float(qty_raw or 0))
        except Exception:
            qty = 0
        journal_key = kwargs.get('journal_key') or '|'.join([
            today, str(event_type), ticker, oid, status, side, str(qty), str(kwargs.get('timestamp') or now)
        ])
        event = {
            'journal_key': str(journal_key),
            'date': today,
            'generated_at': now,
            'event_type': str(event_type),
            'ticker': ticker,
            'broker_order_id': oid,
            'client_order_id': str(kwargs.get('client_order_id') or ''),
            'status': status,
            'side': side,
            'qty': qty,
            'price': _json_safe(kwargs.get('price', kwargs.get('reference_price', kwargs.get('fill_price', kwargs.get('avg_fill_price'))))),
            'reason': str(kwargs.get('reason') or kwargs.get('note') or kwargs.get('reject_reason') or kwargs.get('block_reason') or ''),
            'strategy_name': str(kwargs.get('strategy_name') or ''),
            'signal_id': str(kwargs.get('signal_id') or ''),
            'source': str(kwargs.get('source') or 'execution_path'),
            'raw': {str(k): _json_safe(v) for k, v in kwargs.items()},
        }
        with path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(event, ensure_ascii=False) + '\n')
        return {'ok': True, 'path': str(path), 'event_type': str(event_type), 'journal_key': event['journal_key']}
    except Exception as exc:
        return {'ok': False, 'error': repr(exc), 'event_type': str(event_type)}


class ExecutionJournalService:
    MODULE_VERSION = 'v92_execution_journal_closed_loop'

    def __init__(self) -> None:
        self.jsonl_path = PATHS.runtime_dir / 'execution_journal.jsonl'
        self.summary_path = PATHS.runtime_dir / 'execution_journal_summary.json'

    def _read_csv_first(self, candidates: list[Path]) -> tuple[Path | None, pd.DataFrame]:
        for p in candidates:
            if p.exists():
                try:
                    df = pd.read_csv(p)
                    if not df.empty:
                        return p, df
                except Exception:
                    continue
        return None, pd.DataFrame()

    def _load_decisions(self) -> tuple[Path | None, pd.DataFrame]:
        return self._read_csv_first([
            PATHS.data_dir / 'normalized_decision_output_enriched.csv',
            PATHS.data_dir / 'normalized_decision_output.csv',
            PATHS.base_dir / 'daily_decision_desk.csv',
            PATHS.data_dir / 'daily_decision_desk.csv',
        ])

    def _existing_keys(self) -> set[str]:
        keys: set[str] = set()
        if not self.jsonl_path.exists():
            return keys
        try:
            for line in self.jsonl_path.read_text(encoding='utf-8').splitlines()[-10000:]:
                if not line.strip():
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict) and obj.get('journal_key'):
                    keys.add(str(obj['journal_key']))
        except Exception:
            return keys
        return keys

    @staticmethod
    def _ticker(row: pd.Series) -> str:
        return str(row.get('Ticker') or row.get('Ticker SYMBOL') or row.get('ticker') or '').strip().upper()

    @staticmethod
    def _bool(row: pd.Series, name: str, default: bool = False) -> bool:
        if name not in row:
            return default
        v = row.get(name)
        if isinstance(v, str):
            return v.strip().lower() in {'1', 'true', 'yes', 'y', '是', '可', 'yes'}
        return bool(v)

    @staticmethod
    def _num(row: pd.Series, names: list[str], default: float = 0.0) -> float:
        for name in names:
            if name in row and pd.notna(row.get(name)):
                try:
                    return float(row.get(name))
                except Exception:
                    pass
        return default

    def _row_to_event(self, row: pd.Series, today: str) -> dict[str, Any] | None:
        ticker = self._ticker(row)
        if not ticker:
            return None
        direction = str(row.get('Direction') or row.get('direction') or row.get('Action') or '').strip()
        action_text = ' '.join(str(row.get(c, '')) for c in [
            'Action', 'Entry_State', 'Entry_Action', 'Exit_State', 'Exit_Action',
            'Golden_Type', 'Decision', '今日系統燈號', 'System_Light'
        ])
        execution_eligible = self._bool(row, 'ExecutionEligible', False) or self._bool(row, 'CanAutoSubmit', False)
        desk_usable = self._bool(row, 'DeskUsable', not self._bool(row, 'FallbackBuild', False))
        fallback = self._bool(row, 'FallbackBuild', False)
        qty = int(max(0, round(self._num(row, ['Target_Qty', 'TargetQty', 'qty'], 0.0))))
        ref_price = self._num(row, ['Reference_Price', 'Close', 'Current_Close', 'ref_price'], 0.0)
        market_rule_passed = self._bool(row, 'MarketRulePassed', True) if 'MarketRulePassed' in row else True
        block_reasons = []
        if fallback:
            block_reasons.append('fallback_row')
        if not desk_usable:
            block_reasons.append('desk_not_usable')
        if not execution_eligible:
            block_reasons.append('execution_not_eligible')
        if qty <= 0:
            block_reasons.append('target_qty_zero')
        if ref_price <= 0:
            block_reasons.append('reference_price_invalid')
        if not market_rule_passed:
            block_reasons.append('market_rule_failed')

        upper = action_text.upper()
        event_type = 'ORDER_CANDIDATE' if not block_reasons else 'NO_ORDER_BLOCKED'
        if 'REDUCE' in upper or '減碼' in action_text:
            event_type = 'REDUCE_DECISION' if not block_reasons else 'REDUCE_BLOCKED'
        if 'EXIT' in upper or '出場' in action_text:
            event_type = 'EXIT_DECISION' if not block_reasons else 'EXIT_BLOCKED'
        if 'DEFEND' in upper or '防守' in action_text:
            event_type = 'DEFEND_DECISION' if not block_reasons else 'DEFEND_BLOCKED'
        if 'PREPARE' in upper or '布局' in action_text or '觀察' in action_text:
            event_type = 'PREPARE_TRACKED' if not block_reasons else 'PREPARE_BLOCKED'
        if 'PILOT' in upper or '試單' in action_text:
            event_type = 'PILOT_ENTRY_TRACKED' if not block_reasons else 'PILOT_ENTRY_BLOCKED'

        reason = str(row.get('Reason') or row.get('觸發條件明細') or row.get('Golden_Type') or row.get('Setup_Tag') or row.get('Structure') or '')
        journal_key = f'{today}|{ticker}|{event_type}|{direction}|{qty}|{ref_price}'
        return {
            'journal_key': journal_key,
            'date': today,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'event_type': event_type,
            'ticker': ticker,
            'direction': direction,
            'target_qty': qty,
            'reference_price': ref_price,
            'execution_eligible': execution_eligible,
            'desk_usable': desk_usable,
            'fallback_build': fallback,
            'market_rule_passed': market_rule_passed,
            'block_reasons': block_reasons,
            'decision_reason': reason[:500],
            'raw_action_text': action_text[:300],
            'source': 'decision_desk_scan',
        }

    def build(self) -> tuple[Path, dict[str, Any]]:
        source, df = self._load_decisions()
        today = date.today().isoformat()
        existing = self._existing_keys()
        new_events: list[dict[str, Any]] = []
        if not df.empty:
            for _, row in df.iterrows():
                event = self._row_to_event(row, today)
                if not event or event['journal_key'] in existing:
                    continue
                new_events.append(event)
                existing.add(event['journal_key'])
        self.jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        if new_events:
            with self.jsonl_path.open('a', encoding='utf-8') as fh:
                for e in new_events:
                    fh.write(json.dumps(e, ensure_ascii=False) + '\n')
        all_count = 0
        if self.jsonl_path.exists():
            try:
                all_count = sum(1 for line in self.jsonl_path.read_text(encoding='utf-8').splitlines() if line.strip())
            except Exception:
                all_count = 0
        summary = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'module_version': self.MODULE_VERSION,
            'status': 'execution_journal_ready' if not df.empty else 'waiting_for_decision_output',
            'source': str(source) if source else None,
            'journal_jsonl': str(self.jsonl_path),
            'total_event_count_estimate': all_count,
            'new_event_count': len(new_events),
            'new_order_candidate_count': sum(1 for e in new_events if e['event_type'] == 'ORDER_CANDIDATE'),
            'new_blocked_count': sum(1 for e in new_events if 'BLOCKED' in e['event_type'] or e['event_type'] == 'NO_ORDER_BLOCKED'),
            'new_exit_reduce_defend_count': sum(1 for e in new_events if e['event_type'].startswith(('EXIT', 'REDUCE', 'DEFEND'))),
            'closed_loop_policy': {
                'decision_rows_journaled': True,
                'blocked_rows_journaled': True,
                'control_tower_gate_events_expected': True,
                'paper_broker_order_and_fill_events_expected': True,
            },
            'latest_events': new_events[-50:],
        }
        self.summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.summary_path, summary


def main() -> int:
    path, payload = ExecutionJournalService().build()
    print(f'🧾 execution journal 完成：{path} | status={payload.get("status")} new={payload.get("new_event_count")}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
