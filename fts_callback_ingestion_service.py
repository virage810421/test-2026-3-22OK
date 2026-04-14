# -*- coding: utf-8 -*-
from __future__ import annotations

"""Broker callback ingestion service.

Normalizes raw broker SDK/websocket/polling events, persists them as JSONL, and
optionally fans fill/order status events into the execution ledger.  It is safe
for paper_prelive and ready to sit behind a real adapter later.
"""

import json
from pathlib import Path
from typing import Any, Iterable

from fts_config import PATHS
from fts_utils import now_str
from fts_broker_callback_mapping import normalize_broker_callback
from fts_exception_policy import record_diagnostic


class CallbackIngestionService:
    def __init__(self):
        self.events_path = PATHS.runtime_dir / 'broker_callback_events.jsonl'
        self.summary_path = PATHS.runtime_dir / 'broker_callback_ingestion_summary.json'

    def _append_jsonl(self, row: dict[str, Any]) -> None:
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        with self.events_path.open('a', encoding='utf-8') as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + '\n')

    def ingest(self, events: Iterable[dict[str, Any]] | None, *, broker: str = 'GENERIC', account_id: str = '', fanout_ledger: bool = True) -> tuple[str, dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for event in list(events or []):
            try:
                row = normalize_broker_callback(event, broker=broker, account_id=account_id)
                row['ingested_at'] = now_str()
                normalized.append(row)
                self._append_jsonl(row)
            except Exception as exc:
                record_diagnostic('callback_ingestion', 'normalize_or_persist_callback_failed', exc, severity='error', fail_closed=True)
                errors.append({'error': repr(exc), 'event': str(event)[:500]})
        if fanout_ledger and normalized:
            self._fanout_to_ledger(normalized)
        payload = {
            'generated_at': now_str(),
            'status': 'callback_ingestion_ready' if not errors else 'callback_ingestion_with_errors',
            'events_path': str(self.events_path),
            'ingested_count': len(normalized),
            'error_count': len(errors),
            'errors': errors[-20:],
            'latest_callbacks': normalized[-20:],
        }
        self.summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        return str(self.summary_path), payload

    def _fanout_to_ledger(self, rows: list[dict[str, Any]]) -> None:
        try:
            from fts_execution_ledger import ExecutionLedger
            ledger = ExecutionLedger()
            for row in rows:
                event_type = str(row.get('event_type') or '').upper()
                status = str(row.get('status') or '').upper()
                if event_type == 'FILL' or status in {'FILLED', 'PARTIALLY_FILLED'}:
                    ledger.record_fill(row)
                else:
                    ledger.record_submission(row)
        except Exception as exc:
            record_diagnostic('callback_ingestion', 'ledger_fanout_failed', exc, severity='error', fail_closed=True)

    def ingest_from_broker(self, broker_obj: Any, *, clear: bool = True) -> tuple[str, dict[str, Any]]:
        if not callable(getattr(broker_obj, 'poll_callbacks', None)):
            return self.ingest([], broker=broker_obj.__class__.__name__ if broker_obj is not None else 'UNKNOWN')
        try:
            events = broker_obj.poll_callbacks(clear=clear)
        except TypeError:
            events = broker_obj.poll_callbacks()
        except Exception as exc:
            record_diagnostic('callback_ingestion', 'broker_poll_callbacks_failed', exc, severity='error', fail_closed=True)
            events = []
        return self.ingest(events, broker=broker_obj.__class__.__name__ if broker_obj is not None else 'UNKNOWN')


def main(argv: list[str] | None = None) -> int:
    path, payload = CallbackIngestionService().ingest([])
    print(json.dumps({'status': payload.get('status'), 'path': path, 'ingested_count': payload.get('ingested_count')}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
