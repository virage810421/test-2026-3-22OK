# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, append_jsonl, write_json
from fts_callback_event_schema import CallbackEventSchema


class CallbackEventStore:
    def __init__(self):
        self.schema = CallbackEventSchema()
        self.events_path = PATHS.runtime_dir / 'callback_events.jsonl'
        self.summary_path = PATHS.runtime_dir / 'callback_event_store_summary.json'

    def record(self, raw_event: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        event = self.schema.normalize(raw_event)
        envelope = {
            'recorded_at': now_str(),
            'event': event,
        }
        append_jsonl(self.events_path, envelope)
        summary = {
            'generated_at': now_str(),
            'status': 'callback_event_recorded' if event.get('valid') else 'callback_event_invalid_recorded',
            'last_event_valid': bool(event.get('valid')),
            'last_event_type': event.get('event_type', ''),
            'last_event_status': event.get('status', ''),
            'path': str(self.events_path),
        }
        write_json(self.summary_path, summary)
        return str(self.summary_path), summary
