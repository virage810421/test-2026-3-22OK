# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import traceback
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from fts_config import PATHS, CONFIG  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
    class _Config:
        runtime_diagnostics_enabled = True
        runtime_diagnostics_fail_closed_components = ['exit_ai','execution_sql','broker_callback','regime','feature_service','protective_stop']
        runtime_diagnostics_jsonl = 'runtime_diagnostics_events.jsonl'
        runtime_diagnostics_summary = 'runtime_diagnostics_summary.json'
    PATHS = _Paths()
    CONFIG = _Config()

_EVENTS: list[dict[str, Any]] = []
_COUNTER: Counter[str] = Counter()


def _runtime_dir() -> Path:
    p = Path(getattr(PATHS, 'runtime_dir', 'runtime'))
    p.mkdir(parents=True, exist_ok=True)
    return p


def _json_default(v: Any) -> str:
    try:
        return str(v)
    except Exception:
        return '<unserializable>'


def is_fail_closed_component(component: str) -> bool:
    raw = getattr(CONFIG, 'runtime_diagnostics_fail_closed_components', [])
    if isinstance(raw, str):
        items = [x.strip() for x in raw.split(',') if x.strip()]
    else:
        items = [str(x).strip() for x in (raw or []) if str(x).strip()]
    component_l = str(component or '').lower()
    return any(x.lower() in component_l for x in items)


def record_issue(
    component: str,
    operation: str,
    exc: BaseException | str | None = None,
    *,
    severity: str = 'WARNING',
    fail_mode: str = 'fail_open',
    context: dict[str, Any] | None = None,
    hard_block: bool | None = None,
) -> dict[str, Any]:
    """Record a runtime issue instead of silently swallowing exceptions.

    severity: INFO/WARNING/ERROR/CRITICAL
    fail_mode: fail_open/fail_closed/degraded
    hard_block: True when downstream trading/execution should be blocked.
    """
    if not bool(getattr(CONFIG, 'runtime_diagnostics_enabled', True)):
        return {}
    if hard_block is None:
        hard_block = (str(fail_mode).lower() == 'fail_closed') or is_fail_closed_component(component)
    event = {
        'time': datetime.now().isoformat(timespec='seconds'),
        'component': str(component),
        'operation': str(operation),
        'severity': str(severity).upper(),
        'fail_mode': str(fail_mode),
        'hard_block': bool(hard_block),
        'error_type': type(exc).__name__ if isinstance(exc, BaseException) else None,
        'error': repr(exc) if isinstance(exc, BaseException) else (str(exc) if exc is not None else ''),
        'context': context or {},
    }
    if isinstance(exc, BaseException) and str(event['severity']).upper() in {'ERROR','CRITICAL'}:
        event['traceback_tail'] = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-2500:]
    _EVENTS.append(event)
    _COUNTER[f"{event['component']}::{event['operation']}::{event['severity']}"] += 1
    try:
        rt = _runtime_dir()
        jsonl_name = getattr(CONFIG, 'runtime_diagnostics_jsonl', 'runtime_diagnostics_events.jsonl')
        with (rt / jsonl_name).open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(event, ensure_ascii=False, default=_json_default) + '\n')
        write_summary()
    except Exception:
        # Last-resort only; do not recursively fail diagnostics.
        pass
    try:
        print(f"⚠️ runtime diagnostic | {event['severity']} | {event['component']}.{event['operation']} | {event['fail_mode']} | {event['error']}")
    except Exception:
        pass
    return event


def write_summary(extra: dict[str, Any] | None = None) -> Path | None:
    try:
        rt = _runtime_dir()
        summary_name = getattr(CONFIG, 'runtime_diagnostics_summary', 'runtime_diagnostics_summary.json')
        hard_blocks = [e for e in _EVENTS if e.get('hard_block')]
        payload = {
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'event_count': len(_EVENTS),
            'hard_block_count': len(hard_blocks),
            'warning_count': sum(1 for e in _EVENTS if e.get('severity') == 'WARNING'),
            'error_count': sum(1 for e in _EVENTS if e.get('severity') in {'ERROR','CRITICAL'}),
            'counters': dict(_COUNTER),
            'recent_events': _EVENTS[-50:],
            'hard_blocks': hard_blocks[-50:],
            'extra': extra or {},
        }
        p = rt / summary_name
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding='utf-8')
        return p
    except Exception:
        return None


def get_summary() -> dict[str, Any]:
    hard_blocks = [e for e in _EVENTS if e.get('hard_block')]
    return {
        'event_count': len(_EVENTS),
        'hard_block_count': len(hard_blocks),
        'counters': dict(_COUNTER),
        'recent_events': _EVENTS[-20:],
    }
