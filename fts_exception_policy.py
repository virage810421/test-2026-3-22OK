# -*- coding: utf-8 -*-
"""Exception policy utilities for formal trading system.

Policy:
- Core trading / execution path: fail-closed and write runtime diagnostics.
- ETL / research / legacy wrappers: may fail-open, but must write diagnostics.

This module is intentionally dependency-light so it can be imported by early boot,
execution, governance, and guard services without circular dependencies.
"""
from __future__ import annotations

import ast
import json
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, TypeVar

T = TypeVar('T')
RUNTIME_DIR = Path('runtime')
DIAGNOSTIC_PATH = RUNTIME_DIR / 'exception_policy_runtime.json'

CORE_COMPONENTS = {
    'live_paper_trading',
    'execution_layer',
    'execution_runtime',
    'execution_ledger',
    'broker_adapter',
    'model_layer',
    'system_guard',
    'training_governance',
}


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except Exception:
        return str(value)


def record_diagnostic(
    component: str,
    operation: str,
    exc: BaseException | None = None,
    *,
    severity: str = 'warning',
    fail_closed: bool | None = None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append a structured runtime diagnostic and return the written entry."""
    comp = str(component or 'unknown')
    if fail_closed is None:
        fail_closed = comp in CORE_COMPONENTS
    entry = {
        'ts': datetime.now().isoformat(timespec='seconds'),
        'component': comp,
        'operation': str(operation or 'unknown'),
        'severity': str(severity or 'warning'),
        'policy': 'fail_closed' if fail_closed else 'fail_open_with_diagnostics',
        'exception_type': type(exc).__name__ if exc is not None else None,
        'message': str(exc) if exc is not None else '',
        'context': _json_safe(context or {}),
    }
    if exc is not None and severity in {'error', 'critical'}:
        entry['traceback_tail'] = traceback.format_exc(limit=5)
    try:
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        payload = {'updated_at': entry['ts'], 'events': []}
        if DIAGNOSTIC_PATH.exists():
            try:
                old = json.loads(DIAGNOSTIC_PATH.read_text(encoding='utf-8'))
                if isinstance(old, dict):
                    payload['events'] = list(old.get('events', []))[-200:]
            except Exception:
                payload['events'] = []
        payload['events'].append(entry)
        payload['summary'] = {
            'total_events': len(payload['events']),
            'fail_closed_events': sum(1 for x in payload['events'] if x.get('policy') == 'fail_closed'),
            'last_component': comp,
            'last_operation': entry['operation'],
        }
        DIAGNOSTIC_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        # Never let diagnostics logging crash the caller.
        pass
    return entry


def fail_closed_reason(component: str, operation: str, exc: BaseException | None = None, context: dict[str, Any] | None = None) -> str:
    record_diagnostic(component, operation, exc, severity='error', fail_closed=True, context=context)
    if exc is None:
        return f'{component}.{operation}:fail_closed'
    return f'{component}.{operation}:fail_closed:{type(exc).__name__}'


def guarded_call(
    component: str,
    operation: str,
    fn: Callable[[], T],
    *,
    default: T,
    fail_closed: bool | None = None,
    severity: str = 'warning',
    context: dict[str, Any] | None = None,
) -> T:
    try:
        return fn()
    except Exception as exc:
        record_diagnostic(component, operation, exc, severity=severity, fail_closed=fail_closed, context=context)
        return default

CORE_FILE_HINTS = (
    'live_paper_trading.py', 'fts_execution_layer.py', 'fts_execution_runtime.py',
    'fts_execution_ledger.py', 'fts_broker_api_adapter.py', 'fts_broker_real_stub.py',
    'fts_model_layer.py', 'fts_system_guard_service.py', 'system_guard.py',
    'fts_training_governance_mainline.py', 'fts_control_tower.py',
)
POLICY_TOKENS = ('record_diagnostic', 'record_issue', 'fail_closed_reason', 'guarded_call', '_diag(')


def _handler_has_policy(handler: ast.ExceptHandler, source: str) -> bool:
    try:
        segment = ast.get_source_segment(source, handler) or ''
    except Exception:
        segment = ''
    return ('pragma: no cover' in segment) or any(tok in segment for tok in POLICY_TOKENS)


def audit_exception_policy(project_root: str | Path = '.') -> dict[str, Any]:
    """Static audit for broad exceptions / pass / fallback usage.

    Core trading files must classify broad exceptions with diagnostics/fail-closed.
    ETL/research/legacy files may remain fail-open, but are counted for visibility.
    """
    root = Path(project_root)
    files = [p for p in root.rglob('*.py') if '__pycache__' not in p.parts and '.git' not in p.parts]
    summary: dict[str, Any] = {
        'files_total': len(files),
        'except_exception_total': 0,
        'pass_total': 0,
        'fallback_token_total': 0,
        'core_unclassified_except': [],
        'core_policy_ready': True,
    }
    by_file: dict[str, Any] = {}
    for p in files:
        rel = str(p.relative_to(root))
        text = p.read_text(encoding='utf-8', errors='ignore')
        except_count = text.count('except Exception')
        pass_count = text.count('pass')
        fallback_count = text.lower().count('fallback')
        summary['except_exception_total'] += except_count
        summary['pass_total'] += pass_count
        summary['fallback_token_total'] += fallback_count
        if not except_count and not pass_count and not fallback_count:
            continue
        core = p.name in CORE_FILE_HINTS or p.name.startswith('fts_execution') or p.name.startswith('fts_broker')
        unclassified: list[int] = []
        try:
            tree = ast.parse(text)
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler):
                    typ = node.type
                    is_broad = typ is None or (isinstance(typ, ast.Name) and typ.id == 'Exception')
                    if is_broad and core and not _handler_has_policy(node, text):
                        unclassified.append(getattr(node, 'lineno', -1))
        except Exception:
            pass
        if unclassified:
            summary['core_unclassified_except'].append({'file': rel, 'lines': unclassified[:20], 'count': len(unclassified)})
        by_file[rel] = {
            'core_file': core,
            'except_exception': except_count,
            'pass': pass_count,
            'fallback': fallback_count,
            'unclassified_core_except_lines': unclassified[:20],
        }
    summary['core_policy_ready'] = len(summary['core_unclassified_except']) == 0
    return {'summary': summary, 'by_file': by_file}
