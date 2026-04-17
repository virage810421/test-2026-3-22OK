# -*- coding: utf-8 -*-
from __future__ import annotations

"""Shadow runtime evidence builder.

Hardening v20260417b:
- Decision gate rows are planning evidence only; they no longer count as shadow
  runtime evidence.
- A promotion pass must be backed by execution ledger / fills / TWAP3 callbacks /
  broker callbacks / execution journal events.
- If the system has only planning outputs, the file explicitly reports
  ``shadow_runtime_planning_only`` so promotion cannot accidentally pass.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from fts_config import PATHS, CONFIG  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
        state_dir = Path('state')
    PATHS = _Paths()
    class _Config:
        system_name = 'formal_trading_system'
    CONFIG = _Config()

try:
    from fts_exception_policy import record_diagnostic  # type: ignore
except Exception:  # pragma: no cover
    def record_diagnostic(*args, **kwargs):
        return None


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value in (None, ''):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


def _read_json(path: Path) -> Any:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception as exc:
        record_diagnostic('shadow_runtime_evidence', f'read_json_failed_{path.name}', exc, severity='warning', fail_closed=False)
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    tmp.replace(path)


def _rows_from_payload(data: Any, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    rows: list[dict[str, Any]] = []
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            rows.extend([x for x in value if isinstance(x, dict)])
    return rows


def _status(row: dict[str, Any]) -> str:
    return str(row.get('status') or row.get('order_status') or row.get('event_type') or '').upper().strip()


class ShadowRuntimeEvidenceBuilder:
    MODULE_VERSION = 'v20260417b_shadow_runtime_evidence_truthful_hard_gate'

    def __init__(self, output_path: Path | None = None) -> None:
        self.output_path = Path(output_path or Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'shadow_runtime_evidence.json')

    def build(self) -> tuple[Path, dict[str, Any]]:
        runtime_dir = Path(getattr(PATHS, 'runtime_dir', Path('runtime')))
        ledger = _read_json(runtime_dir / 'execution_ledger_summary.json')
        journal = _read_json(runtime_dir / 'execution_journal_summary.json')
        decision_gate = _read_json(runtime_dir / 'decision_execution_formal_gate.json')
        twap_state = _read_json(runtime_dir / 'twap3_child_order_state.json')
        twap_summary = _read_json(runtime_dir / 'twap3_child_order_summary.json')
        twap_queue = _read_json(runtime_dir / 'twap3_broker_submission_queue.json')
        broker_closure = _read_json(runtime_dir / 'true_broker_live_closure.json')
        live_safety = _read_json(runtime_dir / 'live_safety_gate.json')

        ledger_orders = _rows_from_payload(ledger, ('orders',))
        ledger_fills = _rows_from_payload(ledger, ('fills',))
        twap_children: list[dict[str, Any]] = []
        if isinstance(twap_state, dict):
            children = twap_state.get('children') or {}
            if isinstance(children, dict):
                twap_children = [x for x in children.values() if isinstance(x, dict)]
            elif isinstance(children, list):
                twap_children = [x for x in children if isinstance(x, dict)]
        twap_fills = [x for x in twap_children if _status(x) in {'FILLED', 'PARTIALLY_FILLED', 'PAPER_FILLED'} or _safe_float(x.get('filled_qty'), 0.0) > 0]
        callback_count = _safe_int(((broker_closure.get('callback_summary') or {}).get('ingested_count', 0) if isinstance(broker_closure, dict) else 0), 0)
        journal_events = _safe_int(journal.get('total_event_count_estimate', journal.get('event_count', 0)) if isinstance(journal, dict) else 0, 0)
        final_order_count = _safe_int(decision_gate.get('final_order_count', 0) if isinstance(decision_gate, dict) else 0, 0)
        queue_count = _safe_int(twap_queue.get('queue_count', 0) if isinstance(twap_queue, dict) else 0, 0)

        # Runtime evidence: actual execution artifacts only.  Decision output is
        # planning evidence and must not make promotion pass by itself.
        runtime_observation_count = len(ledger_orders) + len(ledger_fills) + len(twap_children) + callback_count + journal_events
        planning_evidence_count = final_order_count + queue_count
        filled_like_count = len(ledger_fills) + len(twap_fills)
        rejected_like_count = sum(1 for row in ledger_orders + twap_children if _status(row) in {'REJECTED', 'REJECT'})
        submitted_like_count = sum(
            1 for row in ledger_orders + twap_children
            if _status(row) in {'SUBMITTED', 'PENDING_SUBMIT', 'PARTIALLY_FILLED', 'FILLED', 'REJECTED', 'CANCELLED', 'CANCELED', 'PAPER_FILLED'}
        )
        reject_rate = (rejected_like_count / submitted_like_count) if submitted_like_count else None

        drift_candidates: list[float] = []
        for data in (journal, broker_closure, ledger, twap_summary):
            if not isinstance(data, dict):
                continue
            for key in ('shadow_return_drift_pct', 'return_drift_pct', 'paper_return_drift_pct', 'avg_shadow_drift_pct'):
                if key in data:
                    val = _safe_float(data.get(key), None)
                    if val is not None:
                        drift_candidates.append(float(val))
        runtime_drift = max(drift_candidates, default=None)

        live_safety_clear = True
        if isinstance(live_safety, dict) and live_safety:
            live_safety_clear = bool(live_safety.get('go_for_execution', live_safety.get('status') not in {'live_safety_blocked'}))

        runtime_observed = runtime_observation_count > 0
        planning_only = (not runtime_observed) and planning_evidence_count > 0
        if runtime_observed:
            status = 'shadow_runtime_evidence_ready'
        elif planning_only:
            status = 'shadow_runtime_planning_only'
        else:
            status = 'shadow_runtime_evidence_missing'

        payload = {
            'generated_at': _now(),
            'module_version': self.MODULE_VERSION,
            'system_name': getattr(CONFIG, 'system_name', 'formal_trading_system'),
            'status': status,
            'runtime_observed': bool(runtime_observed),
            'planning_only': bool(planning_only),
            'shadow_observation_count': int(runtime_observation_count),
            'planning_evidence_count': int(planning_evidence_count),
            'paper_like_activity_count': int(len(ledger_orders) + len(ledger_fills) + len(twap_children)),
            'ledger_order_count': int(len(ledger_orders)),
            'ledger_fill_count': int(len(ledger_fills)),
            'twap_child_count': int(len(twap_children)),
            'twap_queue_count': int(queue_count),
            'decision_final_order_count_planning_only': int(final_order_count),
            'callback_count': int(callback_count),
            'journal_event_count': int(journal_events),
            'filled_like_count': int(filled_like_count),
            'submitted_like_count': int(submitted_like_count),
            'rejected_like_count': int(rejected_like_count),
            'reject_rate': reject_rate,
            'shadow_return_drift_pct': runtime_drift,
            'live_safety_clear': live_safety_clear,
            'promotion_hard_gate': {
                'pass': bool(runtime_observed and live_safety_clear),
                'reason': 'runtime_evidence_ready' if runtime_observed and live_safety_clear else ('planning_only_not_runtime' if planning_only else 'runtime_evidence_missing'),
            },
            'sources': {
                'execution_ledger_summary': str(runtime_dir / 'execution_ledger_summary.json') if ledger else '',
                'execution_journal_summary': str(runtime_dir / 'execution_journal_summary.json') if journal else '',
                'decision_execution_formal_gate': str(runtime_dir / 'decision_execution_formal_gate.json') if decision_gate else '',
                'twap3_child_order_state': str(runtime_dir / 'twap3_child_order_state.json') if twap_state else '',
                'twap3_broker_submission_queue': str(runtime_dir / 'twap3_broker_submission_queue.json') if twap_queue else '',
                'true_broker_live_closure': str(runtime_dir / 'true_broker_live_closure.json') if broker_closure else '',
                'live_safety_gate': str(runtime_dir / 'live_safety_gate.json') if live_safety else '',
            },
            'truthful_rule': 'promotion/shadow pass 只能使用 execution ledger / fills / TWAP3 runtime / callback / journal；decision gate 訂單數只是 planning evidence。',
        }
        _write_json(self.output_path, payload)
        return self.output_path, payload


def build_shadow_runtime_evidence() -> tuple[Path, dict[str, Any]]:
    return ShadowRuntimeEvidenceBuilder().build()


def main(argv: list[str] | None = None) -> int:
    path, payload = build_shadow_runtime_evidence()
    print(json.dumps({'status': payload.get('status'), 'path': str(path), 'observations': payload.get('shadow_observation_count'), 'planning_only': payload.get('planning_only')}, ensure_ascii=False, indent=2))
    return 0 if payload.get('runtime_observed') else 1


if __name__ == '__main__':
    raise SystemExit(main())
