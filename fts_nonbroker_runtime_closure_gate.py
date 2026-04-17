# -*- coding: utf-8 -*-
from __future__ import annotations

"""Non-broker runtime closed-loop gate.

This module audits the parts that *can* be closed without real broker data:
- decision -> execution ledger evidence
- paper/TWAP3 plan -> child queue -> ledger/shadow evidence
- shadow runtime evidence is not planning-only
- feature review / full ablation gate is enforced
- reconciliation truthfully reports missing broker snapshots instead of passing
- kill switch state is visible

It intentionally separates ``nonbroker_closed_loop_ready`` from
``true_broker_closed_loop_ready``.  The latter still requires broker adapter,
callbacks, broker ledger snapshots and real broker reconciliation.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
        model_dir = Path('models')
    PATHS = _Paths()


def _now() -> str:
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    tmp.replace(path)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except Exception:
        return default


class NonBrokerRuntimeClosureGate:
    MODULE_VERSION = 'v20260417b_nonbroker_runtime_closed_loop_gate'

    def __init__(self) -> None:
        self.runtime_dir = Path(getattr(PATHS, 'runtime_dir', Path('runtime')))
        self.model_dir = Path(getattr(PATHS, 'model_dir', Path('models')))
        self.path = self.runtime_dir / 'nonbroker_runtime_closure_gate.json'

    def _run_builders(self) -> dict[str, Any]:
        ran: dict[str, Any] = {}
        try:
            from fts_execution_ledger import ExecutionLedger
            p, payload = ExecutionLedger().build_summary()
            ran['execution_ledger'] = {'status': 'ok', 'path': str(p), 'payload_status': payload.get('status') if isinstance(payload, dict) else None}
        except Exception as exc:
            ran['execution_ledger'] = {'status': 'error', 'error': repr(exc)}
        try:
            from fts_twap3_runtime_closure import TWAP3RuntimeClosure
            p, payload = TWAP3RuntimeClosure().build()
            ran['twap3_runtime_closure'] = {'status': 'ok', 'path': str(p), 'payload_status': payload.get('status') if isinstance(payload, dict) else None}
        except Exception as exc:
            ran['twap3_runtime_closure'] = {'status': 'error', 'error': repr(exc)}
        try:
            from fts_shadow_runtime_evidence import ShadowRuntimeEvidenceBuilder
            p, payload = ShadowRuntimeEvidenceBuilder().build()
            ran['shadow_runtime_evidence'] = {'status': 'ok', 'path': str(p), 'payload_status': payload.get('status') if isinstance(payload, dict) else None}
        except Exception as exc:
            ran['shadow_runtime_evidence'] = {'status': 'error', 'error': repr(exc)}
        try:
            from fts_reconciliation_runtime import build_from_runtime_files
            p, payload = build_from_runtime_files()
            ran['reconciliation_runtime'] = {'status': 'ok', 'path': str(p), 'payload_status': payload.get('status') if isinstance(payload, dict) else None}
        except Exception as exc:
            ran['reconciliation_runtime'] = {'status': 'error', 'error': repr(exc)}
        return ran

    def build(self) -> tuple[Path, dict[str, Any]]:
        ran = self._run_builders()
        ledger = _read_json(self.runtime_dir / 'execution_ledger_summary.json')
        twap = _read_json(self.runtime_dir / 'twap3_runtime_closure.json')
        queue = _read_json(self.runtime_dir / 'twap3_broker_submission_queue.json')
        shadow = _read_json(self.runtime_dir / 'shadow_runtime_evidence.json')
        recon = _read_json(self.runtime_dir / 'prelive_reconciliation_runtime.json')
        feature_review = _read_json(self.runtime_dir / 'feature_review_report.json')
        ablation = _read_json(self.runtime_dir / 'feature_ablation_report.json')
        decision_gate = _read_json(self.runtime_dir / 'decision_execution_formal_gate.json')
        model_role = _read_json(self.runtime_dir / 'model_role_router.json')
        kill_switch = _read_json(self.runtime_dir / 'kill_switch_state.json')

        final_order_count = _as_int(decision_gate.get('final_order_count'), 0)
        ledger_order_count = _as_int(ledger.get('order_count'), len(ledger.get('orders', []) or []) if isinstance(ledger.get('orders'), list) else 0)
        ledger_fill_count = _as_int(ledger.get('fill_count'), len(ledger.get('fills', []) or []) if isinstance(ledger.get('fills'), list) else 0)
        twap_child_count = _as_int(twap.get('child_count'), 0)
        twap_issue_count = _as_int(twap.get('issue_count'), 0)
        twap_queue_count = _as_int(queue.get('queue_count'), _as_int(twap.get('queue_count'), 0))

        checks: dict[str, dict[str, Any]] = {}
        checks['execution_ledger_evidence'] = {
            'pass': bool(ledger_order_count > 0 or ledger_fill_count > 0 or final_order_count == 0),
            'status': 'ok' if (ledger_order_count > 0 or ledger_fill_count > 0) else ('not_required_no_final_orders' if final_order_count == 0 else 'missing'),
            'ledger_order_count': ledger_order_count,
            'ledger_fill_count': ledger_fill_count,
            'final_order_count': final_order_count,
            'hard': bool(final_order_count > 0),
        }
        checks['twap3_plan_queue_closure'] = {
            'pass': bool(twap_child_count == 0 or (twap_issue_count == 0 and (twap_queue_count > 0 or bool(twap.get('broker_fully_closed'))))),
            'status': twap.get('status') or 'not_available',
            'child_count': twap_child_count,
            'queue_count': twap_queue_count,
            'issue_count': twap_issue_count,
            'hard': bool(twap_child_count > 0),
        }
        checks['shadow_runtime_not_planning_only'] = {
            'pass': bool(shadow.get('runtime_observed') or final_order_count == 0),
            'status': shadow.get('status') or 'not_available',
            'runtime_observed': bool(shadow.get('runtime_observed')),
            'planning_only': bool(shadow.get('planning_only')),
            'observation_count': _as_int(shadow.get('shadow_observation_count'), 0),
            'hard': bool(final_order_count > 0),
        }
        checks['reconciliation_truthful'] = {
            'pass': bool(recon.get('status') in {'reconciliation_clean', 'reconciliation_waiting_for_broker_snapshot', 'reconciliation_waiting_for_runtime_evidence'}),
            'status': recon.get('status') or 'not_available',
            'ready_for_live_promotion': bool(recon.get('ready_for_live_promotion')),
            'broker_snapshot_present': bool(recon.get('broker_snapshot_present')),
            'paper_prelive_local_ready': bool(recon.get('paper_prelive_local_ready')),
            'hard': False,
            'note': '缺 broker snapshot 不能視為 live promotion ready，但非券商閉環可接受 truthful waiting 狀態。',
        }
        checks['feature_ablation_enforced'] = {
            'pass': bool(feature_review.get('status') in {'feature_review_ready'} and (not (feature_review.get('ablation_gate') or {}).get('required', True) or (feature_review.get('ablation_gate') or {}).get('status') == 'feature_ablation_ready')),
            'feature_review_status': feature_review.get('status') or 'not_available',
            'ablation_status': ablation.get('status') or (feature_review.get('ablation_gate') or {}).get('status') or 'not_available',
            'approved_feature_count': _as_int(feature_review.get('approved_feature_count'), 0),
            'hard': True,
        }
        checks['model_role_router_visible'] = {
            'pass': bool(model_role.get('approved') is not None or final_order_count == 0),
            'status': 'visible' if model_role else ('not_required_no_final_orders' if final_order_count == 0 else 'missing'),
            'approved': model_role.get('approved'),
            'veto_reasons': model_role.get('veto_reasons', []) if isinstance(model_role.get('veto_reasons'), list) else [],
            'hard': bool(final_order_count > 0),
        }
        checks['kill_switch_visible'] = {
            'pass': bool(kill_switch),
            'status': kill_switch.get('status') or ('visible' if kill_switch else 'missing'),
            'hard': True,
        }

        hard_blocks = []
        soft_waiting = []
        for name, check in checks.items():
            if not check.get('pass'):
                if check.get('hard'):
                    hard_blocks.append(name)
                else:
                    soft_waiting.append(name)
        if hard_blocks:
            status = 'nonbroker_runtime_closure_blocked'
        elif soft_waiting:
            status = 'nonbroker_runtime_closure_waiting'
        else:
            status = 'nonbroker_runtime_closed_loop_ready'

        payload = {
            'generated_at': _now(),
            'module_version': self.MODULE_VERSION,
            'status': status,
            'nonbroker_closed_loop_ready': status == 'nonbroker_runtime_closed_loop_ready',
            'true_broker_closed_loop_ready': bool(recon.get('ready_for_live_promotion') and twap.get('broker_fully_closed')),
            'hard_blocks': hard_blocks,
            'soft_waiting': soft_waiting,
            'checks': checks,
            'builder_runs': ran,
            'scope': {
                'covered_without_broker_data': ['paper/live local ledger', 'TWAP3 queue', 'shadow runtime evidence', 'feature ablation', 'model-role visibility', 'kill switch visibility'],
                'requires_real_broker_data': ['adapter submit/replace/cancel result', 'broker callbacks', 'broker cash/positions/fills snapshot', 'true broker reconciliation green'],
            },
            'truthful_rule': '非券商閉環與真券商閉環分開判定；不得把 planning-only 或 missing broker snapshot 寫成 ready。',
        }
        _write_json(self.path, payload)
        return self.path, payload


def main(argv: list[str] | None = None) -> int:
    path, payload = NonBrokerRuntimeClosureGate().build()
    print(json.dumps({'status': payload.get('status'), 'path': str(path), 'hard_blocks': payload.get('hard_blocks')}, ensure_ascii=False, indent=2))
    return 0 if payload.get('nonbroker_closed_loop_ready') else 1


if __name__ == '__main__':
    raise SystemExit(main())
