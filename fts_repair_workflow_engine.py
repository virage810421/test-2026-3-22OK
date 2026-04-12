# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, write_json, append_jsonl, normalize_key
from config import PARAMS

class RepairWorkflowEngine:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'repair_workflow_engine.json'
        self.events_path = PATHS.runtime_dir / 'repair_workflow_events.jsonl'
        self.exec_path = PATHS.runtime_dir / 'repair_workflow_execution.json'

    def _plan_lane_steps(self, lane: str, issue_type: str, order_id: str) -> list[dict[str, Any]]:
        base = [{'step': 'mark_repair_review', 'patch': {'repair_flag': True, 'last_issue_type': issue_type}}]
        if issue_type == 'missing_at_broker':
            base.append({'step': 'shadow_rebuild_from_local', 'patch': {'shadow_sync': 'local_source'}})
        elif issue_type == 'orphan_broker_order':
            base.append({'step': 'shadow_attach_broker_order', 'patch': {'shadow_sync': 'broker_source'}})
        elif issue_type == 'status_mismatch':
            base.append({'step': 'align_status_shadow', 'patch': {'shadow_sync': 'status_aligned'}})
        else:
            base.append({'step': 'generic_review', 'patch': {'shadow_sync': 'review_pending'}})
        base.append({'step': 'mark_repaired', 'patch': {'repair_flag': False, 'repair_complete': True}})
        return [{'lane': lane, 'order_id': order_id, **step} for step in base]

    def plan(self, reconciliation_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        issues = reconciliation_payload.get('issues', []) if isinstance(reconciliation_payload, dict) else []
        lane_actions = []
        for issue in issues:
            lane = normalize_key(issue.get('lane')) or 'UNKNOWN'
            order_id = str(issue.get('order_id') or '').strip()
            issue_type = str(issue.get('type') or 'generic_issue')
            if not order_id:
                continue
            lane_actions.append({'lane': lane, 'order_id': order_id, 'issue_type': issue_type, 'priority': 'high' if issue_type in {'missing_at_broker', 'status_mismatch'} else 'medium', 'steps': self._plan_lane_steps(lane, issue_type, order_id)})
        payload = {'generated_at': now_str(), 'status': 'repair_workflow_planned', 'lane_actions': lane_actions, 'lane_count': len({a['lane'] for a in lane_actions})}
        write_json(self.path, payload)
        append_jsonl(self.events_path, payload)
        return str(self.path), payload

    def execute(self, reconciliation_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        from fts_execution_ledger import ExecutionLedger
        from fts_execution_state_machine import DirectionalExecutionStateMachine
        _plan_path, plan = self.plan(reconciliation_payload)
        ledger = ExecutionLedger()
        sm = DirectionalExecutionStateMachine()
        executed = []
        for lane_action in plan.get('lane_actions', []):
            lane = normalize_key(lane_action.get('lane')) or 'UNKNOWN'
            order_id = str(lane_action.get('order_id') or '').strip()
            for step in lane_action.get('steps', []):
                step_name = str(step.get('step') or 'repair_step')
                patch = step.get('patch') or {}
                if bool(PARAMS.get('ENABLE_DIRECTIONAL_LEDGER_MUTATION', True)):
                    ledger.mutate_from_repair(lane, order_id, step_name, note=lane_action.get('issue_type', ''), patch=patch)
                if step_name == 'mark_repaired':
                    sm.force_repair(lane, order_id, 'REPAIRED', reason=lane_action.get('issue_type', ''))
                else:
                    sm.force_repair(lane, order_id, 'REPAIR_REVIEW', reason=step_name)
                executed.append({'lane': lane, 'order_id': order_id, 'step': step_name, 'status': 'executed'})
        payload = {'generated_at': now_str(), 'status': 'repair_workflow_executed', 'executed_actions': executed, 'count': len(executed)}
        write_json(self.exec_path, payload)
        append_jsonl(self.events_path, payload)
        return str(self.exec_path), payload
