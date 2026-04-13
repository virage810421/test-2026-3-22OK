# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from config import PARAMS
from fts_prelive_runtime import PATHS, now_str, write_json, append_jsonl, normalize_key
from fts_execution_ledger import ExecutionLedger
from fts_execution_state_machine import DirectionalExecutionStateMachine
from fts_broker_shadow_mutator import BrokerShadowLedgerMutator


class RepairWorkflowEngine:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'repair_workflow_engine.json'
        self.events_path = PATHS.runtime_dir / 'repair_workflow_events.jsonl'
        self.exec_path = PATHS.runtime_dir / 'repair_workflow_execution.json'
        self.ledger = ExecutionLedger()
        self.state_machine = DirectionalExecutionStateMachine()
        self.shadow_mutator = BrokerShadowLedgerMutator()

    def _steps_for_issue(self, lane: str, issue: dict[str, Any]) -> list[dict[str, Any]]:
        order_id = str(issue.get('order_id') or f'{lane}-repair-{now_str()}')
        issue_type = normalize_key(issue.get('type')) or 'UNKNOWN'
        steps = [
            {'step': 'mark_repair_pending', 'target_status': 'REPAIR_PENDING', 'mutation_type': 'repair_pending', 'patch': {'repair_pending': True, 'issue_type': issue_type}},
            {'step': 'sync_shadow_ledger', 'target_status': 'REPAIR_PENDING', 'mutation_type': 'shadow_sync', 'patch': {'synced_by_repair': True, 'issue_type': issue_type}},
            {'step': 'reconcile_lane_order', 'target_status': 'REPAIR_REVIEW', 'mutation_type': 'reconcile_retry', 'patch': {'reconcile_retry_requested': True}},
        ]
        if bool(PARAMS.get('ENABLE_FULLY_AUTOMATED_REPAIR_MUTATOR', True)):
            steps.append({'step': 'finalize_repair', 'target_status': 'REPAIRED', 'mutation_type': 'repair_finalize', 'patch': {'repair_finalized': True}})
        for step in steps:
            step['order_id'] = order_id
            step['lane'] = lane
            step['issue_type'] = issue_type
        return steps

    def plan(self, reconciliation_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        issues = list((reconciliation_payload or {}).get('issues', []) or [])
        lane_actions = []
        for issue in issues:
            lane = normalize_key(issue.get('lane')) or 'UNKNOWN'
            lane_actions.append({'lane': lane, 'order_id': issue.get('order_id', ''), 'issue_type': issue.get('type', ''), 'steps': self._steps_for_issue(lane, issue)})
        payload = {'generated_at': now_str(), 'status': 'repair_workflow_planned', 'lane_actions': lane_actions, 'lane_count': len({a['lane'] for a in lane_actions})}
        write_json(self.path, payload)
        append_jsonl(self.events_path, payload)
        return str(self.path), payload

    def execute(self, reconciliation_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        _plan_path, plan = self.plan(reconciliation_payload)
        executed = []
        for lane_action in plan.get('lane_actions', []):
            lane = normalize_key(lane_action.get('lane')) or 'UNKNOWN'
            order_id = str(lane_action.get('order_id') or f'{lane}-repair-{now_str()}')
            for step in lane_action.get('steps', []):
                step_name = step.get('step', 'repair_step')
                target_status = step.get('target_status', 'REPAIR_REVIEW')
                mutation_type = step.get('mutation_type', step_name)
                patch = dict(step.get('patch') or {})
                if bool(PARAMS.get('ENABLE_DIRECTIONAL_LEDGER_MUTATION', True)):
                    self.ledger.mutate_from_repair(lane, order_id, mutation_type, note=step_name, patch=patch)
                self.state_machine.force_repair(lane, order_id, target_status, reason=lane_action.get('issue_type', ''), step=step_name)
                if bool(PARAMS.get('ENABLE_BROKER_SHADOW_LEDGER_MUTATOR', True)):
                    self.shadow_mutator.mutate(lane, order_id, mutation_type, patch=patch, reason=step_name)
                executed.append({'lane': lane, 'order_id': order_id, 'step': step_name, 'mutation_type': mutation_type, 'status': 'executed'})
        payload = {'generated_at': now_str(), 'status': 'repair_workflow_executed', 'executed_actions': executed, 'count': len(executed)}
        write_json(self.exec_path, payload)
        append_jsonl(self.events_path, payload)
        return str(self.exec_path), payload
