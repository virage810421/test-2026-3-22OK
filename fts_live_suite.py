# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log
from fts_prelive_runtime import write_json, load_json
from fts_operations_suite import OperatorApprovalRegistry


class LiveAdapterStubBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'live_adapter_stub.json'

    def build(self):
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'stub_methods': ['connect', 'place_order', 'cancel_order', 'get_order_status', 'get_positions', 'get_cash', 'disconnect'],
            'safety_rules': ['default_disabled', 'requires_live_approval_workflow', 'requires_operator_confirmation', 'requires_callback_monitoring'],
            'status': 'stub_defined_not_enabled',
        }
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f'🧱 已輸出 live adapter stub：{self.path}')
        return self.path, payload


class LiveApprovalWorkflowBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'live_approval_workflow.json'

    def build(self):
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'workflow': [
                {'step': 1, 'name': 'launch_gate_pass'},
                {'step': 2, 'name': 'live_safety_gate_pass'},
                {'step': 3, 'name': 'broker_approval_gate_pass'},
                {'step': 4, 'name': 'submission_contract_gate_pass'},
                {'step': 5, 'name': 'operator_confirmation_reserved'},
                {'step': 6, 'name': 'live_adapter_submission_reserved'},
                {'step': 7, 'name': 'callback_monitoring_reserved'},
                {'step': 8, 'name': 'reconciliation_green_required'},
                {'step': 9, 'name': 'true_broker_ready_required'},
            ],
            'status': 'workflow_defined_not_live_enabled',
        }
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f'✅ 已輸出 live approval workflow：{self.path}')
        return self.path, payload


class LiveCutoverPlanBuilder:
    def build(self) -> tuple[str, dict[str, Any]]:
        steps = [
            {'seq': 1, 'name': '固定 broker contract', 'status': 'ready_now_before_account'},
            {'seq': 2, 'name': '固定 order state machine', 'status': 'ready_now_before_account'},
            {'seq': 3, 'name': '固定 callback schema', 'status': 'ready_now_before_account'},
            {'seq': 4, 'name': '建立 callback event store', 'status': 'ready_now_before_account'},
            {'seq': 5, 'name': '建立 operator approval registry', 'status': 'ready_now_before_account'},
            {'seq': 6, 'name': '建立 pre-open checklist', 'status': 'ready_now_before_account'},
            {'seq': 7, 'name': '建立 intraday incident guard', 'status': 'ready_now_before_account'},
            {'seq': 8, 'name': '建立 EOD closebook', 'status': 'ready_now_before_account'},
            {'seq': 9, 'name': '建立 live release gate', 'status': 'ready_now_before_account'},
            {'seq': 10, 'name': '券商 API 綁定', 'status': 'waiting_for_account_and_api'},
            {'seq': 11, 'name': 'callback / ledger 真實對帳綁定', 'status': 'waiting_for_account_and_api'},
            {'seq': 12, 'name': '實盤 cutover', 'status': 'waiting_for_account_and_api'},
        ]
        payload = {
            'generated_at': now_str(),
            'status': 'live_cutover_plan_defined',
            'steps': steps,
            'ready_now_count': sum(1 for x in steps if x['status'] == 'ready_now_before_account'),
            'waiting_count': sum(1 for x in steps if x['status'] != 'ready_now_before_account'),
        }
        path = PATHS.runtime_dir / 'live_cutover_plan.json'
        write_json(path, payload)
        return str(path), payload


class LiveReleaseGate:
    def __init__(self):
        self.approval = OperatorApprovalRegistry()

    def evaluate(self, governance: dict[str, Any] | None = None, safety: dict[str, Any] | None = None, recon: dict[str, Any] | None = None,
                 recovery: dict[str, Any] | None = None, approval: dict[str, Any] | None = None,
                 broker_contract: dict[str, Any] | None = None, true_broker: dict[str, Any] | None = None) -> tuple[str, dict[str, Any]]:
        governance = governance or load_json(PATHS.runtime_dir / 'trainer_promotion_decision.json', {}) or {}
        safety = safety or load_json(PATHS.runtime_dir / 'live_safety_gate.json', {}) or {}
        recon = recon or load_json(PATHS.runtime_dir / 'reconciliation_engine.json', {}) or {}
        recovery = recovery or load_json(PATHS.runtime_dir / 'recovery_validation.json', {}) or {}
        broker_contract = broker_contract or load_json(PATHS.runtime_dir / 'broker_requirements_contract.json', {}) or {}
        true_broker = true_broker or load_json(PATHS.runtime_dir / 'true_broker_readiness_gate.json', {}) or {}
        approval = approval or self.approval.latest_for('live_cutover')
        recon_green = bool(recon.get('all_green', recon.get('summary', {}).get('all_green', False))) or bool(recon.get('status') in {'reconciled', 'ok'} and int(recon.get('order_issue_count', 0) or 0) == 0)
        recovery_ready = bool(recovery.get('ready_for_resume', recovery.get('all_green', False)))
        checks = {
            'promotion_clear': bool(governance.get('go_for_shadow', governance.get('go_for_promote', False))),
            'safety_clear': bool(safety.get('paper_live_safe', False)),
            'reconciliation_green': recon_green,
            'recovery_ready': recovery_ready,
            'broker_contract_defined': bool(broker_contract),
            'operator_approved': bool(approval.get('approved', False)),
            'true_broker_ready': bool(true_broker.get('status') == 'true_broker_ready' or true_broker.get('allow_live', False)),
        }
        allow_pre_live = checks['reconciliation_green'] and checks['recovery_ready'] and checks['broker_contract_defined']
        allow_live = all(checks.values())
        payload = {
            'generated_at': now_str(),
            'status': 'live_release_allowed' if allow_live else 'live_release_blocked',
            'allow_pre_live_dryrun': allow_pre_live,
            'allow_live': allow_live,
            'checks': checks,
        }
        path = PATHS.runtime_dir / 'live_release_gate.json'
        write_json(path, payload)
        return str(path), payload
