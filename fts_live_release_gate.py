# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json
from fts_operator_approval import OperatorApprovalRegistry


class LiveReleaseGate:
    def __init__(self):
        self.approval = OperatorApprovalRegistry()

    def evaluate(self) -> tuple[str, dict[str, Any]]:
        governance = load_json(PATHS.runtime_dir / 'trainer_promotion_decision.json', {}) or {}
        safety = load_json(PATHS.runtime_dir / 'live_safety_gate.json', {}) or {}
        recon = load_json(PATHS.runtime_dir / 'reconciliation_engine.json', {}) or {}
        recovery = load_json(PATHS.runtime_dir / 'recovery_validation.json', {}) or {}
        broker_contract = load_json(PATHS.runtime_dir / 'broker_requirements_contract.json', {}) or {}
        approval = self.approval.latest_for('live_cutover')
        checks = {
            'promotion_clear': bool(governance.get('promote_candidate', False)),
            'safety_clear': bool(safety.get('paper_live_safe', False)),
            'reconciliation_green': bool(recon.get('all_green', False)),
            'recovery_ready': bool(recovery.get('ready_for_resume', False)),
            'broker_contract_defined': bool(broker_contract),
            'operator_approved': bool(approval.get('approved', False)),
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
