# -*- coding: utf-8 -*-
from __future__ import annotations

"""v92 成熟度補齊總套件。

一次產出：特徵審核、進場追蹤、持倉生命週期、模型治理、execution journal、restart recovery、真券商五紅燈。
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fts_config import PATHS


def _run(label: str, factory: str, cls_name: str, method: str = 'build') -> dict[str, Any]:
    try:
        mod = __import__(factory, fromlist=[cls_name])
        cls = getattr(mod, cls_name)
        obj = cls()
        result = getattr(obj, method)()
        if isinstance(result, tuple) and len(result) >= 2:
            return {'label': label, 'status': 'ok', 'path': str(result[0]), 'payload': result[1] if isinstance(result[1], dict) else {'value': result[1]}}
        return {'label': label, 'status': 'ok', 'payload': result if isinstance(result, dict) else {'value': result}}
    except Exception as exc:
        return {'label': label, 'status': 'error', 'error': repr(exc)}


class MaturityUpgradeSuite:
    MODULE_VERSION = 'v92_maturity_gap_closed_loop_suite'

    def __init__(self) -> None:
        self.path = PATHS.runtime_dir / 'maturity_upgrade_suite.json'

    def build(self) -> tuple[Path, dict[str, Any]]:
        items = [
            _run('feature_review', 'fts_feature_review_service', 'FeatureReviewService'),
            _run('entry_tracking', 'fts_entry_tracking_service', 'EntryTrackingService'),
            _run('position_lifecycle', 'fts_position_lifecycle_service', 'PositionLifecycleService'),
            _run('model_governance_enhancement', 'fts_model_governance_enhancement', 'ModelGovernanceEnhancement'),
            _run('execution_journal', 'fts_execution_journal_service', 'ExecutionJournalService'),
            _run('restart_recovery', 'fts_restart_recovery_service', 'RestartRecoveryService', method='build_plan'),
            _run('true_broker_readiness_gate', 'fts_true_broker_readiness_gate', 'TrueBrokerReadinessGate'),
        ]
        hard_errors = [x for x in items if x.get('status') == 'error']
        blocked = []
        for x in items:
            payload = x.get('payload') or {}
            if isinstance(payload, dict) and str(payload.get('status', '')).startswith(('blocked', 'waiting')):
                blocked.append({'label': x.get('label'), 'payload_status': payload.get('status'), 'hard_blocks': payload.get('hard_blocks') or payload.get('production_blocks') or payload.get('red_lights')})
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'module_version': self.MODULE_VERSION,
            'status': 'maturity_upgrade_ready' if not hard_errors else 'maturity_upgrade_partial',
            'items': items,
            'hard_error_count': len(hard_errors),
            'blocked_items': blocked,
            'closed_loop_mapping': {
                'feature_engineering': 'feature_review fail-closed + train/live parity policy',
                'prepare_pilot_full_entry': 'entry_tracking action_plan + control tower gate',
                'exit_reduce_defend': 'position_lifecycle action_plan + generated SELL for EXIT/REDUCE',
                'ai_training_governance': 'trainer report + walk-forward/OOS/drift/retention; rerun after TRAIN',
                'paper_prelive': 'execution_journal + restart_recovery plan',
                'real_broker_readiness': 'true_broker_readiness_gate keeps five red lights until actual API/callback/ledger/reconcile/kill-switch evidence',
            },
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.path, payload


def main() -> int:
    path, payload = MaturityUpgradeSuite().build()
    print(f'📦 v92 成熟度補齊套件完成：{path} | status={payload.get("status")} errors={payload.get("hard_error_count")} blocked={len(payload.get("blocked_items", []))}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
