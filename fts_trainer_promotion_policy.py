# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, write_json


class TrainerPromotionPolicyBuilder:
    """Formal promotion policy with hard gates for training -> shadow -> promote."""

    def __init__(self):
        self.path = PATHS.runtime_dir / 'trainer_promotion_policy.json'
        self.shadow_runtime_path = PATHS.runtime_dir / 'shadow_runtime_evidence.json'

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ''):
                return default
            return float(value)
        except Exception:
            return default

    def _load_json(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _build_shadow_runtime_evidence(self) -> tuple[Any, dict[str, Any]]:
        journal = self._load_json(PATHS.runtime_dir / 'execution_journal_summary.json')
        decision_gate = self._load_json(PATHS.runtime_dir / 'decision_execution_formal_gate.json')
        live_safety = self._load_json(PATHS.runtime_dir / 'live_safety_gate.json')
        closure = self._load_json(PATHS.runtime_dir / 'true_broker_live_closure.json')
        backend = self._load_json(PATHS.runtime_dir / 'trainer_backend_report.json')

        observed_candidates = int(journal.get('total_event_count_estimate', 0) or 0)
        observed_orders = int(journal.get('new_order_candidate_count', 0) or 0)
        final_order_count = int(decision_gate.get('final_order_count', 0) or 0)
        paper_like_activity = max(observed_orders, final_order_count, int(((closure.get('callback_summary') or {}).get('ingested_count', 0) or 0)))
        runtime_observed = bool(observed_candidates > 0 or paper_like_activity > 0)

        runtime_drift = None
        for payload in (journal, decision_gate, closure):
            for key in ('shadow_return_drift_pct', 'return_drift_pct', 'paper_return_drift_pct'):
                if key in payload:
                    runtime_drift = self._safe_float(payload.get(key), 0.0)
                    break
            if runtime_drift is not None:
                break
        if runtime_drift is None:
            runtime_drift = self._safe_float(backend.get('shadow_runtime_drift_pct'), None)

        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'runtime_observed': runtime_observed,
            'shadow_observation_count': int(observed_candidates),
            'paper_like_activity_count': int(paper_like_activity),
            'shadow_return_drift_pct': runtime_drift,
            'runtime_sources': {
                'execution_journal_summary': str(PATHS.runtime_dir / 'execution_journal_summary.json') if journal else '',
                'decision_execution_formal_gate': str(PATHS.runtime_dir / 'decision_execution_formal_gate.json') if decision_gate else '',
                'true_broker_live_closure': str(PATHS.runtime_dir / 'true_broker_live_closure.json') if closure else '',
                'trainer_backend_report': str(PATHS.runtime_dir / 'trainer_backend_report.json') if backend else '',
            },
            'live_safety_clear': bool(live_safety.get('go_for_execution', live_safety.get('status') not in {'live_safety_blocked'})) if live_safety else True,
            'status': 'shadow_runtime_evidence_ready' if runtime_observed else 'shadow_runtime_evidence_missing',
            'truthful_rule': '沒有真實 shadow/paper runtime 觀察證據時，不得把 overfit gap 當作 shadow pass。',
        }
        write_json(self.shadow_runtime_path, payload)
        return self.shadow_runtime_path, payload

    def build(self) -> tuple[Any, dict[str, Any]]:
        _shadow_path, shadow_payload = self._build_shadow_runtime_evidence()
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'promotion_policy': {
                'stages': [
                    'offline_training_pass',
                    'walk_forward_validation_pass',
                    'artifact_integrity_pass',
                    'registry_update_pass',
                    'shadow_live_observation_pass',
                    'operator_approval_pass',
                    'paper_live_safe_pass',
                ],
                'minimum_requirements': [
                    '模型產物完整',
                    '版本號已更新',
                    'walk_forward_score 達標',
                    'shadow runtime evidence 已存在',
                    'shadow drift 未爆表',
                    '未觸發 live safety block',
                    '可回退版本存在',
                ],
                'default_thresholds': {
                    'min_walk_forward_score': 70,
                    'min_profit_factor': 1.15,
                    'min_win_rate': 0.52,
                    'max_drawdown_pct': 0.10,
                    'max_shadow_return_drift_pct': 0.08,
                    'min_shadow_observation_count': 1,
                },
                'deployment_rule': 'not_promote_to_live_without_all_required_stages',
                'rollback_rule': 'auto_recommend_rollback_when_live_metrics_break_thresholds',
                'truthful_shadow_rule': 'shadow result must come from runtime observation, not offline overfit proxy.',
            },
            'shadow_runtime_evidence': shadow_payload,
            'status': 'promotion_policy_ready',
        }
        write_json(self.path, payload)
        log(f'🚦 已輸出 trainer promotion policy：{self.path}')
        return self.path, payload

    def evaluate(
        self,
        artifact_ok: bool,
        registry_updated: bool,
        walk_forward_score: float,
        profit_factor: float,
        win_rate: float,
        max_drawdown_pct: float,
        shadow_return_drift_pct: float,
        rollback_version_exists: bool,
        operator_approved: bool,
        live_safety_clear: bool,
        shadow_runtime_observed: bool | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        failures = []
        warnings = []
        _shadow_path, shadow_runtime = self._build_shadow_runtime_evidence()
        runtime_observed = bool(shadow_runtime.get('runtime_observed')) if shadow_runtime_observed is None else bool(shadow_runtime_observed)
        runtime_drift = shadow_runtime.get('shadow_return_drift_pct')
        effective_shadow_drift = runtime_drift if runtime_drift is not None else shadow_return_drift_pct

        if not artifact_ok:
            failures.append('artifact_integrity_fail')
        if not registry_updated:
            failures.append('registry_update_fail')
        if walk_forward_score < 70:
            failures.append('walk_forward_score_fail')
        if profit_factor < 1.15:
            failures.append('profit_factor_fail')
        if win_rate < 0.52:
            failures.append('win_rate_fail')
        if max_drawdown_pct > 0.10:
            failures.append('drawdown_fail')
        if effective_shadow_drift is not None and effective_shadow_drift > 0.08:
            warnings.append('shadow_drift_high')
        if not runtime_observed:
            failures.append('shadow_runtime_evidence_missing')
        if not rollback_version_exists:
            failures.append('rollback_version_missing')
        if not operator_approved:
            failures.append('operator_approval_missing')
        if not live_safety_clear:
            failures.append('live_safety_blocked')
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'inputs': {
                'artifact_ok': artifact_ok,
                'registry_updated': registry_updated,
                'walk_forward_score': walk_forward_score,
                'profit_factor': profit_factor,
                'win_rate': win_rate,
                'max_drawdown_pct': max_drawdown_pct,
                'shadow_return_drift_pct': effective_shadow_drift,
                'rollback_version_exists': rollback_version_exists,
                'operator_approved': operator_approved,
                'live_safety_clear': live_safety_clear,
                'shadow_runtime_observed': runtime_observed,
            },
            'shadow_runtime_evidence': shadow_runtime,
            'go_for_shadow': len([x for x in failures if x not in {'operator_approval_missing', 'live_safety_blocked'}]) == 0,
            'go_for_promote': len(failures) == 0,
            'failures': failures,
            'warnings': warnings,
            'status': 'promote_ready' if not failures else 'promote_blocked',
        }
        out = PATHS.runtime_dir / 'trainer_promotion_decision.json'
        write_json(out, payload)
        log(f'🧠 已輸出 trainer promotion decision：{out}')
        return out, payload
