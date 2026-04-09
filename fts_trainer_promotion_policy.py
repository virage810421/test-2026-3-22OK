# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, write_json


class TrainerPromotionPolicyBuilder:
    """Formal promotion policy with hard gates for training -> shadow -> promote."""

    def __init__(self):
        self.path = PATHS.runtime_dir / 'trainer_promotion_policy.json'

    def build(self) -> tuple[Any, dict[str, Any]]:
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
                    'shadow drift 未爆表',
                    '未觸發 live safety block',
                    '可回退版本存在',
                ],
                'default_thresholds': {
                    'min_walk_forward_score': 60,
                    'min_profit_factor': 1.10,
                    'min_win_rate': 0.50,
                    'max_drawdown_pct': 0.12,
                    'max_shadow_return_drift_pct': 0.08,
                },
                'deployment_rule': 'not_promote_to_live_without_all_required_stages',
                'rollback_rule': 'auto_recommend_rollback_when_live_metrics_break_thresholds',
            },
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
    ) -> tuple[Any, dict[str, Any]]:
        failures = []
        warnings = []
        if not artifact_ok:
            failures.append('artifact_integrity_fail')
        if not registry_updated:
            failures.append('registry_update_fail')
        if walk_forward_score < 60:
            failures.append('walk_forward_score_fail')
        if profit_factor < 1.10:
            failures.append('profit_factor_fail')
        if win_rate < 0.50:
            failures.append('win_rate_fail')
        if max_drawdown_pct > 0.12:
            failures.append('drawdown_fail')
        if shadow_return_drift_pct > 0.08:
            warnings.append('shadow_drift_high')
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
                'shadow_return_drift_pct': shadow_return_drift_pct,
                'rollback_version_exists': rollback_version_exists,
                'operator_approved': operator_approved,
                'live_safety_clear': live_safety_clear,
            },
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
