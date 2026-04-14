# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

from fts_config import CONFIG, PATHS
from fts_market_rules_tw import validate_order_payload
from fts_feature_observability import FeatureObservability
from fts_compat import apply_decision_integrity_flags
try:
    from fts_runtime_diagnostics import get_summary as get_runtime_diagnostics_summary
except Exception:  # pragma: no cover
    def get_runtime_diagnostics_summary():
        return {}


class LiveReadinessGate:
    MODULE_VERSION = 'v87_split_readiness_integrity_lane'

    def __init__(self):
        self.out = PATHS.runtime_dir / 'live_readiness_gate.json'

    def evaluate(self, normalized_df=None):
        checks = {}
        checks['mode_is_live'] = str(getattr(CONFIG, 'mode', 'PAPER')).upper() == 'LIVE'
        checks['broker_is_real'] = str(getattr(CONFIG, 'broker_type', 'paper')).lower() in ('real', 'live', 'broker')
        checks['manual_live_arm'] = bool(getattr(CONFIG, 'live_manual_arm', False))
        checks['dual_confirm'] = bool(getattr(CONFIG, 'require_dual_confirmation', True))
        checks['kill_switch_defined'] = bool(getattr(CONFIG, 'enable_live_kill_switch', True))
        checks['credentials_file_present'] = (PATHS.base_dir / 'secrets' / 'broker_credentials.json').exists()
        checks['price_snapshot_present'] = (PATHS.data_dir / 'last_price_snapshot.csv').exists() or (PATHS.base_dir / 'last_price_snapshot.csv').exists()
        runtime_diag = get_runtime_diagnostics_summary() or {}
        checks['runtime_diagnostics_event_count'] = int(runtime_diag.get('event_count', 0) or 0)
        checks['runtime_diagnostics_hard_block_count'] = int(runtime_diag.get('hard_block_count', 0) or 0)
        checks['runtime_diagnostics_hard_blocks_present'] = checks['runtime_diagnostics_hard_block_count'] > 0

        # 真券商 readiness 五紅燈：API / callback / ledger / reconcile / kill-switch。
        # 沒有實際券商綁定證據時，保持紅燈，不讓 LIVE 模式誤判為可上線。
        real_readiness_path = PATHS.runtime_dir / 'real_api_readiness.json'
        real_readiness = {}
        try:
            if real_readiness_path.exists():
                real_readiness = json.loads(real_readiness_path.read_text(encoding='utf-8'))
        except Exception:
            real_readiness = {}
        callback_events_path = PATHS.runtime_dir / 'broker_callback_events.jsonl'
        reconciliation_path = PATHS.runtime_dir / 'reconciliation_report.json'
        kill_switch_path = PATHS.runtime_dir / 'kill_switch_state.json'
        real_broker_red_lights = {
            'api': bool(checks['broker_is_real'] and checks['credentials_file_present'] and real_readiness.get('api_bound', False)),
            'callback': bool(checks['broker_is_real'] and (real_readiness.get('callback_bound', False) or callback_events_path.exists())),
            'ledger': bool(checks['broker_is_real'] and real_readiness.get('ledger_bound', False)),
            'reconcile': bool(checks['broker_is_real'] and real_readiness.get('reconcile_bound', False) and reconciliation_path.exists()),
            'kill_switch': bool(checks['kill_switch_defined'] and (kill_switch_path.exists() or not checks['mode_is_live'])),
        }
        checks['real_broker_red_lights'] = real_broker_red_lights
        checks['real_broker_red_light_failures'] = [k for k, ok in real_broker_red_lights.items() if not ok]
        checks['real_broker_five_lights_green'] = len(checks['real_broker_red_light_failures']) == 0

        shared_expected = ['selected_features.pkl', 'model_趨勢多頭.pkl', 'model_區間盤整.pkl', 'model_趨勢空頭.pkl']
        lane_feature_expected = [f'selected_features_{lane}.pkl' for lane in ['long', 'short', 'range']]
        lane_model_candidates = list(PATHS.model_dir.glob('model_long_*.pkl')) + list(PATHS.model_dir.glob('model_short_*.pkl')) + list(PATHS.model_dir.glob('model_range_*.pkl'))
        checks['training_artifacts_present'] = any((PATHS.model_dir / n).exists() for n in shared_expected)
        checks['directional_selected_features_present'] = all((PATHS.model_dir / n).exists() for n in lane_feature_expected)
        checks['directional_lane_models_present'] = len(lane_model_candidates) > 0

        observability_path, observability = FeatureObservability().build()
        checks['feature_observability_present'] = bool(observability.get('selected_feature_count', 0) > 0)
        checks['shared_only_live_mode'] = observability.get('live_feature_policy') == 'shared_only'
        checks['directional_research_not_live'] = bool(observability.get('research_only_features')) and checks['shared_only_live_mode']
        checks['directional_live_partially_blocked'] = bool(observability.get('blocked_directional_live_features')) and not checks['shared_only_live_mode']

        if normalized_df is not None and not normalized_df.empty:
            normalized_df, integrity_diag = apply_decision_integrity_flags(normalized_df)
        else:
            integrity_diag = {'row_count': 0, 'usable_rows': 0, 'execution_eligible_rows': 0, 'blocked_rows': 0, 'incomplete_rows': 0, 'status': 'empty'}
        checks['decision_desk_present'] = normalized_df is not None and not normalized_df.empty
        checks['decision_desk_has_fallback_rows'] = bool(normalized_df is not None and not normalized_df.empty and normalized_df.get('FallbackBuild', pd.Series(dtype=bool)).fillna(False).any())
        checks['decision_desk_usable_rows_present'] = bool(integrity_diag.get('usable_rows', 0) > 0)
        checks['decision_desk_execution_eligible_rows_present'] = bool(integrity_diag.get('execution_eligible_rows', 0) > 0)
        checks['decision_desk_incomplete_rows_present'] = bool(integrity_diag.get('incomplete_rows', 0) > 0)
        checks['decision_desk_blocked_rows_present'] = bool(integrity_diag.get('blocked_rows', 0) > 0)

        payload_checks = []
        if normalized_df is not None and not normalized_df.empty:
            executable_df = normalized_df[normalized_df.get('ExecutionEligible', pd.Series(dtype=bool)).fillna(False)]
            for _, row in executable_df.iterrows():
                payload_checks.append(validate_order_payload(
                    str(row.get('Ticker', '')).strip(),
                    float(row.get('Reference_Price', 0) or 0),
                    int(float(row.get('Target_Qty', 0) or 0)),
                    int(getattr(CONFIG, 'lot_size', 1000))
                ).to_dict())
        valid_payloads = sum(1 for x in payload_checks if x.get('passed'))
        total_payloads = len(payload_checks)
        hard_blocks = []
        warnings = []
        if checks['mode_is_live'] and not checks['broker_is_real']:
            hard_blocks.append('live_mode_but_not_real_broker')
        if checks['mode_is_live'] and not checks['manual_live_arm']:
            hard_blocks.append('live_manual_arm_missing')
        if checks['mode_is_live'] and not checks['credentials_file_present']:
            hard_blocks.append('broker_credentials_missing')
        if checks['mode_is_live'] and not checks['price_snapshot_present']:
            hard_blocks.append('price_snapshot_missing')
        if checks['mode_is_live'] and not checks.get('real_broker_five_lights_green'):
            for name in checks.get('real_broker_red_light_failures', []):
                hard_blocks.append(f'real_broker_red_light_{name}')
        if checks.get('runtime_diagnostics_hard_blocks_present'):
            hard_blocks.append('runtime_diagnostics_hard_blocks_present')
        if total_payloads > 0 and valid_payloads < total_payloads:
            hard_blocks.append('some_order_payloads_fail_tw_market_rules')
        if checks['decision_desk_present'] and not checks['decision_desk_usable_rows_present']:
            hard_blocks.append('decision_desk_unusable_fallback_or_incomplete_only')
        if checks['decision_desk_present'] and not checks['decision_desk_execution_eligible_rows_present']:
            hard_blocks.append('decision_desk_no_execution_eligible_rows')
        if checks['directional_research_not_live']:
            warnings.append('directional_features_present_but_live_runs_shared_only')
        if checks['decision_desk_has_fallback_rows'] and checks['decision_desk_usable_rows_present']:
            warnings.append('decision_desk_contains_mixed_fallback_rows')
        if checks.get('directional_live_partially_blocked'):
            warnings.append('some_directional_features_not_live_approved')
        if checks['decision_desk_incomplete_rows_present']:
            warnings.append('decision_desk_contains_incomplete_rows')
        if not checks['directional_selected_features_present']:
            warnings.append('directional_selected_features_missing')
        if not checks['directional_lane_models_present']:
            warnings.append('directional_lane_models_missing')

        prelive_score = sum([
            int(checks['training_artifacts_present']),
            int(checks['price_snapshot_present']),
            int(total_payloads == valid_payloads if total_payloads > 0 else True),
            int(checks['feature_observability_present']),
            int(checks['kill_switch_defined']),
            int(checks['directional_selected_features_present']),
            int(checks['directional_lane_models_present']),
        ])
        broker_score = sum([
            int(checks['broker_is_real']),
            int(checks['manual_live_arm']),
            int(checks['credentials_file_present']),
            int(checks['dual_confirm']),
        ])
        executable_score = max(0, prelive_score - len(hard_blocks))
        result = {
            'module_version': self.MODULE_VERSION,
            'checks': checks,
            'decision_desk_integrity': integrity_diag,
            'payload_check_total': total_payloads,
            'payload_check_passed': valid_payloads,
            'payload_checks': payload_checks[:50],
            'hard_blocks': hard_blocks,
            'warnings': warnings,
            'prelive_ready': prelive_score >= 5 and len(hard_blocks) == 0,
            'broker_production_ready': broker_score >= 4 and checks['broker_is_real'],
            'live_ready': len(hard_blocks) == 0 and checks['broker_is_real'] and checks['manual_live_arm'] and checks['credentials_file_present'],
            'score_split': {
                'research_paper_prelive_score': prelive_score,
                'true_broker_score': broker_score,
                'real_broker_five_lights_green_count': 5 - len(checks.get('real_broker_red_light_failures', [])),
                'executable_score': executable_score,
            },
            'feature_observability': {'path': str(observability_path), 'payload': observability},
            'runtime_diagnostics': runtime_diag,
            'status': 'ready' if len(hard_blocks) == 0 else 'blocked'
        }
        self.out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.out, result
