# -*- coding: utf-8 -*-
import json
from fts_config import CONFIG, PATHS
from fts_market_rules_tw import validate_order_payload
from fts_feature_service import FeatureService


class LiveReadinessGate:
    MODULE_VERSION = 'v86'

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
        checks['training_artifacts_present'] = any((PATHS.model_dir / n).exists() for n in ['selected_features.pkl', 'model_趨勢多頭.pkl', 'model_區間盤整.pkl', 'model_趨勢空頭.pkl'])
        checks['broker_contract_present'] = (PATHS.runtime_dir / getattr(CONFIG, 'broker_contract_filename', 'broker_submission_contract.json')).exists()
        checks['broker_requirements_present'] = (PATHS.runtime_dir / getattr(CONFIG, 'broker_requirements_filename', 'broker_requirements_contract.json')).exists()

        feature_service = FeatureService()
        manifest_checks = feature_service.validate_live_feature_parity(feature_service.load_selected_features())
        checks['feature_manifest_present'] = bool(feature_service.feature_manifest_path.exists())
        checks['feature_parity_ok'] = bool(manifest_checks.get('ok', False))
        checks['runtime_artifacts_ready'] = bool(manifest_checks.get('artifact_status', {}).get('runtime_artifacts_ready', False))

        payload_checks = []
        if normalized_df is not None and not normalized_df.empty:
            for _, row in normalized_df.iterrows():
                payload_checks.append(validate_order_payload(
                    str(row.get('Ticker', '')).strip(),
                    float(row.get('Reference_Price', 0) or 0),
                    int(float(row.get('Target_Qty', 0) or 0)),
                    int(getattr(CONFIG, 'lot_size', 1000))
                ).to_dict())
        valid_payloads = sum(1 for x in payload_checks if x.get('passed'))
        total_payloads = len(payload_checks)

        hard_blocks = []
        if checks['mode_is_live'] and not checks['broker_is_real']:
            hard_blocks.append('live_mode_but_not_real_broker')
        if checks['mode_is_live'] and not checks['manual_live_arm']:
            hard_blocks.append('live_manual_arm_missing')
        if checks['mode_is_live'] and not checks['credentials_file_present']:
            hard_blocks.append('broker_credentials_missing')
        if checks['mode_is_live'] and not checks['price_snapshot_present']:
            hard_blocks.append('price_snapshot_missing')
        if not checks['broker_contract_present']:
            hard_blocks.append('broker_contract_missing')
        if not checks['broker_requirements_present']:
            hard_blocks.append('broker_requirements_contract_missing')
        if not checks['feature_manifest_present']:
            hard_blocks.append('feature_manifest_missing')
        if not checks['feature_parity_ok']:
            hard_blocks.append('feature_parity_failed')
        if total_payloads > 0 and valid_payloads < total_payloads:
            hard_blocks.append('some_order_payloads_fail_tw_market_rules')

        live_ready = len(hard_blocks) == 0 and checks['broker_is_real'] and checks['manual_live_arm'] and checks['credentials_file_present']
        pipeline_ready = not any(x for x in hard_blocks if x not in {'live_mode_but_not_real_broker', 'live_manual_arm_missing', 'broker_credentials_missing'})
        if live_ready:
            status = 'live_ready'
        elif hard_blocks:
            status = 'blocked'
        else:
            status = 'paper_pipeline_ready'

        result = {
            'module_version': self.MODULE_VERSION,
            'checks': checks,
            'manifest_checks': manifest_checks,
            'payload_check_total': total_payloads,
            'payload_check_passed': valid_payloads,
            'payload_checks': payload_checks[:50],
            'hard_blocks': hard_blocks,
            'live_ready': live_ready,
            'pipeline_ready': pipeline_ready,
            'status': status,
        }
        self.out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.out, result
