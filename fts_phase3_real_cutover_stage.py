# -*- coding: utf-8 -*-
from __future__ import annotations

import json

from fts_broker_api_adapter import ConfigurableBrokerAdapter
from fts_broker_requirements_contract import BrokerRequirementsContract
from fts_config import PATHS
from fts_live_cutover_plan import LiveCutoverPlanBuilder
from fts_live_release_gate import LiveReleaseGate
from fts_operator_approval import OperatorApprovalRegistry
from fts_real_broker_adapter_blueprint import required_real_broker_fields
from fts_utils import now_str, log


class Phase3RealCutoverStage:
    MODULE_VERSION = 'v83_phase3_adapter_ready'

    def __init__(self):
        self.path = PATHS.runtime_dir / 'phase3_real_cutover.json'

    def run(self):
        contract_path, contract = BrokerRequirementsContract().build()
        approval_path, approval = OperatorApprovalRegistry().approve(
            'live_cutover',
            'system_v83',
            False,
            '尚未完成真券商開戶，但 broker adapter / contract / cutover skeleton 已補齊',
        )
        cutover_path, cutover = LiveCutoverPlanBuilder().build()
        release_path, release = LiveReleaseGate().evaluate()

        adapter = ConfigurableBrokerAdapter()
        template_path, config_path = adapter.ensure_template_files()
        probe_path, probe = adapter.probe()

        credentials_template = {
            'broker_name': '',
            'api_key': '',
            'api_secret': '',
            'account_id': '',
            'cert_or_token': '',
            'callback_mode': 'webhook_or_polling',
            'base_url': '',
            'rate_limit_per_minute': '',
            'market_sessions_supported': ['REGULAR', 'ODD', 'AFTER_HOURS'],
        }
        credentials_path = PATHS.runtime_dir / 'real_broker_credentials_template.json'
        credentials_path.write_text(json.dumps(credentials_template, ensure_ascii=False, indent=2), encoding='utf-8')

        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'contract_path': str(contract_path),
            'approval_path': str(approval_path),
            'cutover_plan_path': str(cutover_path),
            'release_gate_path': str(release_path),
            'real_broker_fields': required_real_broker_fields(),
            'credentials_template_path': str(credentials_path),
            'broker_adapter_template_path': str(template_path),
            'broker_adapter_config_path': str(config_path),
            'broker_adapter_probe_path': str(probe_path),
            'broker_adapter_probe': probe,
            'complete_now': [
                'broker contract',
                'callback schema',
                'operator approval registry',
                'live cutover plan',
                'release gate',
                'configurable broker adapter',
                'broker adapter template/config',
            ],
            'waiting_for_real_account': [
                '券商 API 金鑰',
                '認證/簽章方式',
                'callback 或 polling 真實規格',
                '錯誤碼映射',
                '實盤小額 smoke test',
            ],
            'status': 'phase3_adapter_ready_account_pending' if not probe.get('ready_for_live_connect') else 'phase3_ready_for_live_smoketest',
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🥉 Phase3 完成：{self.path}')
        return self.path, payload
