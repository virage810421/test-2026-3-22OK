# -*- coding: utf-8 -*-
from __future__ import annotations

import traceback
from typing import Any

from fts_prelive_runtime import PATHS, now_str, log, load_json, write_json
from fts_broker_requirements_contract import BrokerRequirementsContract
from fts_order_state_machine import OrderStateMachine
from fts_callback_event_schema import CallbackEventSchema
from fts_callback_event_store import CallbackEventStore
from fts_operator_approval import OperatorApprovalRegistry
from fts_preopen_checklist import PreOpenChecklistBuilder
from fts_intraday_incident_guard import IntradayIncidentGuard
from fts_eod_closebook import EODCloseBookBuilder
from fts_live_release_gate import LiveReleaseGate
from fts_live_cutover_plan import LiveCutoverPlanBuilder

try:
    from formal_trading_system_v80_prebroker_sealed import FormalTradingSystemV80PreBrokerSealed  # type: ignore
except Exception:
    FormalTradingSystemV80PreBrokerSealed = None


class FormalTradingSystemV81MainlineMerged:
    MODULE_VERSION = 'v81_mainline_merged'

    def _run_v80_layer(self) -> dict[str, Any]:
        if FormalTradingSystemV80PreBrokerSealed is not None:
            _, report = FormalTradingSystemV80PreBrokerSealed().run()
            return report or {}
        return load_json(PATHS.runtime_dir / 'formal_trading_system_v80_prebroker_sealed_report.json', {}) or {}

    def run(self) -> tuple[str, dict[str, Any]]:
        log('=' * 72)
        log('🚀 啟動 formal_trading_system_v81_mainline_merged')
        log('🧭 單一主控入口：V80 prebroker sealed + V81 prelive bridge 已合併')
        log('🎯 目標：券商開戶前只跑一支主線，完成所有可由 code 封口的層')
        log('=' * 72)

        v80_report = self._run_v80_layer()

        contract_path, contract = BrokerRequirementsContract().build()
        osm_path, osm = OrderStateMachine().build_definition()
        schema_path, schema = CallbackEventSchema().build_definition()
        event_store_path, event_store = CallbackEventStore().record({
            'broker_order_id': 'SIM-ORDER-001',
            'client_order_id': 'CLIENT-ORDER-001',
            'event_type': 'ACK',
            'status': 'SUBMITTED',
            'symbol': '2330.TW',
            'timestamp': now_str(),
            'filled_qty': 0,
            'remaining_qty': 1000,
        })
        approval_path, approval = OperatorApprovalRegistry().approve('live_cutover', 'system_seed', False, '預設不放行，等待真券商資訊')
        preopen_path, preopen = PreOpenChecklistBuilder().build()
        incident_path, incident = IntradayIncidentGuard().evaluate(
            broker_connected=False,
            callback_lag_seconds=999,
            reject_rate=0.0,
            day_loss_pct=0.0,
        )
        closebook_path, closebook = EODCloseBookBuilder().build()
        cutover_path, cutover = LiveCutoverPlanBuilder().build()
        release_path, release = LiveReleaseGate().evaluate()

        report = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'mainline_merge': True,
            'merged_layers': {
                'v80_prebroker_sealed': True,
                'v81_prelive_bridge': True,
            },
            'v80_report_present': bool(v80_report),
            'v80_layer_status': load_json(PATHS.runtime_dir / 'prebroker_seal_layer_status.json', {}) or {},
            'outputs': {
                'broker_requirements_contract': contract_path,
                'order_state_machine_definition': osm_path,
                'callback_event_schema': schema_path,
                'callback_event_store_summary': event_store_path,
                'operator_approval_registry': approval_path,
                'preopen_checklist': preopen_path,
                'intraday_incident_guard': incident_path,
                'eod_closebook': closebook_path,
                'live_cutover_plan': cutover_path,
                'live_release_gate': release_path,
            },
            'statuses': {
                'P0': 'completed',
                'P1': 'completed_for_pre_broker_stage',
                'P2': 'broker_ready_blueprint_only',
                'preopen_all_green': preopen.get('all_green', False),
                'incident_guard': incident.get('status'),
                'release_gate': release.get('status'),
                'ready_now_before_account': cutover.get('ready_now_count', 0),
                'waiting_for_broker': cutover.get('waiting_count', 0),
            },
            'summary': {
                'can_run_as_single_mainline': True,
                'requires_running_v80_separately': False,
                'next_real_gap': [
                    '真券商 API 綁定',
                    'callback / ledger 真實對帳綁定',
                    '實盤 cutover',
                ],
            },
        }
        path = PATHS.runtime_dir / 'formal_trading_system_v81_mainline_merged_report.json'
        write_json(path, report)
        log('✅ V81 單一主線合併完成 | 不需要再分開跑 v80 與 v81')
        log(f'🧩 ready_now={report["statuses"]["ready_now_before_account"]} | waiting_for_broker={report["statuses"]["waiting_for_broker"]}')
        log(f'📄 報告輸出：{path}')
        log('-' * 72)
        return str(path), report


def main() -> int:
    try:
        FormalTradingSystemV81MainlineMerged().run()
        return 0
    except Exception as exc:
        err = {
            'generated_at': now_str(),
            'module_version': FormalTradingSystemV81MainlineMerged.MODULE_VERSION,
            'error': str(exc),
            'traceback': traceback.format_exc(),
        }
        write_json(PATHS.runtime_dir / 'formal_trading_system_v81_mainline_merged_error.json', err)
        log(f'❌ V81 單一主線執行失敗：{exc}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
