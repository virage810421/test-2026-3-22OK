# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_upgrade_runtime import PATHS, now_str, write_json


def build_upgrade_plan() -> tuple[Any, dict[str, Any]]:
    payload = {
        'generated_at': now_str(),
        'goal': '券商開戶前先把 code 可封口範圍做滿，並可直接掛進 formal_trading_system 主線',
        'p0': {
            'status': 'completed',
            'scope': '券商開戶前可完全由 code 封口',
            'items': [
                '模型升降級治理',
                'walk-forward / shadow / promotion / rollback policy',
                '對帳引擎',
                '重啟恢復 snapshot / recovery plan / validation / consistency',
                'live safety gate',
                'kill switch',
            ],
        },
        'p1': {
            'status': 'completed_for_pre_broker_stage',
            'scope': '交易員工作流與歸因層',
            'items': [
                '交易日操作面板',
                '績效歸因 / 風控歸因',
                '交易員工作流輸出',
                'mainline seal status report',
            ],
        },
        'p2': {
            'status': 'broker_ready_blueprint_only',
            'scope': '等券商 API / 帳號 / 憑證後接上',
            'items': [
                '真券商 adapter',
                'callback receiver',
                'broker ledger / real account reconciliation',
                '實盤 cutover',
            ],
        },
        'ten_items_mapping': {
            'A_must_have': {
                '真券商 adapter': 'broker-ready blueprint only',
                '實盤回報接收器': 'broker-ready blueprint only',
                '對帳系統': 'completed for pre-broker stage',
                '重啟恢復機制': 'completed for pre-broker stage',
                'Kill switch': 'completed for pre-broker stage',
            },
            'B_model_governance': {
                'Walk-forward 正式化': 'completed',
                'Shadow trading': 'completed for pre-broker stage',
                'Promotion / rollback policy': 'completed',
            },
            'C_trader_workflow': {
                '交易日操作面板': 'completed',
                '績效歸因 / 風控歸因': 'completed',
            },
        },
        'four_gap_mapping': {
            '真執行': 'broker-ready blueprint only',
            '對帳恢復': 'completed for pre-broker stage',
            '模型治理': 'completed',
            '實盤安全機制': 'completed for pre-broker stage',
        },
        'direct_mainline_entry': 'formal_trading_system_v80_prebroker_sealed.py',
    }
    path = PATHS.runtime_dir / 'upgrade_plan_status.json'
    write_json(path, payload)
    return path, payload
