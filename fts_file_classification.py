# -*- coding: utf-8 -*-
from __future__ import annotations

import json

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class FileClassificationBuilder:
    def __init__(self):
        self.json_path = PATHS.runtime_dir / 'file_classification.json'
        self.md_path = PATHS.runtime_dir / 'FILE_CLASSIFICATION.md'

    def build(self):
        must_keep = [
            'formal_trading_system_v83_official_main.py',
            'formal_trading_system_v82_three_stage_upgrade.py',
            'fts_fundamentals_etl_mainline.py',
            'fts_training_governance_mainline.py',
            'fts_trainer_backend.py',
            'fts_phase1_upgrade.py',
            'fts_phase2_mock_broker_stage.py',
            'fts_phase3_real_cutover_stage.py',
            'fts_decision_execution_bridge.py',
            'fts_live_safety.py',
            'fts_reconciliation_engine.py',
            'fts_admin_suite.py',
            'fts_admin_suite.py',
            'fts_operations_suite.py',
            'fts_market_data_service.py',
            'fts_feature_service.py',
            'fts_chip_enrichment_service.py',
            'fts_screening_engine.py',
            'fts_sector_service.py',
            'fts_system_guard_service.py',
            'fts_risk_gateway.py',
            'fts_watchlist_service.py',
            'fts_market_climate_service.py',
            'fts_decision_desk_builder.py',
            'fts_signal_gate.py',
            'fts_portfolio_gate.py',
            'fts_admin_suite.py',
            'fts_ab_wave_upgrade.py',
            'fts_admin_suite.py',
            'ml_data_generator.py',
            'daily_chip_etl.py',
            'monthly_revenue_simple.py',
            'advanced_chart.py',
        ]
        diff_patch_only = [
            'yahoo_csv_to_sql.py',
            'daily_chip_etl.py',
            'monthly_revenue_simple.py',
            'ml_data_generator.py',
            'advanced_chart.py',
            'config.py',
        ]
        hold = [
            'launcher.py',
            'execution_engine.py',
            'paper_broker.py',
            'portfolio_risk.py',
            'master_pipeline.py',
            'live_paper_trading.py',
        ]
        absorbed_wrappers = [
            ('yahoo_csv_to_sql.py', '已被 fts_fundamentals_etl_mainline.py 收編；只補差異'),
            ('model_governance.py', '已被 fts_training_governance_mainline.py 收編；保留治理核心'),
            ('ml_trainer.py', '已被 fts_trainer_backend.py 收編；保留舊執行入口'),
            ('daily_chip_etl.py', '已被 fts_etl_daily_chip_service.py 收編；保留舊門牌'),
            ('monthly_revenue_simple.py', '已被 fts_etl_monthly_revenue_service.py 收編；保留舊門牌'),
            ('ml_data_generator.py', '已被 fts_training_data_builder.py 收編；保留舊門牌'),
            ('advanced_chart.py', '已被 fts_chart_service.py 收編；保留舊門牌'),
        ]

        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'must_keep': [{'file': x, 'exists': (PATHS.base_dir / x).exists(), 'reason': '主線 or 核心 service / guard / engine'} for x in must_keep],
            'diff_patch_only': [{'file': x, 'exists': (PATHS.base_dir / x).exists(), 'reason': '已收編，現在只補差異'} for x in diff_patch_only],
            'hold_do_not_touch_yet': [{'file': x, 'exists': (PATHS.base_dir / x).exists(), 'reason': '仍可作零件來源，先別整支搬'} for x in hold],
            'absorbed_keep_wrapper': [{'file': x, 'exists': (PATHS.base_dir / x).exists(), 'reason': reason} for x, reason in absorbed_wrappers],
            'status': 'classification_ready_v83_wave123',
        }
        self.json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

        md = [f'# File Classification ({now_str()})', '', '## 一定要留']
        for row in payload['must_keep']:
            md.append(f"- {row['file']}：{row['reason']}")
        md.append('')
        md.append('## 只補差異')
        for row in payload['diff_patch_only']:
            md.append(f"- {row['file']}：{row['reason']}")
        md.append('')
        md.append('## 先別動')
        for row in payload['hold_do_not_touch_yet']:
            md.append(f"- {row['file']}：{row['reason']}")
        md.append('')
        md.append('## 已收編，但先保留相容入口')
        for row in payload['absorbed_keep_wrapper']:
            md.append(f"- {row['file']}：{row['reason']}")
        self.md_path.write_text('\n'.join(md), encoding='utf-8')
        log(f'🗂️ file classification updated: {self.json_path}')
        return self.json_path, payload
