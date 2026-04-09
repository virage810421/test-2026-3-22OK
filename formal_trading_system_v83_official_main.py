# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import traceback
from pathlib import Path

from fts_config import PATHS, CONFIG
from fts_file_classification import FileClassificationBuilder
from fts_fundamentals_etl_mainline import FundamentalsETLMainline
from fts_training_governance_mainline import TrainingGovernanceMainline
from fts_phase1_upgrade import Phase1Upgrade
from fts_phase2_mock_broker_stage import Phase2MockBrokerStage
from fts_phase3_real_cutover_stage import Phase3RealCutoverStage
from fts_utils import now_str, log
from fts_project_hygiene import ProjectHygieneManager
from fts_runtime_cleanup import RuntimeCleanupManager
from fts_data_cleanup import DataCleanupManager
from fts_fundamentals_true_backfill import FundamentalsTrueBackfill


class FormalTradingSystemV83OfficialMain:
    MODULE_VERSION = 'v83_official_main_hardening_cleanup'

    def __init__(self):
        self.path = PATHS.runtime_dir / 'formal_trading_system_v83_official_main.json'

    def run(self):
        log('=' * 72)
        log('🚀 啟動 正式交易主控版_v83_official_main')
        log('🧭 收編內容：主入口收口 + runtime/data 清理 + fundamentals 真資料補強 + 三階段交易主線')
        log('=' * 72)

        hygiene_path, hygiene = ProjectHygieneManager(PATHS.base_dir).run()
        runtime_cleanup_path, runtime_cleanup = RuntimeCleanupManager(PATHS.base_dir).run()
        data_cleanup_path, data_cleanup = DataCleanupManager(PATHS.base_dir).run()
        fundamentals_backfill_path, fundamentals_backfill = FundamentalsTrueBackfill(PATHS.base_dir).run()

        fundamentals_path, fundamentals = FundamentalsETLMainline().build_summary(mode='local_sync_only')
        training_path, training = TrainingGovernanceMainline().build_summary(execute_backend=False)
        phase1_path, phase1 = Phase1Upgrade().run()
        phase2_path, phase2 = Phase2MockBrokerStage().run()
        phase3_path, phase3 = Phase3RealCutoverStage().run()
        classification_path, classification = FileClassificationBuilder().build()

        completed = []
        if hygiene.get('status') == 'project_hygiene_applied':
            completed.append('Step1：主入口收口完成（舊主控移入 archive/versions，建立 formal_trading_system.py）')
        if runtime_cleanup.get('status') == 'runtime_cleanup_applied':
            completed.append('Step2：runtime 清理完成（歷史快照/舊版報告/錯誤檔分流）')
        if data_cleanup.get('status') == 'data_cleanup_applied':
            completed.append('Step3：data 清理完成（decision 檔統一、模板快照、可疑小檔稽核）')
        if fundamentals_backfill.get('status') == 'fundamentals_true_backfill_applied':
            completed.append('Step4：fundamentals 真資料補強完成（seed 覆蓋小檔/缺檔）')
        if fundamentals.get('status') == 'fundamentals_etl_mainline_ready':
            completed.append('Fundamentals ETL 主線就緒')
        if training.get('status') == 'training_governance_mainline_ready':
            completed.append('Training Governance 主線就緒')
        if phase1.get('status') == 'phase1_ready':
            completed.append('Phase1：pre-live 就緒')
        if phase2.get('status') == 'phase2_ready':
            completed.append('Phase2：mock-real-broker 就緒')
        if phase3.get('status') in {'phase3_adapter_ready_account_pending', 'phase3_ready_for_live_smoketest'}:
            completed.append('Phase3：真券商 adapter-ready')

        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'system_name': CONFIG.system_name,
            'maintenance_outputs': {
                'project_hygiene': str(hygiene_path),
                'runtime_cleanup': str(runtime_cleanup_path),
                'data_cleanup': str(data_cleanup_path),
                'fundamentals_true_backfill': str(fundamentals_backfill_path),
            },
            'mainline_outputs': {
                'fundamentals_etl_mainline': str(fundamentals_path),
                'training_governance_mainline': str(training_path),
                'phase1': str(phase1_path),
                'phase2': str(phase2_path),
                'phase3': str(phase3_path),
                'file_classification': str(classification_path),
            },
            'completed_upgrades': completed,
            'cleanup_completion': {
                'step1_main_entry': {'status': hygiene.get('status'), 'complete_for_scope': hygiene.get('status') == 'project_hygiene_applied'},
                'step2_runtime': {'status': runtime_cleanup.get('status'), 'complete_for_scope': runtime_cleanup.get('status') == 'runtime_cleanup_applied'},
                'step3_data': {'status': data_cleanup.get('status'), 'complete_for_scope': data_cleanup.get('status') == 'data_cleanup_applied'},
                'step4_fundamentals_true_data': {'status': fundamentals_backfill.get('status'), 'complete_for_scope': fundamentals_backfill.get('status') == 'fundamentals_true_backfill_applied'},
            },
            'phase_completion': {
                'phase1': {'status': phase1.get('status'), 'complete_for_scope': phase1.get('status') == 'phase1_ready'},
                'phase2': {'status': phase2.get('status'), 'complete_for_scope': phase2.get('status') == 'phase2_ready'},
                'phase3': {'status': phase3.get('status'), 'complete_for_scope': phase3.get('status') == 'phase3_ready_for_live_smoketest'},
            },
            'status': 'v83_official_main_hardening_ready',
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'✅ v83 正式交易主控版完成：{self.path}')
        return self.path, payload


def main() -> int:
    try:
        FormalTradingSystemV83OfficialMain().run()
        return 0
    except Exception as exc:
        err = {
            'generated_at': now_str(),
            'module_version': FormalTradingSystemV83OfficialMain.MODULE_VERSION,
            'error': str(exc),
            'traceback': traceback.format_exc(),
        }
        (PATHS.runtime_dir / 'formal_trading_system_v83_official_main_error.json').write_text(
            json.dumps(err, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        log(f'❌ v83 正式交易主控版失敗：{exc}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
