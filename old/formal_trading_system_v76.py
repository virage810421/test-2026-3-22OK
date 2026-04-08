# -*- coding: utf-8 -*-
import json
import time
from fts_training_orchestrator import TrainingOrchestrator
from fts_decision_execution_bridge import DecisionExecutionBridge
from fts_completion_gap_report import CompletionGapReportBuilder
from fts_progress_full_report import ProgressFullReport
from fts_utils import log, StageProgress, render_progress_bar
from fts_resilience import StageGuard
from fts_config import PATHS, CONFIG


class FormalTradingSystemV76:
    def __init__(self):
        self.training_orchestrator = TrainingOrchestrator()
        self.decision_execution_bridge = DecisionExecutionBridge()
        self.completion_gap_report = CompletionGapReportBuilder()
        self.progress_full_report = ProgressFullReport()
        self.progress = StageProgress(total_stages=4, heartbeat_seconds=2.0)
        self.guard = StageGuard()

    def boot(self):
        log('=' * 60)
        log('🚀 啟動 正式交易主控版_v76')
        log('🛡️ v76 safe-upgrade mode + guarded stages + checkpoints：ON')
        log(f'📊 {render_progress_bar(0.0)} 系統啟動完成，準備進入 4 個階段')
        log('=' * 60)

    def run(self):
        started = time.time()
        results = {}

        with self.progress.stage(1, '訓練治理檢查'):
            results['training'] = self.guard.run(
                'training_orchestrator',
                '訓練治理檢查',
                lambda: self.training_orchestrator.maybe_execute(),
            )

        with self.progress.stage(2, '決策執行橋接'):
            results['execution'] = self.guard.run(
                'decision_execution_bridge',
                '決策執行橋接',
                lambda: self.decision_execution_bridge.build(),
            )

        with self.progress.stage(3, '完成度缺口報告'):
            results['gap'] = self.guard.run(
                'completion_gap_report',
                '完成度缺口報告',
                lambda: self.completion_gap_report.build(),
            )

        with self.progress.stage(4, '完整進度報告'):
            results['progress'] = self.guard.run(
                'progress_full_report',
                '完整進度報告',
                lambda: self.progress_full_report.build(),
            )

        exec_report = (results.get('execution') or (None, {}))[1] if isinstance(results.get('execution'), tuple) else {}
        gap_report = (results.get('gap') or (None, {}))[1] if isinstance(results.get('gap'), tuple) else {}
        headline = gap_report.get('headline', {}) if isinstance(gap_report, dict) else {}
        total_elapsed = int(time.time() - started)
        manifest = {
            'package_version': 'v76',
            'safe_upgrade_mode': CONFIG.safe_upgrade_mode,
            'continue_on_stage_failure': CONFIG.continue_on_stage_failure,
            'elapsed_seconds': total_elapsed,
            'rows_with_price': exec_report.get('rows_with_price', 0),
            'rows_with_qty': exec_report.get('rows_with_qty', 0),
            'rows_market_rule_passed': exec_report.get('rows_market_rule_passed', 0),
            'completion_excluding_real_broker_pct': headline.get('completion_excluding_real_broker_pct', 0),
            'remaining_major_blocks_excluding_real_broker': headline.get('remaining_major_blocks_excluding_real_broker', 0),
        }
        (PATHS.runtime_dir / 'run_manifest_v76.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
        log('============================================================')
        log(f"✅ 主控執行完畢｜{render_progress_bar(1.0)}｜總耗時 {total_elapsed}s")
        log(f"📦 execution bridge rows_with_price={exec_report.get('rows_with_price', 0)} | rows_with_qty={exec_report.get('rows_with_qty', 0)} | passed={exec_report.get('rows_market_rule_passed', 0)}")
        log(f"🧭 非券商完成度：{headline.get('completion_excluding_real_broker_pct', 'n/a')}% | 尚餘硬缺口數：{headline.get('remaining_major_blocks_excluding_real_broker', 'n/a')}")
        log(f"🧯 Safe Upgrade：continue_on_stage_failure={CONFIG.continue_on_stage_failure} | checkpoint={PATHS.state_dir / 'stage_checkpoints_v76.json'}")
        log('============================================================')


def main():
    app = FormalTradingSystemV76()
    app.boot()
    app.run()


if __name__ == '__main__':
    main()
