# -*- coding: utf-8 -*-
import time
from fts_training_orchestrator import TrainingOrchestrator
from fts_decision_execution_bridge import DecisionExecutionBridge
from fts_completion_gap_report import CompletionGapReportBuilder
from fts_progress_full_report import ProgressFullReport
from fts_utils import log, StageProgress, render_progress_bar


class FormalTradingSystemV75:
    def __init__(self):
        self.training_orchestrator = TrainingOrchestrator()
        self.decision_execution_bridge = DecisionExecutionBridge()
        self.completion_gap_report = CompletionGapReportBuilder()
        self.progress_full_report = ProgressFullReport()
        self.progress = StageProgress(total_stages=4, heartbeat_seconds=2.0)

    def boot(self):
        log('=' * 60)
        log('🚀 啟動 正式交易主控版_v75')
        log('📶 v75 staged progress bar + heartbeat：ON')
        log(f'📊 {render_progress_bar(0.0)} 系統啟動完成，準備進入 4 個階段')
        log('=' * 60)

    def run(self):
        started = time.time()

        with self.progress.stage(1, '訓練治理檢查'):
            self.training_orchestrator.maybe_execute()

        with self.progress.stage(2, '決策執行橋接'):
            _, exec_report = self.decision_execution_bridge.build()

        with self.progress.stage(3, '完成度缺口報告'):
            _, gap_report = self.completion_gap_report.build()

        with self.progress.stage(4, '完整進度報告'):
            _, _ = self.progress_full_report.build()

        headline = gap_report.get('headline', {})
        total_elapsed = int(time.time() - started)
        log('============================================================')
        log(f"✅ 主控執行完畢｜{render_progress_bar(1.0)}｜總耗時 {total_elapsed}s")
        log(f"📦 execution bridge rows_with_price={exec_report.get('rows_with_price', 0)} | rows_with_qty={exec_report.get('rows_with_qty', 0)} | passed={exec_report.get('rows_market_rule_passed', 0)}")
        log(f"🧭 非券商完成度：{headline.get('completion_excluding_real_broker_pct', 'n/a')}% | 尚餘硬缺口數：{headline.get('remaining_major_blocks_excluding_real_broker', 'n/a')}")
        log('============================================================')


def main():
    app = FormalTradingSystemV75()
    app.boot()
    app.run()


if __name__ == '__main__':
    main()
