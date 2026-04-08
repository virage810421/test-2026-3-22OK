# -*- coding: utf-8 -*-
import json
import time
from pathlib import Path
from fts_training_orchestrator import TrainingOrchestrator
from fts_decision_execution_bridge import DecisionExecutionBridge
from fts_completion_gap_report import CompletionGapReportBuilder
from fts_progress_full_report import ProgressFullReport
from fts_utils import log, StageProgress, render_progress_bar
from fts_resilience import StageGuard
from fts_config import PATHS, CONFIG


class FormalTradingSystemV77:
    def __init__(self):
        self.training_orchestrator = TrainingOrchestrator()
        self.decision_execution_bridge = DecisionExecutionBridge()
        self.completion_gap_report = CompletionGapReportBuilder()
        self.progress_full_report = ProgressFullReport()
        self.progress = StageProgress(total_stages=4, heartbeat_seconds=2.0)
        self.guard = StageGuard()

    def boot(self):
        log('=' * 60)
        log('🚀 啟動 正式交易主控版_v77')
        log('🛡️ v77 safe-upgrade++：retry + fallback + resume checkpoint + soft-timeout warn：ON')
        log(f'📊 {render_progress_bar(0.0)} 系統啟動完成，準備進入 4 個階段')
        log('=' * 60)

    def _fallback_runtime_json(self, filename: str, default_payload: dict):
        def _fn():
            p = PATHS.runtime_dir / filename
            if p.exists():
                return p, json.loads(p.read_text(encoding='utf-8'))
            p.write_text(json.dumps(default_payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return p, default_payload
        return _fn

    def run(self):
        started = time.time()
        results = {}

        with self.progress.stage(1, '訓練治理檢查'):
            results['training'] = self.guard.run(
                'training_orchestrator',
                '訓練治理檢查',
                lambda: self.training_orchestrator.maybe_execute(),
                fallback_fn=self._fallback_runtime_json('training_orchestrator.json', {
                    'status': 'fallback_stub',
                    'dataset': {'exists': False},
                    'models': {'all_required_present': False},
                }),
            )

        with self.progress.stage(2, '決策執行橋接'):
            results['execution'] = self.guard.run(
                'decision_execution_bridge',
                '決策執行橋接',
                lambda: self.decision_execution_bridge.build(),
                fallback_fn=self._fallback_runtime_json('decision_execution_bridge.json', {
                    'status': 'fallback_stub',
                    'rows_with_price': 0,
                    'rows_with_qty': 0,
                    'rows_market_rule_passed': 0,
                }),
            )

        with self.progress.stage(3, '完成度缺口報告'):
            results['gap'] = self.guard.run(
                'completion_gap_report',
                '完成度缺口報告',
                lambda: self.completion_gap_report.build(),
                fallback_fn=self._fallback_runtime_json('completion_gap_report.json', {
                    'status': 'fallback_stub',
                    'headline': {
                        'completion_excluding_real_broker_pct': 0,
                        'remaining_major_blocks_excluding_real_broker': 3,
                    },
                    'remaining_excluding_real_broker': [
                        'AI訓練資料與模型產物未落地',
                        '決策價格/股數/台股規則 payload 未閉環',
                        'Paper execution 端到端尚未放行',
                    ],
                }),
            )

        with self.progress.stage(4, '完整進度報告'):
            results['progress'] = self.guard.run(
                'progress_full_report',
                '完整進度報告',
                lambda: self.progress_full_report.build(),
                fallback_fn=self._fallback_runtime_json('progress_full_report.json', {
                    'status': 'fallback_stub',
                    'note': 'progress report builder failed; fallback payload generated',
                }),
            )

        exec_report = self._load_json(PATHS.runtime_dir / 'decision_execution_bridge.json')
        gap_report = self._load_json(PATHS.runtime_dir / 'completion_gap_report.json')
        headline = gap_report.get('headline', {}) if isinstance(gap_report, dict) else {}
        total_elapsed = int(time.time() - started)
        manifest = {
            'package_version': CONFIG.package_version,
            'safe_upgrade_mode': CONFIG.safe_upgrade_mode,
            'continue_on_stage_failure': CONFIG.continue_on_stage_failure,
            'resume_completed_stages': CONFIG.resume_completed_stages,
            'max_stage_retries': CONFIG.max_stage_retries,
            'stage_soft_timeout_seconds': CONFIG.stage_soft_timeout_seconds,
            'elapsed_seconds': total_elapsed,
            'rows_with_price': exec_report.get('rows_with_price', 0),
            'rows_with_qty': exec_report.get('rows_with_qty', 0),
            'rows_market_rule_passed': exec_report.get('rows_market_rule_passed', 0),
            'completion_excluding_real_broker_pct': headline.get('completion_excluding_real_broker_pct', 0),
            'remaining_major_blocks_excluding_real_broker': headline.get('remaining_major_blocks_excluding_real_broker', 0),
        }
        manifest_path = PATHS.runtime_dir / 'run_manifest_v77.json'
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
        log('============================================================')
        log(f"✅ 主控執行完畢｜{render_progress_bar(1.0)}｜總耗時 {total_elapsed}s")
        log(f"📦 execution bridge rows_with_price={exec_report.get('rows_with_price', 0)} | rows_with_qty={exec_report.get('rows_with_qty', 0)} | passed={exec_report.get('rows_market_rule_passed', 0)}")
        log(f"🧭 非券商完成度：{headline.get('completion_excluding_real_broker_pct', 'n/a')}% | 尚餘硬缺口數：{headline.get('remaining_major_blocks_excluding_real_broker', 'n/a')}")
        log(f"🧯 Safe Upgrade：continue_on_stage_failure={CONFIG.continue_on_stage_failure} | resume_completed_stages={CONFIG.resume_completed_stages} | retries={CONFIG.max_stage_retries}")
        log(f"🗂️ manifest={manifest_path} | checkpoint={PATHS.state_dir / f'stage_checkpoints_{CONFIG.package_version}.json'}")
        log('============================================================')

    @staticmethod
    def _load_json(path: Path) -> dict:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception:
            return {}


def main():
    app = FormalTradingSystemV77()
    app.boot()
    app.run()


if __name__ == '__main__':
    main()
