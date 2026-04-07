# -*- coding: utf-8 -*-
from dataclasses import asdict
from fts_config import CONFIG, DB
from fts_utils import log, resolve_decision_csv
from fts_progress import ProgressTracker, VersionPolicy
from fts_logger import SQLLogger
from fts_broker_factory import create_broker
from fts_signal import SignalLoader, ExecutionReadinessChecker
from fts_risk import RiskGateway
from fts_execution import ExecutionEngine
from fts_report import ReportBuilder
from fts_tests import PreflightTestSuite
from fts_state import StateStore, RecoveryManager
from fts_compat import DecisionCompatibilityLayer
from fts_package_guard import PackageConsistencyGuard
from fts_runtime_ops import RuntimeLock, HeartbeatWriter, DecisionArchiver, AuditTrail, ConfigSnapshotWriter
from fts_architecture_map import ArchitectureMapWriter
from fts_task_registry import TaskRegistry
from fts_orchestrator import UpstreamOrchestrator
from fts_retry_queue import RetryQueueManager
from fts_dashboard import HealthDashboardBuilder
from fts_daily_ops import DailyOpsSummaryBuilder
from fts_gatekeeper import LaunchGatekeeper
from fts_ai_pipeline import AIPipelineRegistry, AIPipelineInspector, AIDecisionBridge
from fts_ai_manager import AITrainingManager
from fts_model_gate import ModelVersionRegistry, ModelSelectionGate
from fts_live_safety import LiveSafetyGate
from fts_research_registry import ResearchSelectionRegistry

class FormalTradingSystemV32:
    def __init__(self):
        self.progress_tracker = ProgressTracker()
        self.version_policy = VersionPolicy()
        self.logger = SQLLogger(DB)
        self.broker = create_broker()
        self.signal_loader = SignalLoader()
        self.readiness_checker = ExecutionReadinessChecker()
        self.risk_gateway = RiskGateway(self.broker)
        self.execution_engine = ExecutionEngine(self.broker, self.logger)
        self.report_builder = ReportBuilder(self.progress_tracker, self.version_policy)
        self.preflight_tests = PreflightTestSuite()
        self.state_store = StateStore()
        self.recovery_manager = RecoveryManager(self.broker, self.state_store)
        self.compat_layer = DecisionCompatibilityLayer()
        self.package_guard = PackageConsistencyGuard()
        self.runtime_lock = RuntimeLock()
        self.heartbeat = HeartbeatWriter()
        self.archiver = DecisionArchiver()
        self.audit = AuditTrail()
        self.config_snapshot = ConfigSnapshotWriter()
        self.architecture_map = ArchitectureMapWriter()
        self.task_registry = TaskRegistry()
        self.orchestrator = UpstreamOrchestrator()
        self.retry_queue = RetryQueueManager()
        self.dashboard = HealthDashboardBuilder()
        self.daily_ops = DailyOpsSummaryBuilder()
        self.gatekeeper = LaunchGatekeeper()
        self.ai_registry = AIPipelineRegistry()
        self.ai_inspector = AIPipelineInspector()
        self.ai_bridge = AIDecisionBridge()
        self.ai_manager = AITrainingManager()
        self.model_registry = ModelVersionRegistry()
        self.model_gate = ModelSelectionGate()
        self.live_safety = LiveSafetyGate()
        self.research_registry = ResearchSelectionRegistry()

    def boot(self):
        log("=" * 60)
        log(f"🚀 啟動 正式交易主控版_v32")
        log(f"🧭 模式：{CONFIG.mode}")
        log(f"🏦 broker_type：{CONFIG.broker_type}")
        log(f"🎬 execution_style：{CONFIG.execution_style}")
        log(f"🛡️ paper/live safety：ON")
        log(f"📈 目前整體升級進度：{self.progress_tracker.overall_percent()}%")
        log("=" * 60)

        if getattr(CONFIG, "enable_runtime_lock", False):
            self.runtime_lock.acquire()
        if getattr(CONFIG, "write_config_snapshot", False):
            self.config_snapshot.write()
        if getattr(CONFIG, "enable_heartbeat", False):
            self.heartbeat.write("boot")
        self.architecture_map.write()
        self.task_registry.write()
        self.ai_registry_path, self.ai_registry_payload = self.ai_registry.build()
        self.ai_status_path, self.ai_status = self.ai_inspector.inspect()
        self.ai_bridge_path, self.ai_bridge_summary = self.ai_bridge.build_summary()
        self.ai_manager_info = self.ai_manager.inspect()
        self.model_registry_path, self.model_registry_payload = self.model_registry.build()
        self.research_registry_path, self.research_registry_payload = self.research_registry.build()
        self.audit.append("boot", {"system_name": CONFIG.system_name, "package_version": getattr(CONFIG, "package_version", "v32")})

        if self.logger.connect():
            self.logger.ensure_tables()

    def run(self):
        package_check = self.package_guard.run()
        self.audit.append("package_check", package_check)
        if getattr(CONFIG, "strict_package_consistency", False) and not package_check["passed"]:
            raise RuntimeError(f"套件版本不一致，請整包覆蓋同版檔案: {package_check['issues']}")

        ai_exec = self.ai_manager.maybe_run_training_stage()
        self.audit.append("ai_training_exec", ai_exec)

        retry_before = self.retry_queue.summarize()
        retry_exec = {"executed": [], "failed": [], "skipped": []}
        if getattr(CONFIG, "enable_auto_retry_on_boot", False):
            retryable = self.retry_queue.list_retryable_items()
            if retryable:
                log(f"🔁 準備自動補跑 retry queue | retryable={len(retryable)}")
                retry_exec = self.orchestrator.execute_retry_items(retryable)
                for row in retry_exec.get("executed", []):
                    key = f"{row.get('stage')}::{row.get('name')}::{row.get('script')}"
                    self.retry_queue.mark_success(key)
        retry_after = self.retry_queue.summarize()
        self.audit.append("retry_boot", {"before": retry_before, "retry_exec": retry_exec, "after": retry_after})

        registry_summary = self.task_registry.summary()
        upstream_status = self.orchestrator.check_tasks(registry_summary)
        self.audit.append("upstream_status", upstream_status)

        upstream_exec = self.orchestrator.execute_tasks(registry_summary)
        self.audit.append("upstream_exec", upstream_exec)

        self.retry_queue.add_failed_tasks(upstream_exec.get("failed", []))
        queue_state = self.retry_queue.summarize()
        self.audit.append("retry_queue", queue_state)

        recovery_info = self.recovery_manager.recover_if_possible() if getattr(CONFIG, "enable_state_recovery", False) else {"recovered": False, "reason": "disabled"}
        self.audit.append("recovery", recovery_info)

        test_results = self.preflight_tests.run() if getattr(CONFIG, "enable_preflight_tests", False) else {}
        if test_results:
            log(f"🧪 preflight all_passed={test_results['all_passed']}")
            self.audit.append("preflight", test_results)

        decision_path = resolve_decision_csv()
        log(f"📥 載入決策檔：{decision_path}")
        archived = None
        if getattr(CONFIG, "archive_decision_input", False):
            archived = self.archiver.archive(decision_path)
        self.audit.append("decision_input", {"source": str(decision_path), "archived": str(archived) if archived else None})

        normalized_df, compat_info = self.compat_layer.normalize(decision_path)
        self.audit.append("compat", compat_info)

        signals = self.signal_loader.load_from_normalized_df(normalized_df)
        readiness = self.readiness_checker.check(signals)
        log(f"✅ 讀入訊號：{len(signals)} 筆 | execution_ready={readiness['execution_ready']}")
        self.audit.append("readiness", readiness)

        self.model_gate_path, model_gate = self.model_gate.evaluate(self.ai_status, readiness)
        self.audit.append("model_gate", model_gate)

        gate = self.gatekeeper.evaluate(
            upstream_status=upstream_status,
            upstream_exec=upstream_exec,
            retry_queue=queue_state,
            compat_info=compat_info,
            readiness=readiness,
        )
        self.audit.append("launch_gate", gate)

        self.live_safety_path, live_safety = self.live_safety.evaluate(readiness, gate)
        self.audit.append("live_safety_gate", live_safety)

        accepted, rejected = [], []
        execution_result = {"submitted": 0, "filled": 0, "partially_filled": 0, "rejected": 0, "cancelled": 0, "fills_count": 0, "auto_exit_signals": 0, "reconciliation": {}}
        account = None
        position_rows = []

        if gate.get("go_for_execution", False) and live_safety.get("paper_live_safe", False):
            accepted, rejected = self.risk_gateway.filter_signals(signals)
            log(f"🛡️ 風控通過：{len(accepted)}")
            log(f"🚫 風控擋下：{len(rejected)}")
            self.audit.append("risk_result", {"accepted": len(accepted), "rejected": len(rejected)})
            for s, reason in rejected[:20]:
                log(f"   - {s.ticker} {s.action} 被拒：{reason}")

            if getattr(CONFIG, "enable_heartbeat", False):
                self.heartbeat.write("execution_start", {"accepted": len(accepted)})

            execution_result = self.execution_engine.execute(accepted)
            account = self.broker.get_account_snapshot()
            log(f"💰 帳戶快照 | cash={account.cash:,.0f} | mv={account.market_value:,.0f} | equity={account.equity:,.0f} | exposure={account.exposure_ratio:.2%}")
            log(f"🎯 執行摘要 | filled={execution_result['filled']} | partial={execution_result['partially_filled']} | auto_exit={execution_result['auto_exit_signals']}")
            self.audit.append("execution_result", execution_result)

            for ticker, pos in self.broker.get_positions().items():
                log(f"📌 持倉 {ticker} | qty={pos.qty} | avg={pos.avg_cost} | SL={pos.stop_loss_price} | TP={pos.take_profit_price} | high={pos.highest_price} | partialTP={pos.partial_tp_done} | note={pos.lifecycle_note}")
                position_rows.append(asdict(pos))
        else:
            log("⛔ Launch Gate 或 Live Safety Gate 阻擋本輪 execution，已跳過送單。")
            self.audit.append("risk_result", {"accepted": 0, "rejected": 0, "blocked_by_gate": True})
            account = self.broker.get_account_snapshot()

        dashboard_path, dashboard = self.dashboard.build(
            upstream_status=upstream_status,
            upstream_exec=upstream_exec,
            retry_queue=queue_state,
            readiness=readiness,
            execution_result=execution_result,
            positions=position_rows,
        )
        self.audit.append("dashboard_saved", {"path": str(dashboard_path)})

        summary_path, alerts_path, daily_summary = self.daily_ops.build(dashboard)
        self.audit.append("daily_ops_saved", {"summary_path": str(summary_path), "alerts_path": str(alerts_path)})

        state_path = self.state_store.save(
            cash=account.cash,
            positions=self.broker.get_positions(),
            last_prices=getattr(self.broker, "last_prices", {}),
            meta={"equity": account.equity, "exposure_ratio": account.exposure_ratio},
        )
        log(f"💾 已儲存 state：{state_path}")
        self.audit.append("state_saved", {"path": str(state_path)})

        report_path = self.report_builder.save(
            package_check=package_check,
            compat_info=compat_info,
            readiness=readiness,
            test_results=test_results,
            accepted=accepted,
            rejected=rejected,
            execution_result=execution_result,
            account_snapshot=account,
            positions=self.broker.get_positions(),
            decision_path=decision_path,
            recovery_info=recovery_info,
            stage_results={
                "launch_gate": gate,
                "live_safety_gate": live_safety,
                "model_gate": model_gate,
                "ai_training_exec": ai_exec,
                "retry_boot": {"before": retry_before, "retry_exec": retry_exec, "after": retry_after},
                "upstream_status": upstream_status,
                "upstream_exec": upstream_exec,
                "retry_queue": queue_state,
                "dashboard_path": str(dashboard_path),
                "daily_ops_summary_path": str(summary_path),
                "alerts_path": str(alerts_path),
                "ai_pipeline_registry_path": str(self.ai_registry_path),
                "ai_pipeline_status_path": str(self.ai_status_path),
                "ai_decision_bridge_path": str(self.ai_bridge_path),
                "model_registry_path": str(self.model_registry_path),
                "model_gate_path": str(self.model_gate_path),
                "research_registry_path": str(self.research_registry_path),
                "live_safety_gate_path": str(self.live_safety_path),
                "ai_pipeline_status": self.ai_status,
            },
        )
        log(f"📝 執行報告已輸出：{report_path}")
        self.audit.append("report_saved", {"path": str(report_path)})

        if getattr(CONFIG, "enable_heartbeat", False):
            self.heartbeat.write("run_complete", {
                "report_path": str(report_path),
                "dashboard_path": str(dashboard_path),
                "daily_ops_summary_path": str(summary_path),
                "launch_gate_go": gate.get("go_for_execution", False),
                "model_gate_go": model_gate.get("go_for_model_linkage", False),
                "live_safety_go": live_safety.get("paper_live_safe", False),
            })

    def shutdown(self):
        self.logger.close()
        if getattr(CONFIG, "enable_runtime_lock", False):
            self.runtime_lock.release()
        self.audit.append("shutdown", {"ok": True})
        log("🛑 系統關閉。")

def main():
    app = FormalTradingSystemV32()
    try:
        app.boot()
        app.run()
        app.shutdown()
    except Exception as e:
        import traceback
        log("❌ 系統例外：")
        log(str(e))
        log(traceback.format_exc())
        try:
            app.audit.append("crash", {"error": str(e)})
            if getattr(CONFIG, "enable_heartbeat", False):
                app.heartbeat.write("crash", {"error": str(e)})
        except Exception:
            pass
        try:
            if getattr(CONFIG, "enable_runtime_lock", False):
                app.runtime_lock.release()
        except Exception:
            pass
        raise

if __name__ == "__main__":
    main()
