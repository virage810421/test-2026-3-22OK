# -*- coding: utf-8 -*-
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

class FormalTradingSystemV25:
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

    def boot(self):
        log("=" * 60)
        log(f"🚀 啟動 正式交易主控版_v25")
        log(f"🧭 模式：{CONFIG.mode}")
        log(f"🏦 broker_type：{CONFIG.broker_type}")
        log(f"🎬 execution_style：{CONFIG.execution_style}")
        log(f"🗃️ task log archive：ON")
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
        self.audit.append("boot", {"system_name": CONFIG.system_name, "package_version": getattr(CONFIG, "package_version", "v25")})

        if self.logger.connect():
            self.logger.ensure_tables()

    def run(self):
        package_check = self.package_guard.run()
        self.audit.append("package_check", package_check)
        if getattr(CONFIG, "strict_package_consistency", False) and not package_check["passed"]:
            raise RuntimeError(f"套件版本不一致，請整包覆蓋同版檔案: {package_check['issues']}")

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
                "retry_boot": {"before": retry_before, "retry_exec": retry_exec, "after": retry_after},
                "upstream_status": upstream_status,
                "upstream_exec": upstream_exec,
                "retry_queue": queue_state,
            },
        )
        log(f"📝 執行報告已輸出：{report_path}")
        self.audit.append("report_saved", {"path": str(report_path)})

        if getattr(CONFIG, "enable_heartbeat", False):
            self.heartbeat.write("run_complete", {"report_path": str(report_path)})

    def shutdown(self):
        self.logger.close()
        if getattr(CONFIG, "enable_runtime_lock", False):
            self.runtime_lock.release()
        self.audit.append("shutdown", {"ok": True})
        log("🛑 系統關閉。")

def main():
    app = FormalTradingSystemV25()
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
