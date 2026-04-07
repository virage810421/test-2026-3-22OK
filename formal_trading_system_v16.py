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

class FormalTradingSystemV16:
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

    def boot(self):
        log("=" * 60)
        log(f"🚀 啟動 {CONFIG.system_name}")
        log(f"🧭 模式：{CONFIG.mode}")
        log(f"🏦 broker_type：{CONFIG.broker_type}")
        log(f"🎬 execution_style：{CONFIG.execution_style}")
        log(f"🪜 bracket_exit：{CONFIG.enable_bracket_exit}")
        log(f"📈 目前整體升級進度：{self.progress_tracker.overall_percent()}%")
        log("=" * 60)
        if self.logger.connect():
            self.logger.ensure_tables()

    def run(self):
        package_check = self.package_guard.run()
        if CONFIG.strict_package_consistency and not package_check["passed"]:
            raise RuntimeError(f"套件版本不一致，請整包覆蓋同版檔案: {package_check['issues']}")

        recovery_info = self.recovery_manager.recover_if_possible() if CONFIG.enable_state_recovery else {"recovered": False, "reason": "disabled"}
        test_results = self.preflight_tests.run() if CONFIG.enable_preflight_tests else {}
        if test_results:
            log(f"🧪 preflight all_passed={test_results['all_passed']}")

        decision_path = resolve_decision_csv()
        log(f"📥 載入決策檔：{decision_path}")

        normalized_df, compat_info = self.compat_layer.normalize(decision_path)
        signals = self.signal_loader.load_from_normalized_df(normalized_df)
        readiness = self.readiness_checker.check(signals)
        log(f"✅ 讀入訊號：{len(signals)} 筆 | execution_ready={readiness['execution_ready']}")

        accepted, rejected = self.risk_gateway.filter_signals(signals)
        log(f"🛡️ 風控通過：{len(accepted)}")
        log(f"🚫 風控擋下：{len(rejected)}")
        for s, reason in rejected[:20]:
            log(f"   - {s.ticker} {s.action} 被拒：{reason}")

        execution_result = self.execution_engine.execute(accepted)
        account = self.broker.get_account_snapshot()
        log(f"💰 帳戶快照 | cash={account.cash:,.0f} | mv={account.market_value:,.0f} | equity={account.equity:,.0f} | exposure={account.exposure_ratio:.2%}")
        log(f"🎯 執行摘要 | filled={execution_result['filled']} | partial={execution_result['partially_filled']} | auto_exit={execution_result['auto_exit_signals']}")

        for ticker, pos in self.broker.get_positions().items():
            log(f"📌 持倉 {ticker} | qty={pos.qty} | avg={pos.avg_cost} | SL={pos.stop_loss_price} | TP={pos.take_profit_price} | high={pos.highest_price} | partialTP={pos.partial_tp_done} | note={pos.lifecycle_note}")

        state_path = self.state_store.save(
            cash=account.cash,
            positions=self.broker.get_positions(),
            last_prices=getattr(self.broker, "last_prices", {}),
            meta={"equity": account.equity, "exposure_ratio": account.exposure_ratio},
        )
        log(f"💾 已儲存 state：{state_path}")

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
        )
        log(f"📝 執行報告已輸出：{report_path}")

    def shutdown(self):
        self.logger.close()
        log("🛑 系統關閉。")

def main():
    try:
        app = FormalTradingSystemV16()
        app.boot()
        app.run()
        app.shutdown()
    except Exception as e:
        import traceback
        log("❌ 系統例外：")
        log(str(e))
        log(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
