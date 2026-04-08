# -*- coding: utf-8 -*-
from pathlib import Path
from fts_config import CONFIG, DB
from fts_utils import log, resolve_decision_csv
from fts_progress import ProgressTracker, VersionPolicy
from fts_logger import SQLLogger
from fts_broker_factory import create_broker
from fts_signal import SignalLoader, ExecutionReadinessChecker
from fts_report import ReportBuilder
from fts_tests import PreflightTestSuite
from fts_state import StateStore
from fts_compat import DecisionCompatibilityLayer
from fts_package_guard import PackageConsistencyGuard
from fts_runtime_ops_v59_patch import RuntimeLock
from fts_runtime_ops import HeartbeatWriter, ConfigSnapshotWriter
from fts_architecture_map import ArchitectureMapWriter
from fts_task_registry import TaskRegistry
from fts_orchestrator import UpstreamOrchestrator
from fts_dashboard import HealthDashboardBuilder
from fts_daily_ops import DailyOpsSummaryBuilder
from fts_gatekeeper import LaunchGatekeeper
from fts_ai_pipeline import AIPipelineRegistry, AIPipelineInspector, AIDecisionBridge
from fts_ai_manager import AITrainingManager
from fts_model_gate import ModelVersionRegistry
from fts_research_registry import ResearchSelectionRegistry
from fts_interface_audit import InterfaceAuditBuilder
from fts_stage_trace import StageTraceWriter
from fts_console_brief import ConsoleBriefBuilder
from fts_run_manifest import RunManifestBuilder
from fts_target95_scorecard import Target95Scorecard
from fts_target95_plan import Target95Planner
from fts_training_prod_readiness import TrainingProdReadinessBuilder
from fts_training_gap_report import TrainingGapReportBuilder
from fts_trainer_promotion_policy import TrainerPromotionPolicyBuilder
from fts_legacy_core_upgrade_plan import LegacyCoreUpgradePlanBuilder
from fts_legacy_core_readiness_board import LegacyCoreReadinessBoardBuilder
from fts_legacy_core_upgrade_wave import LegacyCoreUpgradeWaveBuilder
from fts_wave1_core_upgrade import Wave1CoreUpgradeBuilder
from fts_legacy_core_metrics import LegacyCoreMetricsBuilder
from fts_wave1_contract_pack import Wave1ContractPackBuilder
from fts_wave1_body_upgrade_templates import Wave1BodyUpgradeTemplatesBuilder
from fts_wave1_io_bindings import Wave1IOBindingsBuilder
from fts_wave1_upgrade_checklist import Wave1UpgradeChecklistBuilder
from fts_real_api_readiness import RealAPIReadinessBuilder
from fts_decision_price_bridge_plus import DecisionPriceBridgePlus
from fts_upgrade_truth_report import UpgradeTruthReportBuilder
from fts_live_readiness_gate import LiveReadinessGate
from fts_training_orchestrator import TrainingOrchestrator
from fts_decision_execution_bridge import DecisionExecutionBridge
from fts_completion_gap_report import CompletionGapReportBuilder
from fts_progress_full_report import ProgressFullReportBuilder
from fts_local_history_bootstrap import LocalHistoryBootstrap

class FormalTradingSystemV69:
    def __init__(self):
        self.progress_tracker = ProgressTracker()
        self.version_policy = VersionPolicy()
        self.logger = SQLLogger(DB)
        self.broker = create_broker()
        self.signal_loader = SignalLoader()
        self.readiness_checker = ExecutionReadinessChecker()
        self.report_builder = ReportBuilder(self.progress_tracker, self.version_policy)
        self.preflight_tests = PreflightTestSuite()
        self.state_store = StateStore()
        self.compat_layer = DecisionCompatibilityLayer()
        self.package_guard = PackageConsistencyGuard()
        self.runtime_lock = RuntimeLock()
        self.heartbeat = HeartbeatWriter()
        self.config_snapshot = ConfigSnapshotWriter()
        self.architecture_map = ArchitectureMapWriter()
        self.task_registry = TaskRegistry()
        self.orchestrator = UpstreamOrchestrator()
        self.dashboard = HealthDashboardBuilder()
        self.daily_ops = DailyOpsSummaryBuilder()
        self.gatekeeper = LaunchGatekeeper()
        self.ai_registry = AIPipelineRegistry()
        self.ai_inspector = AIPipelineInspector()
        self.ai_bridge = AIDecisionBridge()
        self.ai_manager = AITrainingManager()
        self.model_registry = ModelVersionRegistry()
        self.research_registry = ResearchSelectionRegistry()
        self.interface_audit = InterfaceAuditBuilder()
        self.stage_trace = StageTraceWriter()
        self.console_brief = ConsoleBriefBuilder()
        self.run_manifest = RunManifestBuilder()
        self.target95_scorecard = Target95Scorecard()
        self.target95_plan = Target95Planner()
        self.training_prod_readiness = TrainingProdReadinessBuilder()
        self.training_gap_report = TrainingGapReportBuilder()
        self.trainer_promotion_policy = TrainerPromotionPolicyBuilder()
        self.legacy_core_upgrade_plan = LegacyCoreUpgradePlanBuilder()
        self.legacy_core_readiness_board = LegacyCoreReadinessBoardBuilder()
        self.legacy_core_upgrade_wave = LegacyCoreUpgradeWaveBuilder()
        self.wave1_core_upgrade = Wave1CoreUpgradeBuilder()
        self.legacy_core_metrics = LegacyCoreMetricsBuilder()
        self.wave1_contract_pack = Wave1ContractPackBuilder()
        self.wave1_body_upgrade_templates = Wave1BodyUpgradeTemplatesBuilder()
        self.wave1_io_bindings = Wave1IOBindingsBuilder()
        self.wave1_upgrade_checklist = Wave1UpgradeChecklistBuilder()
        self.real_api_readiness = RealAPIReadinessBuilder()
        self.decision_price_bridge_plus = DecisionPriceBridgePlus()
        self.upgrade_truth_report = UpgradeTruthReportBuilder()
        self.live_readiness_gate = LiveReadinessGate()
        self.training_orchestrator = TrainingOrchestrator()
        self.decision_execution_bridge = DecisionExecutionBridge()
        self.completion_gap_report = CompletionGapReportBuilder()
        self.progress_full_report = ProgressFullReportBuilder()
        self.local_history_bootstrap = LocalHistoryBootstrap()

    def boot(self):
        log("=" * 60)
        log("🚀 啟動 正式交易主控版_v69")
        log(f"🧭 模式：{CONFIG.mode}")
        log(f"🏦 broker_type：{CONFIG.broker_type}")
        log("🧱 v69 local history bootstrap + auto price scan：ON")
        log(f"📈 目前整體升級進度：{self.progress_tracker.overall_percent()}%")
        log("=" * 60)
        if getattr(CONFIG, "enable_runtime_lock", False):
            self.runtime_lock.acquire()
        if getattr(CONFIG, "write_config_snapshot", False):
            self.config_snapshot.write()
        self.architecture_map.write()
        self.task_registry.write()
        self.ai_registry.build()
        self.ai_inspector.inspect()
        self.ai_bridge.build_summary()
        self.model_registry.build()
        self.research_registry.build()
        self.interface_audit.build()
        self.training_prod_readiness.build()
        self.training_gap_report.build()
        self.trainer_promotion_policy.build()
        self.legacy_core_upgrade_plan.build()
        self.legacy_core_readiness_board.build()
        self.legacy_core_upgrade_wave.build()
        self.wave1_core_upgrade.build()
        self.legacy_core_metrics.build()
        self.wave1_contract_pack.build()
        self.wave1_body_upgrade_templates.build()
        self.real_api_readiness.build()
        self.decision_price_bridge_plus.build()
        self.upgrade_truth_report.build()
        self.wave1_io_bindings.build()
        self.wave1_upgrade_checklist.build()
        self.target95_scorecard.build(self.progress_tracker.modules)
        self.target95_plan.build(self.progress_tracker.modules)
        self.completion_gap_report.build()
        self.local_history_bootstrap.build()
        if self.logger.connect():
            self.logger.ensure_tables()

    def run(self):
        package_check = self.package_guard.run()
        ai_exec = self.ai_manager.maybe_run_training_stage()
        training_orch = self.training_orchestrator.maybe_execute()
        decision_path = resolve_decision_csv()
        normalized_df, compat_info = self.compat_layer.normalize(decision_path)
        self.decision_execution_bridge.build()
        exec_payload_path = Path(self.decision_execution_bridge.output_path)
        if exec_payload_path.exists():
            import pandas as pd
            signal_df = pd.read_csv(exec_payload_path, encoding="utf-8-sig")
        else:
            signal_df = normalized_df
        signals = self.signal_loader.load_from_normalized_df(signal_df)
        readiness = self.readiness_checker.check(signals)
        self.live_readiness_gate.evaluate(signal_df)
        test_results = self.preflight_tests.run() if getattr(CONFIG, "enable_preflight_tests", False) else {}
        upstream_status = self.orchestrator.check_tasks(self.task_registry.summary())
        upstream_exec = self.orchestrator.execute_tasks(self.task_registry.summary())
        gate = self.gatekeeper.evaluate(upstream_status=upstream_status, upstream_exec=upstream_exec, retry_queue={}, compat_info=compat_info, readiness=readiness)
        execution_result = {"submitted": 0, "filled": 0, "partially_filled": 0, "rejected": 0, "cancelled": 0, "fills_count": 0, "auto_exit_signals": 0, "reconciliation": {}}
        account = self.broker.get_account_snapshot()
        accepted_count = int(readiness.get("total_signals", 0) or 0)
        rejected_count = max(int(compat_info.get("row_count", 0) or 0) - accepted_count, 0)
        self.stage_trace.build(ai_exec=ai_exec | {"training_orchestrator": training_orch}, compat_info=compat_info, readiness=readiness, accepted_count=accepted_count, rejected_count=rejected_count, execution_result=execution_result)
        empty_gate = {"go_for_decision_linkage": False, "go_for_model_linkage": False, "paper_live_safe": CONFIG.mode != "LIVE", "go_for_broker_submission": False, "go_for_submission_contract": False}
        self.console_brief.build(ai_exec=ai_exec | {"training_orchestrator": training_orch}, compat_info=compat_info, readiness=readiness, research_gate=empty_gate, model_gate=empty_gate, launch_gate=gate, live_safety=empty_gate, broker_approval=empty_gate, submission_gate=empty_gate, execution_result=execution_result)
        positions = self.broker.get_positions() if hasattr(self.broker, "get_positions") else {}
        accepted = []
        rejected = []
        self.report_builder.save(package_check=package_check, compat_info=compat_info, readiness=readiness, test_results=test_results, accepted=accepted, rejected=rejected, execution_result=execution_result, account_snapshot=account, positions=positions, decision_path=decision_path, stage_results={"training_orchestrator": training_orch, "launch_gate": gate})
        if getattr(CONFIG, "enable_heartbeat", False):
            self.heartbeat.write(stage="ok", extra={"signal_count": readiness.get("total_signals", 0)})
        dashboard_path, dashboard_payload = self.dashboard.build(upstream_status=upstream_status, upstream_exec=upstream_exec, retry_queue={}, readiness=readiness, execution_result=execution_result, positions=list(positions.values()) if isinstance(positions, dict) else positions)
        self.daily_ops.build(dashboard_payload)
        generated_paths = {
            "training_orchestrator": str(self.training_orchestrator.report_path),
            "local_history_bootstrap": str(self.local_history_bootstrap.report_path),
            "training_bootstrap_recipe": str(self.training_orchestrator.recipe_path),
            "decision_execution_bridge": str(self.decision_execution_bridge.report_path),
            "manual_price_template": str(self.decision_execution_bridge.price_template_path),
            "progress_full_report_json": str(self.progress_full_report.json_path),
            "progress_full_report_md": str(self.progress_full_report.md_path),
        }
        self.run_manifest.build(ai_exec=ai_exec | {"training_orchestrator": training_orch}, compat_info=compat_info, readiness=readiness, accepted_count=accepted_count, rejected_count=rejected_count, execution_result=execution_result, generated_paths=generated_paths)
        self.progress_full_report.build()
        log("✅ v69 主控完成：已補 local history bootstrap / auto price scan / full progress report")
        return {
            "package_check": package_check,
            "readiness": readiness,
            "gate": gate,
        }

if __name__ == "__main__":
    app = FormalTradingSystemV69()
    app.boot()
    app.run()
