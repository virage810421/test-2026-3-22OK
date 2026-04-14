# -*- coding: utf-8 -*-
from dataclasses import dataclass
from pathlib import Path
from typing import List
import os


def _split_env_list(name: str) -> List[Path]:
    raw = os.environ.get(name, '').strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(os.pathsep) if p.strip()]
    return [Path(p).expanduser() for p in parts]


def _detect_base_dir() -> Path:
    env_base = os.environ.get('FTS_BASE_DIR', '').strip()
    candidates = []
    if env_base:
        candidates.append(Path(env_base).expanduser())
    candidates.extend([Path.cwd(), Path(__file__).resolve().parent, Path(__file__).resolve().parent.parent])
    for c in candidates:
        if (c / 'daily_decision_desk.csv').exists() or (c / 'launcher.py').exists() or (c / 'master_pipeline.py').exists():
            return c
    return Path(__file__).resolve().parent


@dataclass
class AppPaths:
    base_dir: Path
    data_dir: Path
    log_dir: Path
    model_dir: Path
    state_dir: Path
    runtime_dir: Path
    decision_csv_candidates: List[Path]
    history_scan_dirs: List[Path]
    price_scan_dirs: List[Path]
    source_mount_dirs: List[Path]

    @classmethod
    def build(cls, base_dir: Path) -> 'AppPaths':
        data_dir = base_dir / 'data'
        log_dir = base_dir / 'logs'
        model_dir = base_dir / 'models'
        state_dir = base_dir / 'state'
        runtime_dir = base_dir / 'runtime'
        for d in [data_dir, log_dir, model_dir, state_dir, runtime_dir]:
            d.mkdir(exist_ok=True)
        source_mount_dirs = [d for d in _split_env_list('FTS_SOURCE_MOUNT_DIRS') if d.exists()]
        history_dirs = [d for d in _split_env_list('FTS_HISTORY_SCAN_DIRS') if d.exists()]
        price_dirs = [d for d in _split_env_list('FTS_PRICE_SCAN_DIRS') if d.exists()]
        merged_history = []
        merged_price = []
        for d in [base_dir, data_dir, *source_mount_dirs, *history_dirs]:
            if d.exists() and d not in merged_history:
                merged_history.append(d)
        for d in [base_dir, data_dir, *source_mount_dirs, *price_dirs]:
            if d.exists() and d not in merged_price:
                merged_price.append(d)
        return cls(
            base_dir=base_dir,
            data_dir=data_dir,
            log_dir=log_dir,
            model_dir=model_dir,
            state_dir=state_dir,
            runtime_dir=runtime_dir,
            decision_csv_candidates=[
                base_dir / 'daily_decision_desk.csv',
                data_dir / 'daily_decision_desk.csv',
                data_dir / 'normalized_decision_output.csv',
                data_dir / 'normalized_decision_output_enriched.csv',
            ],
            history_scan_dirs=merged_history,
            price_scan_dirs=merged_price,
            source_mount_dirs=source_mount_dirs,
        )


@dataclass
class SystemConfig:
    system_name: str = '正式交易主控版_v83_official_main'
    package_version: str = 'v84_phase4_fundgrade_three_layer_split'
    mode: str = 'PAPER'
    broker_type: str = 'paper'
    starting_cash: float = 3_000_000
    lot_size: int = 1000
    price_round: int = 2
    max_single_position_pct: float = 0.10
    max_order_notional: float = 500_000
    max_industry_exposure_pct: float = 0.25
    daily_loss_limit_pct: float = 0.03
    default_stop_loss_pct: float = 0.04
    default_take_profit_pct: float = 0.12
    commission_rate: float = 0.001425
    tax_rate_sell: float = 0.003
    slippage_bps: float = 8.0
    execution_style: str = 'TWAP3'
    current_bar_index: int = 0

    upstream_timeout_seconds: int = 3600
    run_ai_stage: bool = True
    dry_run_upstream_execution: bool = True
    allow_online_history_backfill: bool = False
    scan_recursive_depth: int = 3
    continue_on_stage_failure: bool = True
    safe_upgrade_mode: bool = True
    stage_soft_timeout_seconds: int = 120
    max_stage_retries: int = 1
    resume_completed_stages: bool = True

    allow_odd_lot_in_paper: bool = True
    paper_min_qty: int = 1
    partial_ready_price_threshold: float = 0.34
    partial_ready_qty_threshold: float = 0.34

    enable_retry_queue: bool = True
    max_retry_attempts: int = 3
    fail_on_retry_queue_required_items: bool = True

    live_manual_arm: bool = False
    require_dual_confirmation: bool = True
    enable_live_kill_switch: bool = True

    mock_broker_auto_connect: bool = True
    mock_broker_partial_fill_threshold_lots: int = 2
    mock_broker_reject_notional: float = 900_000
    mock_broker_callback_lag_seconds: int = 1

    fundamentals_csv_filename: str = 'market_financials_backup_fullspeed.csv'
    fundamentals_table_name: str = 'fundamentals_clean'
    fundamentals_target_reports_per_stock: int = 2
    fundamentals_enable_network_fetch: bool = False
    broker_adapter_config_filename: str = 'broker_adapter_config.json'

    db_driver: str = 'ODBC Driver 17 for SQL Server'
    db_server: str = 'localhost'
    db_database: str = '股票online'

    # ---- 補齊 execution config 欄位 ----
    trailing_stop_pct: float = 0.05
    enable_bracket_exit: bool = True
    partial_take_profit_ratio: float = 0.50
    break_even_after_partial_tp: bool = True
    max_holding_bars: int = 10
    position_cooldown_bars: int = 2

    # ---- 訓練 / live 一致性與策略治理 ----
    strict_feature_parity: bool = True
    selected_features_required_for_live: bool = True
    strategy_policy_mode: str = 'explicit'
    strategy_policy_filename: str = 'strategy_policy_book.json'
    model_layer_status_filename: str = 'model_layer_status.json'
    execution_layer_status_filename: str = 'execution_layer_status.json'
    strategy_layer_status_filename: str = 'strategy_policy_book.json'
    selected_features_min_count_for_live: int = 6
    selected_features_min_count_for_training: int = 8
    feature_manifest_filename: str = 'training_feature_manifest.json'
    feature_parity_require_runtime_artifacts: bool = True
    feature_artifact_max_age_days: int = 7
    force_shared_feature_universe: bool = False
    broker_contract_filename: str = 'broker_submission_contract.json'
    broker_requirements_filename: str = 'broker_requirements_contract.json'
    model_min_oot_pf: float = 1.0
    model_min_oot_hit_rate: float = 0.45
    model_min_promotion_score: float = 0.0

    # ---- 文件8：directional/range maturity safe overlay ----
    enable_directional_features_in_training: bool = True
    enable_directional_features_in_live: bool = True
    enable_directional_alpha_miner: bool = True
    enable_range_confidence_service: bool = True
    directional_backtest_safe_mode: bool = True
    live_feature_policy: str = 'shared_plus_vetted_directional'
    live_allow_directional_shared_fallback: bool = False
    live_directional_min_feature_count: int = 4
    directional_live_require_approved_features: bool = True
    directional_require_independent_lane_models: bool = True
    signal_path_fail_closed: bool = True
    target_return_unit: str = 'decimal_return'
    exit_model_require_position_day_samples: bool = True

    # ---- legacy confirmation influence guard ----
    legacy_confirm_influence: float = 0.0
    legacy_ai_proba_influence: float = 0.0
    legacy_score_alert_only: bool = True

    # ---- exit model / auto reduce-close workflow ----
    enable_exit_model_workflow: bool = True
    exit_model_primary: bool = True
    exit_model_min_features: int = 6
    exit_model_fallback_to_hazard: bool = False
    exit_model_hard_block_when_unavailable: bool = True
    exit_selected_features_filename: str = 'selected_features_exit.pkl'
    exit_defend_model_filename: str = 'exit_model_defend.pkl'
    exit_reduce_model_filename: str = 'exit_model_reduce.pkl'
    exit_confirm_model_filename: str = 'exit_model_confirm.pkl'
    active_positions_csv_filename: str = 'active_positions.csv'
    active_open_orders_csv_filename: str = 'open_orders_snapshot.csv'
    stop_replace_payload_filename: str = 'stop_replace_payloads.csv'
    enable_exit_stop_replace_workflow: bool = True
    exit_stop_replace_min_bps: int = 20
    exit_stop_min_gap_pct: float = 0.003
    exit_break_even_trigger_r: float = 0.80
    exit_break_even_buffer_pct: float = 0.0005
    exit_stop_workflow_allow_upsert: bool = True

    # ---- legacy detachment / service-first guard ----
    force_service_api_only: bool = True
    allow_legacy_facade_in_research: bool = False
    allow_legacy_facade_in_live: bool = False
    bridge_guard_fail_closed: bool = True
    prefer_sqlalchemy_db: bool = True


@dataclass
class DBConfig:
    driver: str = 'ODBC Driver 17 for SQL Server'
    server: str = 'localhost'
    database: str = '股票online'
    trusted_connection: str = 'yes'


BASE_DIR = _detect_base_dir()
PATHS = AppPaths.build(BASE_DIR)
CONFIG = SystemConfig()
DB = DBConfig()


# runtime hysteresis / execution SQL sync defaults
if not hasattr(CONFIG, 'regime_hysteresis_switch_band'): CONFIG.regime_hysteresis_switch_band = 0.08
if not hasattr(CONFIG, 'regime_hysteresis_confirm_bars'): CONFIG.regime_hysteresis_confirm_bars = 2
if not hasattr(CONFIG, 'regime_hysteresis_min_hold_bars'): CONFIG.regime_hysteresis_min_hold_bars = 2
if not hasattr(CONFIG, 'regime_hysteresis_tail_bars'): CONFIG.regime_hysteresis_tail_bars = 15
if not hasattr(CONFIG, 'enable_execution_sql_sync'): CONFIG.enable_execution_sql_sync = True
if not hasattr(CONFIG, 'execution_sql_sync_snapshots'): CONFIG.execution_sql_sync_snapshots = True
if not hasattr(CONFIG, 'execution_sql_sync_stop_orders'): CONFIG.execution_sql_sync_stop_orders = True


# v88 live-safe EV / no-lookahead gate defaults
try:
    CONFIG.live_min_expected_return = getattr(CONFIG, 'live_min_expected_return', -0.0015)
    CONFIG.model_layer_min_expected_return = getattr(CONFIG, 'model_layer_min_expected_return', CONFIG.live_min_expected_return)
    CONFIG.live_ev_min_sample_for_hard_block = getattr(CONFIG, 'live_ev_min_sample_for_hard_block', 8)
    CONFIG.live_ev_score_edge_scale = getattr(CONFIG, 'live_ev_score_edge_scale', 0.012)
    CONFIG.live_ev_proba_edge_scale = getattr(CONFIG, 'live_ev_proba_edge_scale', 0.050)
    CONFIG.live_ev_readiness_scale = getattr(CONFIG, 'live_ev_readiness_scale', 0.012)
    CONFIG.live_ev_risk_penalty_scale = getattr(CONFIG, 'live_ev_risk_penalty_scale', 0.010)
    CONFIG.live_ev_abs_cap = getattr(CONFIG, 'live_ev_abs_cap', 0.20)
except Exception:
    pass

# vNext lot-level / true-broker callback / execution reconciliation settings
try:
    CONFIG.lot_level_position_model_enabled = True
    CONFIG.lot_level_fifo_close = True
    CONFIG.execution_callback_ingest_enabled = True
    CONFIG.execution_reconciliation_enabled = True
    CONFIG.execution_reconciliation_write_sql = True
    CONFIG.execution_lot_snapshot_csv = 'execution_logs/position_lot_snapshot.csv'
    CONFIG.execution_callback_blotter_csv = 'execution_logs/broker_callback_blotter.csv'
    CONFIG.execution_reconciliation_blotter_csv = 'execution_logs/execution_reconciliation_blotter.csv'
except Exception:
    pass


# vNext institutional lot lifecycle settings
if not hasattr(CONFIG, "lot_accounting_method"): CONFIG.lot_accounting_method = "FIFO"
if not hasattr(CONFIG, "lot_partition_by_strategy"): CONFIG.lot_partition_by_strategy = True
if not hasattr(CONFIG, "lot_partition_by_signal"): CONFIG.lot_partition_by_signal = True
if not hasattr(CONFIG, "lot_allow_cross_strategy_close"): CONFIG.lot_allow_cross_strategy_close = False
if not hasattr(CONFIG, "lot_stop_linkage_enabled"): CONFIG.lot_stop_linkage_enabled = True
if not hasattr(CONFIG, "lot_stop_linkage_match_strategy"): CONFIG.lot_stop_linkage_match_strategy = True
if not hasattr(CONFIG, "lot_stop_linkage_match_signal"): CONFIG.lot_stop_linkage_match_signal = False
if not hasattr(CONFIG, "lot_track_partial_fill_lifecycle"): CONFIG.lot_track_partial_fill_lifecycle = True
if not hasattr(CONFIG, "lot_close_match_tolerance_qty"): CONFIG.lot_close_match_tolerance_qty = 0

# === vNext tax-lot jurisdiction / report / wash-sale rules ===
try:
    CONFIG.tax_lot_method = getattr(CONFIG, 'tax_lot_method', 'FIFO')
    CONFIG.tax_lot_currency = getattr(CONFIG, 'tax_lot_currency', 'TWD')
    CONFIG.tax_lot_long_term_days = getattr(CONFIG, 'tax_lot_long_term_days', 365)
    CONFIG.tax_lot_wash_sale_rule_enabled = getattr(CONFIG, 'tax_lot_wash_sale_rule_enabled', True)
    CONFIG.tax_lot_wash_sale_window_days = getattr(CONFIG, 'tax_lot_wash_sale_window_days', 30)
    CONFIG.tax_lot_specific_id_enabled = getattr(CONFIG, 'tax_lot_specific_id_enabled', True)
    CONFIG.tax_report_output_dir = getattr(CONFIG, 'tax_report_output_dir', 'runtime/tax_reports')
    CONFIG.tax_report_export_enabled = getattr(CONFIG, 'tax_report_export_enabled', True)
    CONFIG.tax_auto_classify_instrument = getattr(CONFIG, 'tax_auto_classify_instrument', True)
    CONFIG.tax_rule_tw_equity_currency = getattr(CONFIG, 'tax_rule_tw_equity_currency', 'TWD')
    CONFIG.tax_rule_us_equity_currency = getattr(CONFIG, 'tax_rule_us_equity_currency', 'USD')
    CONFIG.tax_rule_fx_currency = getattr(CONFIG, 'tax_rule_fx_currency', 'USD')
    CONFIG.tax_rule_futures_currency = getattr(CONFIG, 'tax_rule_futures_currency', 'USD')
except Exception:
    pass

# -----------------------------------------------------------------------------
# Formal architecture cleanup / observability / tax-rule config
# -----------------------------------------------------------------------------
FORMAL_CLASS_LAYER_ENABLED = True
THREE_PATH_DASHBOARD_ENABLED = True
BROKER_CALLBACK_MAPPING_ENABLED = True
BROKER_CALLBACK_MAPPING_PROFILE = "GENERIC_V1"
TAX_RULES_EXTERNAL_JSON_ENABLED = True
TAX_RULES_JSON_PATH = "config/tax_rules.json"
SMOKE_TEST_OUTPUT_PATH = "runtime/formal_healthcheck_smoke_report.json"
