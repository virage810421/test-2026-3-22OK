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
    live_allow_directional_shared_fallback: bool = True
    live_directional_min_feature_count: int = 4
    directional_live_require_approved_features: bool = True

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
