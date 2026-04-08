# -*- coding: utf-8 -*-
from dataclasses import dataclass
from pathlib import Path
from typing import List
import os


def _detect_base_dir() -> Path:
    env_base = os.environ.get("FTS_BASE_DIR", "").strip()
    candidates = []
    if env_base:
        candidates.append(Path(env_base).expanduser())
    candidates.extend([
        Path.cwd(),
        Path(__file__).resolve().parent,
        Path(__file__).resolve().parent.parent,
    ])
    for c in candidates:
        if (c / "daily_decision_desk.csv").exists() or (c / "launcher.py").exists() or (c / "master_pipeline.py").exists():
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

    @classmethod
    def build(cls, base_dir: Path) -> "AppPaths":
        data_dir = base_dir / "data"
        log_dir = base_dir / "logs"
        model_dir = base_dir / "models"
        state_dir = base_dir / "state"
        runtime_dir = base_dir / "runtime"
        data_dir.mkdir(exist_ok=True)
        log_dir.mkdir(exist_ok=True)
        model_dir.mkdir(exist_ok=True)
        state_dir.mkdir(exist_ok=True)
        runtime_dir.mkdir(exist_ok=True)
        return cls(
            base_dir=base_dir,
            data_dir=data_dir,
            log_dir=log_dir,
            model_dir=model_dir,
            state_dir=state_dir,
            runtime_dir=runtime_dir,
            decision_csv_candidates=[
                base_dir / "daily_decision_desk.csv",
                data_dir / "daily_decision_desk.csv",
                data_dir / "daily_decision_desk_ai.csv",
                data_dir / "decision_output.csv",
                data_dir / "normalized_decision_output.csv",
            ],
        )

@dataclass
class SystemConfig:
    system_name: str = "正式交易主控版_v64"
    package_version: str = "v64"
    mode: str = "PAPER"
    broker_type: str = "paper"
    starting_cash: float = 3_000_000
    lot_size: int = 1000
    slippage_bps: float = 8
    commission_rate: float = 0.001425
    tax_rate_sell: float = 0.003
    price_round: int = 2
    max_orders_per_run: int = 8
    max_single_position_pct: float = 0.10
    max_total_exposure_pct: float = 0.70
    cash_buffer_pct: float = 0.15
    max_industry_exposure_pct: float = 0.25
    min_score_to_trade: float = 60.0
    min_ai_confidence: float = 0.55
    min_expected_return: float = -0.05
    min_kelly_fraction: float = 0.00
    allow_partial_fill: bool = True
    partial_fill_ratio: float = 0.35
    enable_preflight_tests: bool = True
    enable_state_recovery: bool = True
    enable_decision_compat_layer: bool = True
    auto_export_normalized_decision_csv: bool = True
    strict_package_consistency: bool = True
    block_duplicate_buy_same_run: bool = True
    enable_bracket_exit: bool = True
    default_stop_loss_pct: float = 0.04
    default_take_profit_pct: float = 0.08
    trailing_stop_pct: float = 0.03
    execution_style: str = "TWAP3"
    allow_add_on_signal: bool = False
    partial_take_profit_ratio: float = 0.5
    break_even_after_partial_tp: bool = True
    max_holding_bars: int = 12
    current_bar_index: int = 1
    position_cooldown_bars: int = 1
    enable_runtime_lock: bool = True
    enable_heartbeat: bool = True
    archive_decision_input: bool = True
    write_config_snapshot: bool = True
    enable_upstream_execution: bool = True
    fail_on_required_upstream_failure: bool = True
    run_etl_stage: bool = False
    run_ai_stage: bool = False
    run_decision_stage: bool = False
    upstream_timeout_seconds: int = 3600
    require_manual_stage_enable: bool = True
    dry_run_upstream_execution: bool = True
    require_decision_file_after_decision_stage: bool = True
    enable_retry_queue: bool = True
    max_retry_attempts: int = 3
    auto_retry_failed_optional_tasks: bool = False
    fail_on_retry_queue_required_items: bool = True
    enable_auto_retry_on_boot: bool = True
    auto_retry_required_tasks: bool = False
    retry_only_same_stage_enabled: bool = True
    live_manual_arm: bool = False
    require_dual_confirmation: bool = True
    enable_live_kill_switch: bool = True

@dataclass
class DBConfig:
    driver: str = "ODBC Driver 17 for SQL Server"
    server: str = "localhost"
    database: str = "股票online"
    trusted_connection: str = "yes"

BASE_DIR = _detect_base_dir()
PATHS = AppPaths.build(BASE_DIR)
CONFIG = SystemConfig()
DB = DBConfig()
