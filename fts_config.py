# -*- coding: utf-8 -*-
from dataclasses import dataclass
from pathlib import Path
from typing import List

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
    system_name: str = "正式交易主控版_v18"
    package_version: str = "v18"
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
    # v18 stability ops
    enable_runtime_lock: bool = True
    enable_heartbeat: bool = True
    heartbeat_write_seconds: int = 15
    archive_decision_input: bool = True
    write_config_snapshot: bool = True
    fail_when_zero_signal: bool = False

@dataclass
class DBConfig:
    driver: str = "ODBC Driver 17 for SQL Server"
    server: str = "localhost"
    database: str = "股票online"
    trusted_connection: str = "yes"

BASE_DIR = Path(__file__).resolve().parent
PATHS = AppPaths.build(BASE_DIR)
CONFIG = SystemConfig()
DB = DBConfig()
