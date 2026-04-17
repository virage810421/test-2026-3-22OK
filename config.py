# ==========================================
# 中央參數總控台（升級整合版 v2.2）
# ==========================================
from __future__ import annotations

import os
from pathlib import Path
from types import MappingProxyType

from fts_watchlist_runtime_service import build_dynamic_watch_list_from_env


STRATEGY_PARAMS = MappingProxyType({
    "RSI_PERIOD": 14,
    "MACD_FAST": 12,
    "MACD_SLOW": 26,
    "MACD_SIGNAL": 9,
    "BB_WINDOW": 20,
    "BB_STD": 2.0,
    "VOL_WINDOW": 20,
    "MA_LONG": 60,
    "BBI_PERIODS": [3, 6, 12, 24],
    "DMI_PERIOD": 14,
    "TRIGGER_SCORE": 2,
    "ADX_TREND_THRESHOLD": 20,
    "MIN_PRICE": 10.0,
    "MIN_VOL_MA20": 1_000_000,
    "VOL_BREAKOUT_MULTIPLIER": 1.1,
    "FEE_RATE": 0.001425,
    "FEE_DISCOUNT": 0.6,
    "TAX_RATE": 0.003,
    "SL_MIN_PCT": 0.030,
    "SL_MAX_PCT": 0.080,
    "TP_BASE_PCT": 0.10,
    "TP_TREND_PCT": 0.250,
    "TOTAL_BUDGET": 10_000_000,
    "MAX_POSITIONS": 20,
    "MIN_RR_RATIO": 1.5,
    "MDD_LEVEL_1": 0.10,
    "MDD_MULTIPLIER_1": 0.5,
    "MDD_LEVEL_2": 0.15,
    "MDD_MULTIPLIER_2": 0.2,
    "MDD_LIMIT": 0.20,
    "EV_HIGH_THRESHOLD": 2.0,
    "EV_HIGH_MULTIPLIER": 1.5,
    "EV_BASE_THRESHOLD": 1.0,
    "EV_BASE_MULTIPLIER": 1.0,
    "EV_LOW_MULTIPLIER": 0.5,
    "SCAN_INTERVAL": 300,
    "MAX_BATCHES": 3,
    "MARKET_SLIPPAGE": 0.0015,
    "MIN_SIGNAL_SAMPLE_SIZE": 8,
    "ML_LABEL_HOLD_DAYS": 5,
    "MODEL_MIN_REGIME_SAMPLES": 36,
    "MODEL_SEED_FEATURE_LIMIT": 12,
    "WF_SPLITS": 5,
    "MODEL_N_ESTIMATORS": 200,
    "MODEL_MAX_DEPTH": 7,
    "MODEL_MIN_TRAIN_ROWS": 60,
    "MODEL_MIN_SELECTED_FEATURES": 6,
    "MODEL_MAX_SELECTED_FEATURES": 18,
    "MODEL_MIN_OOT_PF": 1.02,
    "MODEL_MIN_OOT_HIT_RATE": 0.50,
    "MODEL_MIN_PROMOTION_SCORE": 2.0,
    "MODEL_ALLOW_KEEP_TRAINED_IF_NOT_PROMOTED": False,
    "MODEL_REQUIRE_TARGET_RETURN": True,
    "MODEL_TARGET_RETURN_MIN_VALID_RATIO": 0.80,
    "MODEL_MIN_WF_EFFECTIVE_SPLITS": 3,
    "MODEL_MIN_WF_RET_MEAN": 0.0,
    "MODEL_BLOCK_LIVE_ON_UNPROMOTED": True,
    "REGIME_USE_ENHANCED_CLASSIFIER": True,
    "LIVE_MONITOR_WIN_RATE": 0.30,
    "LIVE_MONITOR_MIN_AVG_RETURN": -0.20,
    "IGNORE_CASH_LIMIT": False,
    "FUNDAMENTAL_YOY_BASE": 0,
    "FUNDAMENTAL_YOY_EXCELLENT": 20,
    "FUNDAMENTAL_OPM_BASE": 0,
    "W_C2_RSI": 0.5,
    "W_C3_VOLUME": 0.5,
    "W_C4_MACD": 1.5,
    "W_C5_BOLL": 0.5,
    "W_C6_BBI": 2.0,
    "W_C7_FOREIGN": 0.7,
    "W_C8_DMI_ADX": 2.0,
    "W_C9_TOTAL_RATIO": 0.3,
    "LABEL_USE_EXECUTION_AWARE": True,
    "LABEL_USE_NEXT_OPEN": True,
    "LABEL_REQUIRE_STOP_SAFE": True,
    "LIVE_REQUIRE_SELECTED_FEATURES": True,
    "LIVE_FEATURE_PARITY_MODE": "strict",
    "EXECUTION_POLICY_MODE": "explicit",
    "STRATEGY_LAYER_MODE": "independent",
    "MODEL_LAYER_MODE": "independent",
    "EXECUTION_LAYER_MODE": "independent",
})

PORTFOLIO_PARAMS = MappingProxyType({
    "PORT_MAX_SECTOR_POSITIONS": 2,
    "PORT_MAX_SECTOR_ALLOC": 0.35,
    "PORT_MAX_TOTAL_ALLOC": 0.60,
    "PORT_MAX_DIRECTION_ALLOC": 0.45,
    "PORT_MAX_SINGLE_POS": 0.12,
    "PORT_MIN_POSITION": 0.01,
})

LIVE_GATE_PARAMS = MappingProxyType({
    "LONG_MIN_PROBA": 0.52,
    "SHORT_MIN_PROBA": 0.55,
    "RANGE_MIN_PROBA": 0.53,
    "LONG_MIN_OOT_EV": 0.0,
    "SHORT_MIN_OOT_EV": 0.0,
    "RANGE_MIN_OOT_EV": 0.0,
    "LONG_MAX_HOLD_DAYS": 10,
    "SHORT_MAX_HOLD_DAYS": 6,
    "RANGE_MAX_HOLD_DAYS": 4,
    "LONG_TP_PCT": 0.10,
    "SHORT_TP_PCT": 0.08,
    "RANGE_TP_PCT": 0.05,
    "LONG_SL_PCT": 0.04,
    "SHORT_SL_PCT": 0.035,
    "RANGE_SL_PCT": 0.025,
    "RANGE_MIN_CONFIDENCE": 0.55,
    "LIVE_WATCHLIST_LONG_MAX_NAMES": 12,
    "LIVE_WATCHLIST_SHORT_MAX_NAMES": 8,
    "LIVE_WATCHLIST_RANGE_MAX_NAMES": 8,
    "LIVE_WATCHLIST_TOTAL_MAX_NAMES": 18,
    "LIVE_WATCHLIST_MAX_PER_SECTOR": 3,
    "LIVE_WATCHLIST_MIN_FEATURE_COVERAGE": 0.95,
    "LIVE_WATCHLIST_MIN_LIQUIDITY_SCORE": 0.20,
    "LIVE_WATCHLIST_MAX_NET_SHORT_OVER_LONG": 0.60,
    "LIVE_WATCHLIST_MAX_NET_LONG_OVER_SHORT": 1.20,
    "ENABLE_DIRECTIONAL_REPAIR_EXECUTION": True,
    "ENABLE_DIRECTIONAL_LEDGER_MUTATION": True,
    "ENABLE_TRI_LANE_STAGE_RUNNERS": True,
})

ALERT_PARAMS = MappingProxyType({
    "ALERT_TEST_MODE": True,
    "ALERT_LINE_BOT_TOKEN": os.getenv("ALERT_LINE_BOT_TOKEN", "").strip(),
    "ALERT_LINE_USER_ID": os.getenv("ALERT_LINE_USER_ID", "").strip(),
})

PARAM_SECTIONS = MappingProxyType({
    'strategy': STRATEGY_PARAMS,
    'portfolio': PORTFOLIO_PARAMS,
    'live_gate': LIVE_GATE_PARAMS,
    'alert': ALERT_PARAMS,
})

PARAMS = MappingProxyType({k: v for section in PARAM_SECTIONS.values() for k, v in section.items()})
IMMUTABLE_PARAMS = PARAMS

WATCH_LIST = [
    "2330.TW",
    "2317.TW",
    "2454.TW",
    "2881.TW",
    "2603.TW",
]

TRAINING_POOL = [
    "2330.TW", "2317.TW", "2454.TW", "2603.TW", "2881.TW", "3231.TW", "1519.TW", "2002.TW",
    "2303.TW", "2308.TW", "2382.TW", "2408.TW", "2882.TW", "2891.TW", "3711.TW", "6505.TW",
]

BREAK_TEST_POOL: list[str] = []
OPTIONAL_UNIVERSE_FILES = [
    Path('data/training_bootstrap_universe.csv'),
    Path('data/paper_execution_watchlist.csv'),
    Path('runtime/approved_live_watchlist.csv'),
]

FINMIND_API_TOKEN = os.getenv("FINMIND_API_TOKEN", "").strip()


def get_params_copy() -> dict[str, object]:
    return dict(PARAMS)


def get_param_sections() -> dict[str, dict[str, object]]:
    return {name: dict(section) for name, section in PARAM_SECTIONS.items()}


def get_dynamic_watch_list() -> list[str]:
    return build_dynamic_watch_list_from_env(
        watch_list=list(WATCH_LIST),
        training_pool=list(TRAINING_POOL),
        break_test_pool=list(BREAK_TEST_POOL),
        optional_universe_files=OPTIONAL_UNIVERSE_FILES,
    )


def get_dynamic_training_universe() -> list[str]:
    return get_dynamic_watch_list()
