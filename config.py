# ==========================================
# 中央參數總控台（安全收編版 v3.0 approved pipeline）
# 說明：
# 1) 保留新模組的訓練治理 / live parity / promotion 參數為正式主線
# 2) 安全收編舊 config 的名單樞紐 / 破壞性測試名單 / sector 候選參數
# 3) 加入 approved pipeline 掛載開關，但預設不直接改真倉 live 參數
# ==========================================
from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Dict, List

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None

PARAMS = {
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
    "MODEL_MIN_REGIME_SAMPLES": 50,
    "MODEL_SEED_FEATURE_LIMIT": 12,
    "WF_SPLITS": 5,
    "MODEL_N_ESTIMATORS": 200,
    "MODEL_MAX_DEPTH": 7,

    "MODEL_MIN_SELECTED_FEATURES": 8,
    "MODEL_MAX_SELECTED_FEATURES": 18,
    "MODEL_MIN_OOT_PF": 1.00,
    "MODEL_MIN_OOT_HIT_RATE": 0.45,
    "MODEL_MIN_PROMOTION_SCORE": 0.0,
    "MODEL_ALLOW_KEEP_TRAINED_IF_NOT_PROMOTED": True,
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

    # --- 名單管理開關 ---
    "WATCHLIST_ENABLE_DYNAMIC_CSV": True,
    "WATCHLIST_DYNAMIC_CSV": "stock_list_cache_listed.csv",
    "WATCHLIST_INCLUDE_TRAINING_POOL": True,
    "WATCHLIST_INCLUDE_BREAK_TEST_POOL": True,
    "WATCHLIST_INCLUDE_EXTENDED_STATIC": True,
    "WATCHLIST_INCLUDE_LOSERS": True,
    "WATCHLIST_MAX_NAMES": 500,
    "WATCHLIST_DYNAMIC_APPLY_TO_TRAINING": True,
    "WATCHLIST_DYNAMIC_APPLY_TO_LIVE": False,

    # --- 舊 config 好邏輯保留，但只作 candidate，不自動接 live ---
    "ENABLE_SECTOR_PARAM_CANDIDATES": True,
    "SECTOR_PARAM_CANDIDATES_LIVE_AUTO_APPLY": False,

    # --- approved pipeline ---
    "APPROVED_PIPELINE_ENABLED": True,
    "APPROVED_DEFAULT_SCOPE": "default",
    "APPROVED_PARAMS_USE_IN_TRAINING": True,
    "APPROVED_PARAMS_USE_IN_LIVE": False,
    "APPROVED_FEATURE_SNAPSHOT_USE_IN_TRAINING": True,
    "APPROVED_FEATURE_SNAPSHOT_USE_IN_LIVE": True,
    "APPROVED_AUTO_CAPTURE_SELECTED_FEATURES": True,
    "APPROVED_ALPHA_AUTO_PROMOTION": True,

    # --- approved live watchlist pipeline ---
    "APPROVED_LIVE_WATCHLIST_ENABLED": True,
    "LIVE_WATCHLIST_MAX_NAMES": 8,
    "LIVE_WATCHLIST_MAX_PER_SECTOR": 2,
    "LIVE_WATCHLIST_MIN_OOT_HIT_RATE": 0.52,
    "LIVE_WATCHLIST_MIN_OOT_EV": 0.0,
    "LIVE_WATCHLIST_MIN_TOTAL_SAMPLES": 30,
    "LIVE_WATCHLIST_MAX_DRAWDOWN": 0.25,
    "LIVE_WATCHLIST_MIN_RECENT_TREND": -0.02,
    "LIVE_WATCHLIST_MIN_FEATURE_COVERAGE": 0.95,
    "LIVE_WATCHLIST_MIN_LIQUIDITY_SCORE": 0.10,
}


WATCH_LIST = [
    "2330.TW",
    "2317.TW",
    "2454.TW",
    "2881.TW",
    "2603.TW",
]

TRAINING_POOL = [
    "2330.TW",
    "2317.TW",
    "2454.TW",
    "2603.TW",
    "2881.TW",
    "3231.TW",
    "1519.TW",
    "2002.TW",
]

BREAK_TEST_POOL = []

EXTENDED_STATIC_WATCH_LIST = [
    "2330.TW", "2317.TW", "2454.TW", "2382.TW", "2308.TW", "3231.TW",
    "2881.TW", "2882.TW", "2891.TW", "2886.TW", "2884.TW", "2892.TW",
    "2603.TW", "2609.TW", "2615.TW", "2618.TW", "2610.TW", "2606.TW",
    "1519.TW", "1513.TW", "1504.TW", "1101.TW", "2002.TW", "8996.TW",
    "6472.TW", "1795.TW", "6446.TW", "4162.TW", "4743.TW", "3176.TW",
    "2409.TW", "3481.TW", "6116.TW", "2344.TW", "2408.TW", "2337.TW",
]

LOSERS_LIST = [
    "2498.TW",
    "3481.TW",
    "2349.TW",
    "2888.TW",
]

FINMIND_API_TOKEN = os.getenv("FINMIND_API_TOKEN", "").strip()

PARAMS["PORT_MAX_SECTOR_POSITIONS"] = 2
PARAMS["PORT_MAX_SECTOR_ALLOC"] = 0.35
PARAMS["PORT_MAX_TOTAL_ALLOC"] = 0.60
PARAMS["PORT_MAX_DIRECTION_ALLOC"] = 0.45
PARAMS["PORT_MAX_SINGLE_POS"] = 0.12
PARAMS["PORT_MIN_POSITION"] = 0.01

PARAMS["ALERT_TEST_MODE"] = True
PARAMS["ALERT_LINE_BOT_TOKEN"] = os.getenv("ALERT_LINE_BOT_TOKEN", "").strip()
PARAMS["ALERT_LINE_USER_ID"] = os.getenv("ALERT_LINE_USER_ID", "").strip()

SECTOR_PARAM_CANDIDATES: Dict[str, Dict] = {
    "TECH": {
        "RSI_PERIOD": 14,
        "TP_TREND_PCT": 0.25,
        "VOL_BREAKOUT_MULTIPLIER": 1.2,
    },
    "SHIPPING": {
        "RSI_PERIOD": 10,
        "SL_MAX_PCT": 0.10,
        "BB_STD": 2.2,
        "TP_BASE_PCT": 0.15,
    },
    "FINANCE": {
        "RSI_PERIOD": 20,
        "MIN_RR_RATIO": 1.2,
        "MDD_LIMIT": 0.10,
    },
}


def _dedup_keep_order(items: List[str]) -> List[str]:
    merged: List[str] = []
    for item in items:
        if item and item not in merged:
            merged.append(item)
    return merged


def _load_dynamic_list_from_csv(csv_name: str) -> List[str]:
    csv_path = Path(csv_name)
    if not csv_path.exists() or pd is None:
        return []
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
        except Exception:
            return []
    cols = ['Ticker SYMBOL', 'Ticker', 'ticker']
    for col in cols:
        if col in df.columns:
            return [str(x).strip() for x in df[col].dropna().tolist() if str(x).strip()]
    return []


def _build_pool(include_dynamic: bool = True, include_extended: bool = True, include_losers: bool = True) -> List[str]:
    pools: List[str] = []
    pools.extend(WATCH_LIST)
    if PARAMS.get("WATCHLIST_INCLUDE_TRAINING_POOL", True):
        pools.extend(TRAINING_POOL)
    if PARAMS.get("WATCHLIST_INCLUDE_BREAK_TEST_POOL", True):
        pools.extend(BREAK_TEST_POOL)
    if include_extended and PARAMS.get("WATCHLIST_INCLUDE_EXTENDED_STATIC", True):
        pools.extend(EXTENDED_STATIC_WATCH_LIST)
    if include_losers and PARAMS.get("WATCHLIST_INCLUDE_LOSERS", True):
        pools.extend(LOSERS_LIST)
    if include_dynamic and PARAMS.get("WATCHLIST_ENABLE_DYNAMIC_CSV", True):
        pools.extend(_load_dynamic_list_from_csv(str(PARAMS.get("WATCHLIST_DYNAMIC_CSV", "stock_list_cache_listed.csv"))))
    merged = _dedup_keep_order(pools)
    return merged[: int(PARAMS.get("WATCHLIST_MAX_NAMES", 500))]


def get_dynamic_watch_list(mode: str = 'training') -> List[str]:
    mode = str(mode or 'training').lower()
    if mode == 'live' and not bool(PARAMS.get('WATCHLIST_DYNAMIC_APPLY_TO_LIVE', False)):
        return _dedup_keep_order(WATCH_LIST)
    if mode == 'training' and not bool(PARAMS.get('WATCHLIST_DYNAMIC_APPLY_TO_TRAINING', True)):
        return _dedup_keep_order(TRAINING_POOL or WATCH_LIST)
    return _build_pool(include_dynamic=True, include_extended=True, include_losers=(mode == 'training'))


def get_training_watch_list() -> List[str]:
    return get_dynamic_watch_list(mode='training')


def get_live_watch_list() -> List[str]:
    return get_dynamic_watch_list(mode='live')


def get_sector_param_candidate(sector_name: str) -> Dict:
    return deepcopy(SECTOR_PARAM_CANDIDATES.get(str(sector_name).upper(), {}))
