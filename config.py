# ==========================================
# 中央參數總控台（升級整合版 v2.1）
# ==========================================
import os

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

    # 新版加權燈號設定
    "W_C2_RSI": 0.5,
    "W_C3_VOLUME": 0.5,
    "W_C4_MACD": 1.5,
    "W_C5_BOLL": 0.5,
    "W_C6_BBI": 2.0,
    "W_C7_FOREIGN": 0.7,
    "W_C8_DMI_ADX": 2.0,
    "W_C9_TOTAL_RATIO": 0.3,

    # 訓練 / live 一致性
    "LABEL_USE_EXECUTION_AWARE": True,
    "LABEL_USE_NEXT_OPEN": True,
    "LABEL_REQUIRE_STOP_SAFE": True,
    "LIVE_REQUIRE_SELECTED_FEATURES": True,
    "LIVE_FEATURE_PARITY_MODE": "strict",
    "EXECUTION_POLICY_MODE": "explicit",
    "STRATEGY_LAYER_MODE": "independent",
    "MODEL_LAYER_MODE": "independent",
    "EXECUTION_LAYER_MODE": "independent",
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

# 不再把 token 寫死在程式裡。正式環境請改設環境變數。
FINMIND_API_TOKEN = os.getenv("FINMIND_API_TOKEN", "").strip()


def get_dynamic_watch_list():
    merged = []
    for pool in [WATCH_LIST, TRAINING_POOL, BREAK_TEST_POOL]:
        for ticker in pool:
            if ticker not in merged:
                merged.append(ticker)
    return merged


# ---- Portfolio Risk Layer ----
PARAMS["PORT_MAX_SECTOR_POSITIONS"] = 2
PARAMS["PORT_MAX_SECTOR_ALLOC"] = 0.35
PARAMS["PORT_MAX_TOTAL_ALLOC"] = 0.60
PARAMS["PORT_MAX_DIRECTION_ALLOC"] = 0.45
PARAMS["PORT_MAX_SINGLE_POS"] = 0.12
PARAMS["PORT_MIN_POSITION"] = 0.01


# ---- Alert / Guard ----
PARAMS["ALERT_TEST_MODE"] = True
PARAMS["ALERT_LINE_BOT_TOKEN"] = os.getenv("ALERT_LINE_BOT_TOKEN", "").strip()
PARAMS["ALERT_LINE_USER_ID"] = os.getenv("ALERT_LINE_USER_ID", "").strip()
