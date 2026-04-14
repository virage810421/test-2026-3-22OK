# ==========================================
# 中央參數總控台（升級整合版 v2.1）
# ==========================================
import csv
import os
from pathlib import Path


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
    "MODEL_MIN_OOT_PF": 1.15,
    "MODEL_MIN_OOT_HIT_RATE": 0.52,
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
    "2303.TW",
    "2308.TW",
    "2382.TW",
    "2408.TW",
    "2882.TW",
    "2891.TW",
    "3711.TW",
    "6505.TW",
]

BREAK_TEST_POOL = []
OPTIONAL_UNIVERSE_FILES = [
    Path('data/training_bootstrap_universe.csv'),
    Path('data/paper_execution_watchlist.csv'),
    Path('runtime/approved_live_watchlist.csv'),
]

# 不再把 token 寫死在程式裡。正式環境請改設環境變數。
FINMIND_API_TOKEN = os.getenv("FINMIND_API_TOKEN", "").strip()


def _normalize_ticker(v: str) -> str:
    s = str(v or '').strip().upper()
    if not s:
        return ''
    if s.endswith('.TW') or s.endswith('.TWO'):
        return s
    if s.isdigit():
        return f'{s}.TW'
    return s


def _load_optional_tickers_from_csvs() -> list[str]:
    out: list[str] = []
    env_raw = os.getenv('FTS_EXTRA_TICKERS', '').strip()
    if env_raw:
        for token in env_raw.replace(';', ',').split(','):
            ticker = _normalize_ticker(token)
            if ticker:
                out.append(ticker)
    for path in OPTIONAL_UNIVERSE_FILES:
        if not Path(path).exists():
            continue
        try:
            with open(path, 'r', encoding='utf-8-sig', newline='') as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    for col in ('Ticker SYMBOL', 'Ticker', 'ticker', 'symbol', 'stock_id', '代號'):
                        ticker = _normalize_ticker(row.get(col, ''))
                        if ticker:
                            out.append(ticker)
                            break
        except Exception:
            continue
    return out



def get_dynamic_watch_list():
    merged = []
    universe_sources = [WATCH_LIST, TRAINING_POOL, BREAK_TEST_POOL, _load_optional_tickers_from_csvs()]
    max_names = max(int(os.getenv('FTS_MAX_DYNAMIC_TICKERS', '60') or 60), len(WATCH_LIST), len(TRAINING_POOL))
    for pool in universe_sources:
        for ticker in pool:
            ticker = _normalize_ticker(ticker)
            if ticker and ticker not in merged:
                merged.append(ticker)
            if len(merged) >= max_names:
                return merged
    return merged


def get_dynamic_training_universe():
    return get_dynamic_watch_list()


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


# ---- tri-lane final orchestration / bookkeeping ----
PARAMS.setdefault("LONG_MIN_PROBA", 0.52)
PARAMS.setdefault("SHORT_MIN_PROBA", 0.55)
PARAMS.setdefault("RANGE_MIN_PROBA", 0.53)
PARAMS.setdefault("LONG_MIN_OOT_EV", 0.0)
PARAMS.setdefault("SHORT_MIN_OOT_EV", 0.0)
PARAMS.setdefault("RANGE_MIN_OOT_EV", 0.0)
PARAMS.setdefault("LONG_MAX_HOLD_DAYS", 10)
PARAMS.setdefault("SHORT_MAX_HOLD_DAYS", 6)
PARAMS.setdefault("RANGE_MAX_HOLD_DAYS", 4)
PARAMS.setdefault("LONG_TP_PCT", 0.10)
PARAMS.setdefault("SHORT_TP_PCT", 0.08)
PARAMS.setdefault("RANGE_TP_PCT", 0.05)
PARAMS.setdefault("LONG_SL_PCT", 0.04)
PARAMS.setdefault("SHORT_SL_PCT", 0.035)
PARAMS.setdefault("RANGE_SL_PCT", 0.025)
PARAMS.setdefault("RANGE_MIN_CONFIDENCE", 0.55)
PARAMS.setdefault("LIVE_WATCHLIST_LONG_MAX_NAMES", 12)
PARAMS.setdefault("LIVE_WATCHLIST_SHORT_MAX_NAMES", 8)
PARAMS.setdefault("LIVE_WATCHLIST_RANGE_MAX_NAMES", 8)
PARAMS.setdefault("LIVE_WATCHLIST_TOTAL_MAX_NAMES", 18)
PARAMS.setdefault("LIVE_WATCHLIST_MAX_PER_SECTOR", 3)
PARAMS.setdefault("LIVE_WATCHLIST_MIN_FEATURE_COVERAGE", 0.95)
PARAMS.setdefault("LIVE_WATCHLIST_MIN_LIQUIDITY_SCORE", 0.20)
PARAMS.setdefault("LIVE_WATCHLIST_MAX_NET_SHORT_OVER_LONG", 0.60)
PARAMS.setdefault("LIVE_WATCHLIST_MAX_NET_LONG_OVER_SHORT", 1.20)
PARAMS.setdefault("ENABLE_DIRECTIONAL_REPAIR_EXECUTION", True)
PARAMS.setdefault("ENABLE_DIRECTIONAL_LEDGER_MUTATION", True)
PARAMS.setdefault("ENABLE_TRI_LANE_STAGE_RUNNERS", True)
PARAMS.setdefault("ENABLE_DIRECTIONAL_LEDGER", True)
PARAMS.setdefault("ENABLE_DIRECTIONAL_REPAIR_WORKFLOW", True)
PARAMS.setdefault("ENABLE_DIRECTIONAL_CALLBACK_PIPELINE", True)
PARAMS.setdefault("ENABLE_DIRECTIONAL_CANDIDATES_IN_LIVE", True)
PARAMS.setdefault("ENABLE_DIRECTIONAL_MODEL_LOADING", True)
PARAMS.setdefault("ENABLE_DIRECTIONAL_WATCHLIST_IN_LIVE", True)


# ---- directional strengthening pack ----
PARAMS.setdefault("ENABLE_DIRECTIONAL_ARTIFACT_BOOTSTRAP", True)
PARAMS.setdefault("DIRECTIONAL_BOOTSTRAP_FORCE_SHARED", True)
PARAMS.setdefault("DIRECTIONAL_SEED_FROM_CORE_WATCHLIST", True)
PARAMS.setdefault("DIRECTIONAL_SCOREBOARD_AUTO_BUILD", True)
PARAMS.setdefault("DIRECTIONAL_PROMOTION_MIN_COUNT", 3)
PARAMS.setdefault("LONG_SEED_SCORE", 0.35)
PARAMS.setdefault("SHORT_SEED_SCORE", 0.25)
PARAMS.setdefault("RANGE_SEED_SCORE", 0.25)
PARAMS.setdefault("SHORT_FALLBACK_FROM_CORE", True)
PARAMS.setdefault("RANGE_FALLBACK_FROM_CORE", True)
PARAMS.setdefault("LIVE_WATCHLIST_SHORT_MAX_NAMES", max(int(PARAMS.get("LIVE_WATCHLIST_SHORT_MAX_NAMES", 8)), 5))
PARAMS.setdefault("LIVE_WATCHLIST_RANGE_MAX_NAMES", max(int(PARAMS.get("LIVE_WATCHLIST_RANGE_MAX_NAMES", 8)), 5))

# ---- long/range activation + short/range event flow ----
PARAMS.setdefault("LIVE_WATCHLIST_MIN_PER_LANE_LONG", 2)
PARAMS.setdefault("LIVE_WATCHLIST_MIN_PER_LANE_SHORT", 2)
PARAMS.setdefault("LIVE_WATCHLIST_MIN_PER_LANE_RANGE", 2)
PARAMS.setdefault("LIVE_WATCHLIST_UNKNOWN_SECTOR_SOFT_CAP", True)
PARAMS.setdefault("DIRECTIONAL_DECISION_AUGMENT", True)
PARAMS.setdefault("DIRECTIONAL_DECISION_AUGMENT_MAX_PER_LANE", 2)
PARAMS.setdefault("DIRECTIONAL_DECISION_AUGMENT_USE_SCREENING", True)
PARAMS.setdefault("DIRECTIONAL_SYNTHETIC_KELLY", 0.03)
PARAMS.setdefault("LONG_SEED_SCORE", max(float(PARAMS.get("LONG_SEED_SCORE", 0.35)), 0.45))
PARAMS.setdefault("RANGE_SEED_SCORE", max(float(PARAMS.get("RANGE_SEED_SCORE", 0.25)), 0.35))


PARAMS.setdefault("LONG_MIN_CONFIDENCE", 0.50)
PARAMS.setdefault("SHORT_MIN_CONFIDENCE", 0.50)
PARAMS.setdefault("RANGE_MIN_CONFIDENCE", float(PARAMS.get("RANGE_MIN_CONFIDENCE", 0.55)))
PARAMS.setdefault("LABEL_TP_PCT", max(float(PARAMS.get("TP_BASE_PCT", 0.10)), 0.06))
PARAMS.setdefault("LABEL_MIN_POSITIVE_RETURN", 0.0)
PARAMS.setdefault("MODEL_PRIMARY_SIGNAL_GATE", True)
PARAMS.setdefault("WEIGHTED_SCORE_DIAGNOSTIC_ONLY", True)
PARAMS.setdefault("RANGE_MAX_SCORE_GAP_ABS", 1.25)


# ---- bridge / legacy detachment hardening ----
PARAMS.setdefault("FORCE_SERVICE_API_ONLY", True)
PARAMS.setdefault("ALLOW_LEGACY_FACADE_IN_RESEARCH", False)
PARAMS.setdefault("ALLOW_LEGACY_FACADE_IN_LIVE", False)
PARAMS.setdefault("BRIDGE_GUARD_FAIL_CLOSED", True)
PARAMS.setdefault("PREFER_SQLALCHEMY_DB", True)



# ---- legacy confirmation influence guard ----
# 0.0 means buy_c2~c9 / sell_c2~c9 / weighted scores are alert-only diagnostics.
PARAMS.setdefault("LEGACY_CONFIRM_INFLUENCE", 0.0)
PARAMS.setdefault("LEGACY_AI_PROBA_INFLUENCE", 0.0)
PARAMS.setdefault("LEGACY_SCORE_ALERT_ONLY", True)

# ---- independent exit AI model workflow ----
PARAMS.setdefault("ENABLE_EXIT_MODEL_WORKFLOW", True)
PARAMS.setdefault("EXIT_MODEL_PRIMARY", True)
PARAMS.setdefault("EXIT_MODEL_MIN_FEATURES", 6)
PARAMS.setdefault("EXIT_MODEL_FALLBACK_TO_HAZARD", False)
PARAMS.setdefault("EXIT_DEFEND_THRESHOLD", 0.58)
PARAMS.setdefault("EXIT_REDUCE_THRESHOLD", 0.62)
PARAMS.setdefault("EXIT_CONFIRM_THRESHOLD", 0.66)
PARAMS.setdefault("EXIT_DEFEND_POSITION_MULTIPLIER", 0.60)
PARAMS.setdefault("EXIT_REDUCE_POSITION_MULTIPLIER", 0.35)
PARAMS.setdefault("EXIT_CONFIRM_POSITION_MULTIPLIER", 0.00)
PARAMS.setdefault("EXIT_DEFEND_STOP_TIGHTEN", 0.80)
PARAMS.setdefault("EXIT_REDUCE_STOP_TIGHTEN", 0.60)
PARAMS.setdefault("EXIT_CONFIRM_STOP_TIGHTEN", 0.00)
PARAMS.setdefault("EXIT_LABEL_DEFEND_HAZARD", 0.55)
PARAMS.setdefault("EXIT_LABEL_REDUCE_HAZARD", 0.68)
PARAMS.setdefault("EXIT_LABEL_CONFIRM_HAZARD", 0.82)
PARAMS.setdefault("EXIT_LABEL_DEFEND_ADVERSE_PCT", 1.20)
PARAMS.setdefault("EXIT_LABEL_REDUCE_ADVERSE_PCT", 2.00)

# ---- exit defend / stop replace workflow ----
PARAMS.setdefault("EXIT_STOP_REPLACE_MIN_BPS", 20)
PARAMS.setdefault("EXIT_STOP_MIN_GAP_PCT", 0.003)
PARAMS.setdefault("EXIT_BREAK_EVEN_TRIGGER_R", 0.80)
PARAMS.setdefault("EXIT_BREAK_EVEN_BUFFER_PCT", 0.0005)
PARAMS.setdefault("EXIT_STOP_WORKFLOW_ENABLE", True)
PARAMS.setdefault("EXIT_STOP_WORKFLOW_ALLOW_UPSERT", True)
PARAMS.setdefault("EXIT_DEFEND_ORDER_NOTE", "DEFEND stop tighten")

PARAMS.setdefault("REGIME_HYSTERESIS_SWITCH_BAND", 0.08)
PARAMS.setdefault("REGIME_HYSTERESIS_CONFIRM_BARS", 2)
PARAMS.setdefault("REGIME_HYSTERESIS_MIN_HOLD_BARS", 2)
PARAMS.setdefault("REGIME_HYSTERESIS_TAIL_BARS", 15)
PARAMS.setdefault("EXECUTION_SQL_SYNC_ENABLED", True)
PARAMS.setdefault("EXECUTION_SQL_SYNC_SNAPSHOTS", True)
PARAMS.setdefault("EXECUTION_SQL_SYNC_STOP_ORDERS", True)

# === v88 live-safe EV / no-lookahead gate defaults ===
PARAMS.setdefault("LIVE_MIN_EXPECTED_RETURN", -0.0015)
PARAMS.setdefault("MODEL_LAYER_MIN_EXPECTED_RETURN", PARAMS.get("LIVE_MIN_EXPECTED_RETURN", -0.0015))
PARAMS.setdefault("LIVE_EV_MIN_SAMPLE_FOR_HARD_BLOCK", PARAMS.get("MIN_SIGNAL_SAMPLE_SIZE", 8))
PARAMS.setdefault("LIVE_EV_SCORE_EDGE_SCALE", 0.012)
PARAMS.setdefault("LIVE_EV_PROBA_EDGE_SCALE", 0.050)
PARAMS.setdefault("LIVE_EV_READINESS_SCALE", 0.012)
PARAMS.setdefault("LIVE_EV_RISK_PENALTY_SCALE", 0.010)
PARAMS.setdefault("LIVE_EV_ABS_CAP", 0.20)

# vNext lot-level / callback / reconciliation workflow switches
LOT_LEVEL_POSITION_MODEL_ENABLED = True
LOT_LEVEL_FIFO_CLOSE = True
EXECUTION_CALLBACK_INGEST_ENABLED = True
EXECUTION_RECONCILIATION_ENABLED = True
EXECUTION_RECONCILIATION_WRITE_SQL = True
EXECUTION_LOT_SNAPSHOT_CSV = 'execution_logs/position_lot_snapshot.csv'
EXECUTION_CALLBACK_BLOTTER_CSV = 'execution_logs/broker_callback_blotter.csv'
EXECUTION_RECONCILIATION_BLOTTER_CSV = 'execution_logs/execution_reconciliation_blotter.csv'


# vNext institutional lot lifecycle settings
LOT_ACCOUNTING_METHOD = "FIFO"  # FIFO | AVERAGE
LOT_PARTITION_BY_STRATEGY = True
LOT_PARTITION_BY_SIGNAL = True
LOT_ALLOW_CROSS_STRATEGY_CLOSE = False
LOT_STOP_LINKAGE_ENABLED = True
LOT_STOP_LINKAGE_MATCH_STRATEGY = True
LOT_STOP_LINKAGE_MATCH_SIGNAL = False
LOT_TRACK_PARTIAL_FILL_LIFECYCLE = True
LOT_CLOSE_MATCH_TOLERANCE_QTY = 0

# === v83 hard exit AI policy: no hazard fallback unless explicitly re-enabled ===
PARAMS.setdefault("EXIT_MODEL_REQUIRE_ALL_ARTIFACTS", True)
PARAMS.setdefault("EXIT_MODEL_HARD_BLOCK_IF_MISSING", True)
PARAMS["EXIT_MODEL_FALLBACK_TO_HAZARD"] = bool(PARAMS.get("EXIT_MODEL_FALLBACK_TO_HAZARD", False))

# v83+ exit AI hard-block policy: no hidden fallback to hazard unless explicitly re-enabled.
PARAMS.setdefault('EXIT_MODEL_HARD_BLOCK_WHEN_UNAVAILABLE', True)
PARAMS['EXIT_MODEL_FALLBACK_TO_HAZARD'] = False

# === vNext tax-lot jurisdiction / report / wash-sale rules ===
TAX_LOT_METHOD = "FIFO"  # FIFO | LIFO | AVERAGE | SPECIFIC_ID
TAX_LOT_CURRENCY = "TWD"
TAX_LOT_LONG_TERM_DAYS = 365
TAX_LOT_WASH_SALE_RULE_ENABLED = True
TAX_LOT_WASH_SALE_WINDOW_DAYS = 30
TAX_LOT_SPECIFIC_ID_ENABLED = True
TAX_REPORT_OUTPUT_DIR = "runtime/tax_reports"
TAX_REPORT_EXPORT_ENABLED = True
TAX_AUTO_CLASSIFY_INSTRUMENT = True
TAX_RULE_TW_EQUITY_CURRENCY = "TWD"
TAX_RULE_US_EQUITY_CURRENCY = "USD"
TAX_RULE_FX_CURRENCY = "USD"
TAX_RULE_FUTURES_CURRENCY = "USD"
PARAMS.setdefault("TAX_LOT_METHOD", TAX_LOT_METHOD)
PARAMS.setdefault("TAX_LOT_WASH_SALE_RULE_ENABLED", TAX_LOT_WASH_SALE_RULE_ENABLED)
PARAMS.setdefault("TAX_LOT_WASH_SALE_WINDOW_DAYS", TAX_LOT_WASH_SALE_WINDOW_DAYS)
PARAMS.setdefault("TAX_REPORT_EXPORT_ENABLED", TAX_REPORT_EXPORT_ENABLED)
PARAMS.setdefault("TAX_AUTO_CLASSIFY_INSTRUMENT", TAX_AUTO_CLASSIFY_INSTRUMENT)

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
