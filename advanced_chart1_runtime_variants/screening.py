import os
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import pyodbc
import yfinance as yf
from FinMind.data import DataLoader

from .advanced_chart import draw_chart
from .config import PARAMS, WATCH_LIST, FINMIND_API_TOKEN

warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")
warnings.filterwarnings("ignore", category=ResourceWarning)
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", message=".*scikit-learn configuration.*")

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)


def _build_finmind_loader():
    try:
        if FINMIND_API_TOKEN:
            try:
                return DataLoader(token=FINMIND_API_TOKEN)
            except TypeError:
                dl = DataLoader()
                if hasattr(dl, "login_by_token"):
                    dl.login_by_token(api_token=FINMIND_API_TOKEN)
                return dl
    except Exception:
        pass
    return None


dl = _build_finmind_loader()


def normalize_ticker_symbol(ticker: str, default_suffix: str = ".TW") -> str:
    ticker = str(ticker).strip()
    if not ticker:
        return ticker
    if ticker.endswith(".TW") or ticker.endswith(".TWO"):
        return ticker
    if ticker.isdigit():
        return f"{ticker}{default_suffix}"
    return ticker


def smart_download(ticker, period="1y"):
    ticker = normalize_ticker_symbol(ticker)
    os.makedirs("data/kline_cache", exist_ok=True)
    safe_name = ticker.replace("/", "_")
    cache_file = f"data/kline_cache/{safe_name}_{period}.csv"

    today = datetime.now().date()
    is_weekend = today.weekday() >= 5

    if os.path.exists(cache_file):
        file_mtime_date = datetime.fromtimestamp(os.path.getmtime(cache_file)).date()
        days_diff = (today - file_mtime_date).days

        if days_diff == 0 or (is_weekend and days_diff <= 3):
            try:
                return pd.read_csv(cache_file, index_col=0, parse_dates=True)
            except Exception:
                pass

    try:
        data = yf.download(ticker, period=period, progress=False, auto_adjust=False)
        if data.empty:
            return pd.DataFrame()

        df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()
        df.to_csv(cache_file)
        return df
    except Exception as e:
        print(f"⚠️ {ticker} 網路下載失敗: {e}")
        if os.path.exists(cache_file):
            print(f"♻️ 啟用備用方案：讀取 {ticker} 舊有快取資料。")
            return pd.read_csv(cache_file, index_col=0, parse_dates=True)
        return pd.DataFrame()


def extract_ai_features(row):
    features = {}

    close_p = row.get("Close", 1)
    open_p = row.get("Open", 1)
    high_p = row.get("High", 1)
    low_p = row.get("Low", 1)
    ma20 = row.get("MA20", close_p)

    features["K_Body_Pct"] = (close_p - open_p) / open_p if open_p else 0
    features["Upper_Shadow"] = (high_p - max(close_p, open_p)) / close_p if close_p else 0
    features["Lower_Shadow"] = (min(close_p, open_p) - low_p) / close_p if close_p else 0
    features["Dist_to_MA20"] = (close_p - ma20) / (ma20 + 0.0001)

    vol_ma20 = row.get("Vol_MA20", 0)
    features["Volume_Ratio"] = row.get("Volume", 0) / (vol_ma20 + 0.001) if vol_ma20 != 0 else 1
    features["BB_Width"] = row.get("BB_Width", 0)
    features["RSI"] = row.get("RSI", 50)
    features["MACD_Hist"] = row.get("MACD_Hist", 0)
    features["ADX"] = row.get("ADX14", 25)

    features["Foreign_Ratio"] = row.get("Foreign_Ratio", 0)
    features["Trust_Ratio"] = row.get("Trust_Ratio", 0)
    features["Total_Ratio"] = row.get("Total_Ratio", 0)
    features["Foreign_Consec_Days"] = row.get("Foreign_Consecutive", 0)
    features["Trust_Consec_Days"] = row.get("Trust_Consecutive", 0)

    for key in [
        "buy_c2", "buy_c3", "buy_c4", "buy_c5", "buy_c6", "buy_c7", "buy_c8", "buy_c9",
        "sell_c2", "sell_c3", "sell_c4", "sell_c5", "sell_c6", "sell_c7", "sell_c8", "sell_c9"
    ]:
        features[key] = int(row.get(key, 0))

    features["Weighted_Buy_Score"] = float(row.get("Weighted_Buy_Score", 0))
    features["Weighted_Sell_Score"] = float(row.get("Weighted_Sell_Score", 0))
    features["Score_Gap"] = float(row.get("Score_Gap", 0))
    features["Signal_Conflict"] = int(row.get("Signal_Conflict", 0))

    features["Trap_Signal"] = 1 if row.get("Fake_Breakout", False) else (-1 if row.get("Bear_Trap", False) else 0)
    features["Vol_Squeeze"] = int(row.get("Vol_Squeeze", False))
    features["Absorption"] = int(row.get("Absorption", False))

    features["MR_Long_Spring"] = 1 if (
        (row.get("Low", 0) < row.get("BB_Lower", 0))
        and (row.get("Volume", 0) < row.get("Vol_MA20", 0))
        and (row.get("Total_Ratio", 0) >= -0.01)
    ) else 0

    features["MR_Short_Trap"] = 1 if (
        (row.get("High", 0) > row.get("BB_Upper", 0))
        and (features.get("Upper_Shadow", 0) > 0.02)
        and (row.get("RSI", 50) > 65)
    ) else 0

    features["MR_Long_Accumulation"] = 1 if (
        (row.get("RSI", 50) < 35)
        and (row.get("Total_Ratio", 0) > 0.05)
    ) else 0

    features["MR_Short_Distribution"] = 1 if (
        (row.get("RSI", 50) > 65)
        and (row.get("Total_Ratio", 0) < -0.05)
    ) else 0

    return features


def add_chip_data(df, ticker):
    if df is None or df.empty:
        return pd.DataFrame()

    ticker = normalize_ticker_symbol(ticker)
    alternate = ticker[:-3] + ".TWO" if ticker.endswith(".TW") else (
        ticker[:-4] + ".TW" if ticker.endswith(".TWO") else ticker
    )

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = """
            SELECT [日期], [Ticker SYMBOL], [外資買賣超], [投信買賣超], [自營商買賣超]
            FROM daily_chip_data
            WHERE [Ticker SYMBOL] IN (?, ?)
            """
            chip_df = pd.read_sql(query, conn, params=(ticker, alternate))

        if chip_df.empty:
            df["Foreign_Net"], df["Trust_Net"], df["Dealers_Net"] = 0, 0, 0
            return df

        chip_df["日期"] = pd.to_datetime(chip_df["日期"]).dt.normalize()
        chip_df.sort_values(["日期", "Ticker SYMBOL"], inplace=True)
        chip_df = chip_df.drop_duplicates(subset=["日期"], keep="last")
        chip_df.set_index("日期", inplace=True)

        if getattr(df.index, "tz", None) is not None:
            df.index = df.index.tz_localize(None)
        df.index = pd.to_datetime(df.index).normalize()

        df = df.join(chip_df["外資買賣超"].rename("Foreign_Net"), how="left")
        df = df.join(chip_df["投信買賣超"].rename("Trust_Net"), how="left")
        df = df.join(chip_df["自營商買賣超"].rename("Dealers_Net"), how="left")

        df["Foreign_Net"] = df["Foreign_Net"].ffill().fillna(0)
        df["Trust_Net"] = df["Trust_Net"].ffill().fillna(0)
        df["Dealers_Net"] = df["Dealers_Net"].ffill().fillna(0)

    except Exception as e:
        print(f"⚠️ {ticker} 本地籌碼庫讀取失敗: {e}")
        df["Foreign_Net"], df["Trust_Net"], df["Dealers_Net"] = 0, 0, 0

    return df


def add_fundamental_filter(ticker, p=PARAMS):
    pure_ticker = normalize_ticker_symbol(ticker).split(".")[0]
    if dl is None:
        return {"營收年增率(%)": 0.000, "營業利益率(%)": 0.000, "基本面總分": 0}

    try:
        rev_df = dl.taiwan_stock_month_revenue(stock_id=pure_ticker)
        rev_yoy = rev_df.iloc[-1]["revenue_year_growth"] if not rev_df.empty else 0.0

        st_df = dl.taiwan_stock_financial_statement(stock_id=pure_ticker)
        if not st_df.empty:
            op_margin_row = st_df[st_df["type"] == "OperatingProfitMargin"]
            op_margin = op_margin_row.iloc[-1]["value"] if not op_margin_row.empty else 0.0
        else:
            op_margin = 0.0

        f_score = 0
        if rev_yoy > p.get("FUNDAMENTAL_YOY_BASE", 0):
            f_score += 1
        if rev_yoy > p.get("FUNDAMENTAL_YOY_EXCELLENT", 20):
            f_score += 1
        if op_margin > p.get("FUNDAMENTAL_OPM_BASE", 0):
            f_score += 1
        if op_margin < p.get("FUNDAMENTAL_OPM_BASE", 0):
            f_score -= 2

        return {"營收年增率(%)": rev_yoy, "營業利益率(%)": op_margin, "基本面總分": f_score}
    except Exception:
        return {"營收年增率(%)": 0.000, "營業利益率(%)": 0.000, "基本面總分": 0}


def apply_slippage(price, direction, slippage):
    return price * (1 + slippage * direction)


def get_exit_price(entry_price, open_price, sl_pct, direction):
    stop_price = entry_price * (1 - sl_pct * direction)
    if (direction == 1 and open_price < stop_price) or (direction == -1 and open_price > stop_price):
        return open_price
    return stop_price


def get_tp_price(entry_price, open_price, tp_pct, direction):
    target_price = entry_price * (1 + tp_pct * direction)
    if (direction == 1 and open_price > target_price) or (direction == -1 and open_price < target_price):
        return open_price
    return target_price


def calculate_pnl(direction, entry_price, exit_price, shares, fee_rate, tax_rate):
    invested = entry_price * shares
    if direction == 1:
        entry_cost = invested * (1 + fee_rate)
        exit_value = exit_price * shares * (1 - fee_rate - tax_rate)
        pnl = exit_value - entry_cost
    else:
        entry_value = invested * (1 - fee_rate - tax_rate)
        exit_cost = exit_price * shares * (1 + fee_rate)
        pnl = entry_value - exit_cost
    return pnl, invested


def _get_score_weights(p=PARAMS):
    return {
        "c2_rsi": float(p.get("W_C2_RSI", 0.5)),
        "c3_volume": float(p.get("W_C3_VOLUME", 0.5)),
        "c4_macd": float(p.get("W_C4_MACD", 1.5)),
        "c5_boll_reversal": float(p.get("W_C5_BOLL", 0.5)),
        "c6_bbi": float(p.get("W_C6_BBI", 2.0)),
        "c7_foreign": float(p.get("W_C7_FOREIGN", 0.7)),
        "c8_dmi_adx": float(p.get("W_C8_DMI_ADX", 2.0)),
        "c9_total_ratio": float(p.get("W_C9_TOTAL_RATIO", 0.3)),
    }


def _apply_weighted_scores(df, p=PARAMS):
    weights = _get_score_weights(p)

    df["Weighted_Buy_Score"] = (
        df["buy_c2"] * weights["c2_rsi"] +
        df["buy_c3"] * weights["c3_volume"] +
        df["buy_c4"] * weights["c4_macd"] +
        df["buy_c5"] * weights["c5_boll_reversal"] +
        df["buy_c6"] * weights["c6_bbi"] +
        df["buy_c7"] * weights["c7_foreign"] +
        df["buy_c8"] * weights["c8_dmi_adx"] +
        df["buy_c9"] * weights["c9_total_ratio"]
    )

    df["Weighted_Sell_Score"] = (
        df["sell_c2"] * weights["c2_rsi"] +
        df["sell_c3"] * weights["c3_volume"] +
        df["sell_c4"] * weights["c4_macd"] +
        df["sell_c5"] * weights["c5_boll_reversal"] +
        df["sell_c6"] * weights["c6_bbi"] +
        df["sell_c7"] * weights["c7_foreign"] +
        df["sell_c8"] * weights["c8_dmi_adx"] +
        df["sell_c9"] * weights["c9_total_ratio"]
    )

    df["Score_Gap"] = df["Weighted_Buy_Score"] - df["Weighted_Sell_Score"]
    return df


def _assign_golden_type(df, trigger_score):
    trigger = max(2.0, float(trigger_score))
    buy_active = df["Weighted_Buy_Score"] >= trigger
    sell_active = df["Weighted_Sell_Score"] >= trigger

    df["Signal_Conflict"] = (buy_active & sell_active).astype(int)
    df["Golden_Type"] = "無"

    long_only = buy_active & (~sell_active)
    short_only = sell_active & (~buy_active)
    both = buy_active & sell_active

    df.loc[long_only, "Golden_Type"] = "多方進場"
    df.loc[short_only, "Golden_Type"] = "空方進場"

    df.loc[both & (df["Weighted_Buy_Score"] > df["Weighted_Sell_Score"]), "Golden_Type"] = "多方進場"
    df.loc[both & (df["Weighted_Sell_Score"] > df["Weighted_Buy_Score"]), "Golden_Type"] = "空方進場"
    df.loc[both & (df["Weighted_Buy_Score"] == df["Weighted_Sell_Score"]), "Golden_Type"] = "無"

    return df


def _compute_realized_signal_stats(df, p=PARAMS, hold_days=5):
    fee_rate = float(p.get("FEE_RATE", 0.001425)) * float(p.get("FEE_DISCOUNT", 1.0))
    tax_rate = float(p.get("TAX_RATE", 0.003))
    min_samples = int(p.get("MIN_SIGNAL_SAMPLE_SIZE", 8))

    if df is None or df.empty or len(df) <= hold_days + 1:
        return {
            "系統勝率(%)": 50.0,
            "累計報酬率(%)": 0.0,
            "期望值": 0.0,
            "平均獲利(%)": 0.0,
            "平均虧損(%)": 0.0,
            "歷史訊號樣本數": 0,
            "Kelly建議倉位": 0.0,
            "Realized_Signal_Returns": [],
        }

    realized_returns = []
    valid_df = df.copy().reset_index().rename(columns={df.index.name or "index": "TradeDate"})

    for i in range(1, len(valid_df) - hold_days):
        setup = str(valid_df.loc[i - 1, "Golden_Type"]).strip()
        if setup not in ("多方進場", "空方進場"):
            continue

        entry_open = valid_df.loc[i, "Open"]
        exit_close = valid_df.loc[i + hold_days - 1, "Close"]

        if pd.isna(entry_open) or pd.isna(exit_close) or entry_open <= 0:
            continue

        direction = 1 if setup == "多方進場" else -1
        gross_ret = direction * ((float(exit_close) - float(entry_open)) / float(entry_open))
        net_ret = gross_ret - (2 * fee_rate + tax_rate)
        realized_returns.append(float(net_ret * 100.0))

    sample_size = len(realized_returns)
    if sample_size == 0:
        return {
            "系統勝率(%)": 50.0,
            "累計報酬率(%)": 0.0,
            "期望值": 0.0,
            "平均獲利(%)": 0.0,
            "平均虧損(%)": 0.0,
            "歷史訊號樣本數": 0,
            "Kelly建議倉位": 0.0,
            "Realized_Signal_Returns": [],
        }

    wins = [r for r in realized_returns if r > 0]
    losses = [r for r in realized_returns if r <= 0]

    win_rate = (len(wins) / sample_size) * 100.0
    avg_win = float(np.mean(wins)) if wins else 0.0
    avg_loss = abs(float(np.mean(losses))) if losses else 0.0
    expectancy = float(np.mean(realized_returns))
    total_profit = float(np.sum(realized_returns))

    reward_risk_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0
    p_win = len(wins) / sample_size
    q_loss = 1 - p_win

    if reward_risk_ratio > 0 and expectancy > 0:
        kelly_fraction = p_win - (q_loss / reward_risk_ratio)
    else:
        kelly_fraction = 0.0

    safe_kelly = max(0.0, min(0.30, kelly_fraction * 0.5))

    if sample_size < min_samples:
        shrink = sample_size / float(min_samples)
        win_rate = 50.0 + (win_rate - 50.0) * shrink
        expectancy = expectancy * shrink
        total_profit = total_profit * shrink
        safe_kelly = safe_kelly * shrink

    return {
        "系統勝率(%)": round(win_rate, 2),
        "累計報酬率(%)": round(total_profit, 2),
        "期望值": round(expectancy, 4),
        "平均獲利(%)": round(avg_win, 4),
        "平均虧損(%)": round(avg_loss, 4),
        "歷史訊號樣本數": sample_size,
        "Kelly建議倉位": round(safe_kelly, 4),
        "Realized_Signal_Returns": realized_returns,
    }


def inspect_stock(ticker, preloaded_df=None, p=PARAMS):
    ticker = normalize_ticker_symbol(ticker)

    try:
        if preloaded_df is not None:
            df = preloaded_df.copy()
        else:
            df = smart_download(ticker, period="2y")

        if df.empty:
            return None

        df.dropna(subset=["Close"], inplace=True)
        df.ffill(inplace=True)
        if df.empty:
            return None

        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.ewm(alpha=1 / p["RSI_PERIOD"], min_periods=p["RSI_PERIOD"], adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / p["RSI_PERIOD"], min_periods=p["RSI_PERIOD"], adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df["RSI"] = np.where(avg_loss == 0, 100, 100 - (100 / (1 + rs)))

        df["RSI_MA"] = df["RSI"].rolling(window=p["RSI_PERIOD"]).mean()
        df["RSI_STD"] = df["RSI"].rolling(window=p["RSI_PERIOD"]).std()
        df["DZ_Upper"] = df["RSI_MA"] + (df["RSI_STD"] * 1.5)
        df["DZ_Lower"] = df["RSI_MA"] - (df["RSI_STD"] * 1.5)

        df["EMA12"] = df["Close"].ewm(span=p["MACD_FAST"], adjust=False).mean()
        df["EMA26"] = df["Close"].ewm(span=p["MACD_SLOW"], adjust=False).mean()
        df["DIF"] = df["EMA12"] - df["EMA26"]
        df["MACD_Signal"] = df["DIF"].ewm(span=p["MACD_SIGNAL"], adjust=False).mean()
        df["MACD_Hist"] = (df["DIF"] - df["MACD_Signal"]) * 2

        df["MA20"] = df["Close"].rolling(window=p["BB_WINDOW"]).mean()
        df["BB_std"] = df["Close"].rolling(window=p["BB_WINDOW"]).std()
        df["BB_Upper"] = df["MA20"] + (df["BB_std"] * p["BB_STD"])
        df["BB_Lower"] = df["MA20"] - (df["BB_std"] * p["BB_STD"])
        df["BB_Width"] = (df["BB_Upper"] - df["BB_Lower"]) / df["MA20"]
        df["Vol_MA20"] = df["Volume"].rolling(window=p["VOL_WINDOW"]).mean()

        bbi_cols = []
        for days in p["BBI_PERIODS"]:
            col_name = f"MA{days}"
            df[col_name] = df["Close"].rolling(window=days).mean()
            bbi_cols.append(df[col_name])
        df["BBI"] = sum(bbi_cols) / len(p["BBI_PERIODS"])

        high_diff = df["High"].diff()
        low_diff = -df["Low"].diff()
        df["+DM"] = np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0)
        df["-DM"] = np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0)

        tr1 = df["High"] - df["Low"]
        tr2 = abs(df["High"] - df["Close"].shift(1))
        tr3 = abs(df["Low"] - df["Close"].shift(1))
        df["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        df["+DI14"] = 100 * (df["+DM"].rolling(p["DMI_PERIOD"]).sum() / df["TR"].rolling(p["DMI_PERIOD"]).sum())
        df["-DI14"] = 100 * (df["-DM"].rolling(p["DMI_PERIOD"]).sum() / df["TR"].rolling(p["DMI_PERIOD"]).sum())
        df["DX"] = 100 * abs(df["+DI14"] - df["-DI14"]) / (df["+DI14"] + df["-DI14"])
        df["ADX14"] = df["DX"].rolling(p["DMI_PERIOD"]).mean()
        df["ATR"] = df["TR"].ewm(alpha=1 / p["DMI_PERIOD"], adjust=False).mean()

        df["Total_Net"] = df.get("Foreign_Net", 0) + df.get("Trust_Net", 0) + df.get("Dealers_Net", 0)
        safe_vol = df["Volume"].replace(0, 1)
        df["Foreign_Ratio"] = df.get("Foreign_Net", 0) / safe_vol
        df["Trust_Ratio"] = df.get("Trust_Net", 0) / safe_vol
        df["Total_Ratio"] = df["Total_Net"] / safe_vol
        df["Foreign_Consecutive"] = df["Foreign_Net"].groupby((df["Foreign_Net"] <= 0).cumsum()).cumcount()
        df["Trust_Consecutive"] = df["Trust_Net"].groupby((df["Trust_Net"] <= 0).cumsum()).cumcount()

        # 原始條件訊號（保留給 AI 特徵）
        df["buy_c2"] = (df["RSI"] < 35).astype(int)
        df["buy_c3"] = (df["Volume"] > (df["Vol_MA20"] * p.get("VOL_BREAKOUT_MULTIPLIER", 1.1))).astype(int)
        df["buy_c4"] = ((df["MACD_Hist"] > 0) & (df["MACD_Hist"] > df["MACD_Hist"].shift(1))).astype(int)
        df["buy_c5"] = ((df["Close"] < df["BB_Lower"]) & (df["RSI"] > df["RSI"].shift(1))).astype(int)
        df["buy_c6"] = ((df["Close"] > df["BBI"]) & (df["Close"].shift(1) <= df["BBI"].shift(1))).astype(int)
        df["buy_c7"] = (df["Foreign_Ratio"] > 0.03).astype(int)
        df["buy_c8"] = ((df["+DI14"] > df["-DI14"]) & (df["ADX14"] > p["ADX_TREND_THRESHOLD"])).astype(int)
        df["buy_c9"] = ((df["Total_Ratio"] > 0) & (df["Total_Ratio"] > df["Total_Ratio"].shift(1))).astype(int)

        df["sell_c2"] = (df["RSI"] > 70).astype(int)
        df["sell_c3"] = (df["Volume"] > (df["Vol_MA20"] * p.get("VOL_BREAKOUT_MULTIPLIER", 1.1))).astype(int)
        df["sell_c4"] = ((df["MACD_Hist"] < 0) & (df["MACD_Hist"] < df["MACD_Hist"].shift(1))).astype(int)
        df["sell_c5"] = ((df["Close"] > df["BB_Upper"]) & (df["RSI"] < df["RSI"].shift(1))).astype(int)
        df["sell_c6"] = ((df["Close"] < df["BBI"]) & (df["Close"].shift(1) >= df["BBI"].shift(1))).astype(int)
        df["sell_c7"] = (df["Foreign_Ratio"] < -0.03).astype(int)
        df["sell_c8"] = ((df["-DI14"] > df["+DI14"]) & (df["ADX14"] > p["ADX_TREND_THRESHOLD"])).astype(int)
        df["sell_c9"] = ((df["Total_Ratio"] < 0) & (df["Total_Ratio"] < df["Total_Ratio"].shift(1))).astype(int)

        # 舊等權分數保留相容
        df["Buy_Score"] = df[[f"buy_c{i}" for i in range(2, 10)]].sum(axis=1)
        df["Sell_Score"] = df[[f"sell_c{i}" for i in range(2, 10)]].sum(axis=1)

        # 新加權分數
        df = _apply_weighted_scores(df, p=p)

        df["Fake_Breakout"] = ((df["High"] > df["BB_Upper"]) & (df["Close"] < df["BB_Upper"])).fillna(False)
        df["Bear_Trap"] = ((df["Low"] < df["BB_Lower"]) & (df["Close"] > df["BB_Lower"])).fillna(False)
        df["Vol_Squeeze"] = (df["BB_Width"] < df["BB_Width"].rolling(20).quantile(0.25)).fillna(False)
        df["Absorption"] = ((df["Close"] > df["Open"]) & (df["Total_Ratio"] > 0.02)).fillna(False)

        conditions = [
            (df["ADX14"] > p["ADX_TREND_THRESHOLD"]) & (df["+DI14"] > df["-DI14"]),
            (df["ADX14"] > p["ADX_TREND_THRESHOLD"]) & (df["-DI14"] > df["+DI14"]),
        ]
        choices = ["趨勢多頭", "趨勢空頭"]
        df["Regime"] = np.select(conditions, choices, default="區間盤整")

        df = _assign_golden_type(df, p.get("TRIGGER_SCORE", 2))
        latest = df.iloc[-1]

        signal_stats = _compute_realized_signal_stats(df, p=p, hold_days=int(p.get("ML_LABEL_HOLD_DAYS", 5)))
        signal_count = int((df["Golden_Type"] != "無").sum())

        # 新版信心分數：加權分數差值 + 衝突懲罰
        weighted_gap = float(latest["Score_Gap"])
        conflict_penalty = 10.0 if int(latest.get("Signal_Conflict", 0)) == 1 else 0.0
        signal_confidence = float(max(0.0, min(100.0, 50.0 + weighted_gap * 12.0 - conflict_penalty)))

        return {
            "Ticker": ticker,
            "系統勝率(%)": signal_stats["系統勝率(%)"],
            "累計報酬率(%)": signal_stats["累計報酬率(%)"],
            "期望值": signal_stats["期望值"],
            "平均獲利(%)": signal_stats["平均獲利(%)"],
            "平均虧損(%)": signal_stats["平均虧損(%)"],
            "Kelly建議倉位": signal_stats["Kelly建議倉位"],
            "歷史訊號樣本數": signal_stats["歷史訊號樣本數"],
            "訊號信心分數(%)": round(signal_confidence, 2),
            "Regime": latest["Regime"],
            "Buy_Score": int(latest["Buy_Score"]),
            "Sell_Score": int(latest["Sell_Score"]),
            "Weighted_Buy_Score": round(float(latest["Weighted_Buy_Score"]), 3),
            "Weighted_Sell_Score": round(float(latest["Weighted_Sell_Score"]), 3),
            "Score_Gap": round(float(latest["Score_Gap"]), 3),
            "Golden_Type": latest["Golden_Type"],
            "Signal_Count": signal_count,
            "Signal_Conflict": int(latest.get("Signal_Conflict", 0)),
            "計算後資料": df,
        }

    except Exception as e:
        print(f"⚠️ inspect_stock({ticker}) 運算失敗: {e}")
        return None


if __name__ == "__main__":
    test_targets = WATCH_LIST[:3] if WATCH_LIST else ["2330.TW", "2454.TW", "2317.TW"]

    for ticker in test_targets:
        ticker = normalize_ticker_symbol(ticker)
        print(f"\n📡 正在測試 {ticker} ...")
        df = smart_download(ticker, period="2y")
        if df.empty:
            print("⚠️ 無價格資料")
            continue
        df = add_chip_data(df, ticker)
        result = inspect_stock(ticker, preloaded_df=df)
        if result and "計算後資料" in result:
            print(
                f"✅ {ticker} | Regime={result['Regime']} | "
                f"Win={result['系統勝率(%)']}% | EV={result['期望值']} | "
                f"WB={result['Weighted_Buy_Score']} | WS={result['Weighted_Sell_Score']} | "
                f"Samples={result['歷史訊號樣本數']}"
            )
            try:
                draw_chart(
                    ticker,
                    preloaded_df=result["計算後資料"],
                    win_rate=result.get("系統勝率(%)", "N/A"),
                    total_profit=result.get("累計報酬率(%)", "N/A"),
                    expected_value=result.get("期望值", "N/A"),
                    signal_confidence=result.get("訊號信心分數(%)", "N/A"),
                    sample_size=result.get("歷史訊號樣本數", "N/A"),
                )
            except Exception as chart_err:
                print(f"⚠️ 畫圖失敗：{chart_err}")