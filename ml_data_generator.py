import os

import numpy as np
import pandas as pd
import yfinance as yf

from screening import inspect_stock, add_chip_data, extract_ai_features, smart_download
from config import PARAMS



def get_dynamic_watchlist():
    """從中央樞紐直接獲取包含績優股與破壞組的完整名單"""
    print("📡 啟動動態索敵雷達：正在連接 config 名單樞紐...")
    try:
        from config import get_dynamic_watch_list
        dynamic_list = get_dynamic_watch_list()
        print(f"✅ 成功鎖定 {len(dynamic_list)} 檔目標 (已混入破壞對照組)，準備印製歷史課本！")
        return dynamic_list
    except Exception as e:
        print(f"⚠️ 無法獲取名單: {e}")
        from config import WATCH_LIST
        return WATCH_LIST



def generate_ml_dataset(tickers):
    print("🏭 [兵工廠] 啟動 AI 雙向訓練資料生成器...")

    if os.path.exists("data/ml_training_data.csv"):
        os.remove("data/ml_training_data.csv")
        print("🗑️ 已銷毀昨日舊有訓練資料，確保數據絕對純淨。")

    ml_dataset = []

    test_params = PARAMS.copy()
    test_params["IS_OPTIMIZING"] = True
    test_params["TRIGGER_SCORE"] = 2

    for ticker in tickers:
        print(f"📡 正在萃取 {ticker} 的歷史特徵與勝負標籤...")
        try:
            df = smart_download(ticker, period="3y")
            if df.empty:
                continue

            df = add_chip_data(df, ticker)

            result = inspect_stock(ticker, preloaded_df=df, p=test_params)
            if not result or "計算後資料" not in result:
                continue

            computed_df = result["計算後資料"]

            for i in range(len(computed_df) - 5):
                row = computed_df.iloc[i]
                setup_tag = str(row.get("Golden_Type", "無")).strip()
                regime = str(row.get("Regime", "區間盤整")).strip()

                if setup_tag == "無":
                    continue

                is_short_signal = ("空" in setup_tag) or ("SHORT" in setup_tag.upper())
                is_long_signal = ("多" in setup_tag) or ("LONG" in setup_tag.upper())
                if not is_short_signal and not is_long_signal:
                    continue

                features = extract_ai_features(row)
                features["Ticker"] = ticker
                features["Date"] = computed_df.index[i]
                features["Regime"] = regime
                features["Setup"] = setup_tag

                entry_price = computed_df.iloc[i + 1]["Open"]
                future_window = computed_df.iloc[i + 1 : i + 6]
                if future_window.empty or pd.isna(entry_price) or entry_price <= 0:
                    continue

                sl_pct = float(PARAMS.get("SL_MIN_PCT", 0.03))

                if is_short_signal:
                    max_high = future_window["High"].max()
                    if (max_high - entry_price) / entry_price > sl_pct:
                        win_condition = False
                    else:
                        min_low = future_window["Low"].min()
                        win_condition = (entry_price - min_low) / entry_price > sl_pct
                else:
                    min_low = future_window["Low"].min()
                    if (entry_price - min_low) / entry_price > sl_pct:
                        win_condition = False
                    else:
                        max_high = future_window["High"].max()
                        win_condition = (max_high - entry_price) / entry_price > sl_pct

                features["Label_Y"] = 1 if win_condition else 0

                future_close = future_window["Close"].iloc[-1]
                if is_short_signal:
                    features["Target_Return"] = (entry_price - future_close) / entry_price
                else:
                    features["Target_Return"] = (future_close - entry_price) / entry_price

                ml_dataset.append(features)

        except Exception as e:
            print(f"⚠️ {ticker} 萃取失敗: {e}")

    final_df = pd.DataFrame(ml_dataset)
    if not final_df.empty:
        os.makedirs("data", exist_ok=True)
        final_df.to_csv("data/ml_training_data.csv", index=False, encoding="utf-8-sig")
        print(f"\n✅ [兵工廠] 成功萃取 {len(final_df)} 筆雙向戰鬥紀錄，已存為 ml_training_data.csv！")
    else:
        print("\n⚠️ 萃取失敗，沒有產生任何有效數據。")


if __name__ == "__main__":
    import sys

    try:
        watch_list = get_dynamic_watchlist()
        if watch_list:
            generate_ml_dataset(watch_list)
        else:
            print("⚠️ SQL 連線成功，但名單內沒有股票代碼，任務中止。")
    except Exception as e:
        print("🛑 [系統中斷] 無法獲取動態名單，歷史課本印製任務強制取消！")
        print(f"   原因: {e}")
        sys.exit(1)
