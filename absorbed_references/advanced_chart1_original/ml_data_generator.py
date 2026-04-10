import os
import pandas as pd
import numpy as np

from screening import inspect_stock, add_chip_data, extract_ai_features, smart_download
from config import PARAMS


def get_dynamic_watchlist():
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


def _signal_flags(setup_tag: str):
    tag = str(setup_tag).strip()
    is_short = ("空" in tag) or ("SHORT" in tag.upper())
    is_long = ("多" in tag) or ("LONG" in tag.upper())
    return is_long, is_short


def generate_ml_dataset(tickers):
    print("🏭 [兵工廠] 啟動 AI 雙向訓練資料生成器...")

    os.makedirs("data", exist_ok=True)

    dataset_path = "data/ml_training_data.csv"
    if os.path.exists(dataset_path):
        os.remove(dataset_path)
        print("🗑️ 已銷毀昨日舊有訓練資料，確保數據絕對純淨。")

    ml_dataset = []

    test_params = PARAMS.copy()
    test_params["IS_OPTIMIZING"] = True
    test_params["TRIGGER_SCORE"] = max(2, int(PARAMS.get("TRIGGER_SCORE", 1)))

    hold_days = int(PARAMS.get("ML_LABEL_HOLD_DAYS", 5))
    sl_pct = float(PARAMS.get("SL_MIN_PCT", 0.03))
    fee_rate = float(PARAMS.get("FEE_RATE", 0.001425)) * float(PARAMS.get("FEE_DISCOUNT", 1.0))
    tax_rate = float(PARAMS.get("TAX_RATE", 0.003))
    round_trip_cost = (2 * fee_rate) + tax_rate

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

            computed_df = result["計算後資料"].copy()
            if len(computed_df) <= hold_days + 2:
                continue

            for i in range(len(computed_df) - hold_days - 1):
                row = computed_df.iloc[i]
                setup_tag = str(row.get("Golden_Type", "無")).strip()
                regime = str(row.get("Regime", "區間盤整")).strip()

                if setup_tag == "無":
                    continue

                is_long, is_short = _signal_flags(setup_tag)
                if not is_long and not is_short:
                    continue

                entry_price = computed_df.iloc[i + 1]["Open"]
                future_window = computed_df.iloc[i + 1:i + 1 + hold_days].copy()

                if future_window.empty or pd.isna(entry_price) or entry_price <= 0:
                    continue

                features = extract_ai_features(row)
                features["Ticker"] = ticker
                features["Date"] = computed_df.index[i]
                features["Regime"] = regime
                features["Setup"] = setup_tag

                stop_hit = False
                label_y = 0

                if is_short:
                    adverse_move = (future_window["High"].max() - entry_price) / entry_price
                    if adverse_move > sl_pct:
                        stop_hit = True

                    realized_return = ((entry_price - future_window.iloc[-1]["Close"]) / entry_price) - round_trip_cost
                    favorable_move = (entry_price - future_window["Low"].min()) / entry_price

                    if (not stop_hit) and (realized_return > 0 or favorable_move > sl_pct):
                        label_y = 1
                else:
                    adverse_move = (entry_price - future_window["Low"].min()) / entry_price
                    if adverse_move > sl_pct:
                        stop_hit = True

                    realized_return = ((future_window.iloc[-1]["Close"] - entry_price) / entry_price) - round_trip_cost
                    favorable_move = (future_window["High"].max() - entry_price) / entry_price

                    if (not stop_hit) and (realized_return > 0 or favorable_move > sl_pct):
                        label_y = 1

                features["Label_Y"] = int(label_y)
                features["Target_Return"] = round(float(realized_return * 100.0), 4)
                features["Stop_Hit"] = int(stop_hit)
                features["Hold_Days"] = hold_days

                ml_dataset.append(features)

        except Exception as e:
            print(f"⚠️ {ticker} 萃取失敗：{e}")

    if not ml_dataset:
        print("❌ 沒有生成任何訓練樣本！")
        return pd.DataFrame()

    df_ml = pd.DataFrame(ml_dataset)
    df_ml.to_csv(dataset_path, index=False, encoding="utf-8-sig")

    print("======================================================")
    print(f"✅ 訓練教材生成完成：{len(df_ml)} 筆")
    print(f"📦 輸出檔案：{dataset_path}")
    print("======================================================")

    if "Label_Y" in df_ml.columns:
        print(df_ml["Label_Y"].value_counts(dropna=False))

    return df_ml


if __name__ == "__main__":
    tickers = get_dynamic_watchlist()
    generate_ml_dataset(tickers)
