import json
import logging
import os
import subprocess
import time
from datetime import datetime

import joblib
import pandas as pd
import pyodbc
import yfinance as yf

from performance import check_strategy_health
from screening import add_chip_data, extract_ai_features, inspect_stock, normalize_ticker_symbol, smart_download

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def log(msg):
    print(msg)
    logging.info(msg)


def run_script(script_name, retries=2, timeout=900):
    if not os.path.exists(script_name):
        log(f"⚠️ 找不到 {script_name}，跳過")
        return False

    custom_env = os.environ.copy()
    custom_env["PYTHONIOENCODING"] = "utf-8"

    for attempt in range(retries + 1):
        log(f"🚀 執行 {script_name}（第 {attempt + 1} 次）")
        start = time.time()
        try:
            result = subprocess.run(
                ["python", script_name],
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding="utf-8",
                env=custom_env,
            )
            if result.stdout:
                log(result.stdout)
            if result.stderr:
                log(result.stderr)
            elapsed = time.time() - start
            log(f"✅ 完成 {script_name}（{elapsed:.1f}s）")
            return True
        except subprocess.TimeoutExpired:
            log(f"⏰ Timeout: {script_name} 執行超時！")
        except subprocess.CalledProcessError as e:
            log(f"❌ 錯誤: {script_name} 執行失敗！")
            if e.stderr:
                log(e.stderr)
        time.sleep(2)
    return False


def validate_outputs():
    if not os.path.exists("data/ml_training_data.csv"):
        log("❌ 驗證失敗：特徵訓練教材 (ml_training_data.csv) 未產出！")
        return False

    os.makedirs("models", exist_ok=True)
    models_found = [f for f in os.listdir("models") if f.startswith("model_") and f.endswith(".pkl")]
    if not models_found:
        log("❌ 驗證失敗：精神時光屋未能鍛造出任何 AI 大腦！")
        return False

    log(f"✅ 驗證通過：兵工廠成功掛載 {len(models_found)} 顆 AI 大腦！")
    return True


def generate_report(start_time, status):
    report = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration_sec": round(time.time() - start_time, 2),
        "status": status,
    }
    with open("daily_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)
    log("📊 已生成系統日誌 daily_report.json")


def get_system_performance():
    try:
        DB_CONN_STR = (
            r"DRIVER={ODBC Driver 17 for SQL Server};"
            r"SERVER=localhost;"
            r"DATABASE=股票online;"
            r"Trusted_Connection=yes;"
        )
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = "SELECT TOP 100 [報酬率(%)], [淨損益金額], [結餘本金] FROM trade_history ORDER BY [出場時間] DESC"
            df_stats = pd.read_sql(query, conn)

            if not df_stats.empty:
                df_stats = df_stats.iloc[::-1].reset_index(drop=True)

                total_trades = len(df_stats)
                wins = len(df_stats[df_stats["報酬率(%)"] > 0])
                winrate = wins / total_trades if total_trades > 0 else 0

                gross_profit = df_stats[df_stats["淨損益金額"] > 0]["淨損益金額"].sum()
                gross_loss = abs(df_stats[df_stats["淨損益金額"] < 0]["淨損益金額"].sum())
                profit_factor = gross_profit / gross_loss if gross_loss != 0 else 99.9

                if "結餘本金" in df_stats.columns and df_stats["結餘本金"].notna().any():
                    df_stats["Peak"] = df_stats["結餘本金"].cummax()
                    df_stats["Drawdown"] = (df_stats["結餘本金"] - df_stats["Peak"]) / df_stats["Peak"]
                    mdd = abs(df_stats["Drawdown"].min())
                else:
                    mdd = 0.0

                log(f"📊 [系統戰績] 近 {total_trades} 筆 | 勝率: {winrate:.1%} | 獲利因子(PF): {profit_factor:.2f} | 最大回撤(MDD): {mdd:.1%}")
                return winrate, profit_factor, mdd
    except Exception as e:
        log(f"⚠️ 無法連線 SQL 讀取歷史戰績，預設回傳穩態值 ({e})")

    return 0.5, 1.0, 0.0


def should_retrain():
    today = datetime.now().weekday()

    if today == 6:
        log("🗓️ 系統判定：今日為週日，啟動【例行性 AI 大腦重塑】！")
        return True

    recent_winrate, recent_pf, recent_mdd = get_system_performance()

    if recent_winrate < 0.4:
        log(f"🚨 系統警告：近期勝率跌至 {recent_winrate:.1%} (低於 40%)！啟動【緊急重訓】！")
        return True
    if recent_mdd > 0.15:
        log(f"🚨 系統警告：近期資金最大回撤達 {recent_mdd:.1%} (破 15%)！啟動【防禦性重訓】！")
        return True
    if recent_pf < 0.8 and recent_pf != 0:
        log(f"⚠️ 系統警告：獲利因子降至 {recent_pf:.2f}！啟動【校正性重訓】！")
        return True

    return False


def get_dynamic_watchlist():
    log("📡 啟動動態索敵雷達：正在從 SQL 資料庫獲取監控名單...")
    try:
        DB_CONN_STR = (
            r"DRIVER={ODBC Driver 17 for SQL Server};"
            r"SERVER=localhost;"
            r"DATABASE=股票online;"
            r"Trusted_Connection=yes;"
        )
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT [Ticker SYMBOL] FROM daily_chip_data")
            rows = cursor.fetchall()
            dynamic_list = [normalize_ticker_symbol(row[0]) for row in rows if row[0]]

            if dynamic_list:
                log(f"✅ 成功鎖定 {len(dynamic_list)} 檔目標進行掃描！")
                return dynamic_list
            raise ValueError("資料庫中找不到任何股票代碼！請檢查資料表是否為空。")
    except Exception as e:
        log(f"🛑 致命錯誤：無法連線 SQL 或獲取動態名單！詳細原因: {e}")
        raise


def load_market_data(watch_list):
    log(f"📡 戰情中心：正在同步 {len(watch_list)} 檔標的資料 (⚡啟用智慧快取)...")
    data_dict = {}
    for ticker in watch_list:
        ticker = normalize_ticker_symbol(ticker)
        df = smart_download(ticker, period="2y")
        if df.empty:
            log(f"⚠️ {ticker} 無 K 線資料，跳過")
            continue
        df = add_chip_data(df, ticker)
        data_dict[ticker] = df
    log(f"✅ 已完成 {len(data_dict)} 檔資料載入")
    return data_dict


def analyze_market_climate():
    try:
        twii = yf.download("^TWII", period="6mo", progress=False, auto_adjust=False)
        if twii.empty:
            return "未知", 1.0

        if isinstance(twii.columns, pd.MultiIndex):
            twii = twii.xs("^TWII", axis=1, level=1).copy()

        twii["MA20"] = twii["Close"].rolling(20).mean()
        twii["MA60"] = twii["Close"].rolling(60).mean()

        latest = twii.iloc[-1]
        climate = "中性"
        risk_multiplier = 1.0

        if latest["Close"] > latest["MA20"] > latest["MA60"]:
            climate = "多頭順風"
            risk_multiplier = 1.0
            log("🌤️ 大盤氣候判定：多頭順風，允許標準火力。")
        elif latest["Close"] < latest["MA20"] < latest["MA60"]:
            climate = "空頭逆風"
            risk_multiplier = 0.5
            log("⛈️ 大盤氣候判定：空頭逆風，系統自動降載 50%。")
        else:
            climate = "震盪盤"
            risk_multiplier = 0.75
            log("🌫️ 大盤氣候判定：震盪盤，系統保守降載 25%。")

        return climate, risk_multiplier
    except Exception as e:
        log(f"⚠️ 大盤探測發生異常: {e}")
        return "未知", 1.0


def build_decision_desk(watch_list, market_data, global_risk_multiplier):
    ai_models = {}
    selected_features = []

    if os.path.exists("models/selected_features.pkl"):
        try:
            selected_features = joblib.load("models/selected_features.pkl")
        except Exception as e:
            log(f"⚠️ 特徵檔載入失敗：{e}")

    for regime in ["趨勢多頭", "區間盤整", "趨勢空頭"]:
        model_path = f"models/model_{regime}.pkl"
        if os.path.exists(model_path):
            try:
                ai_models[regime] = joblib.load(model_path)
            except Exception as e:
                log(f"⚠️ {regime} 模型載入失敗：{e}")

    rows = []
    for ticker in watch_list:
        if ticker not in market_data:
            continue

        df = market_data[ticker]
        result = inspect_stock(ticker, preloaded_df=df)
        if not result or "計算後資料" not in result:
            continue

        latest_row = result["計算後資料"].iloc[-1]
        regime = result.get("Regime", latest_row.get("Regime", "區間盤整"))
        score = result.get("期望值", 0.0)
        hist_win = result.get("系統勝率(%)", 50.0) / 100

        setup_tag = result.get("Golden_Type", "無")
        if setup_tag == "無":
            continue

        model = ai_models.get(regime)
        proba = hist_win
        if model is not None:
            try:
                features_dict = extract_ai_features(latest_row)
                if selected_features:
                    X_input = pd.DataFrame([{f: features_dict.get(f, 0) for f in selected_features}])
                else:
                    X_input = pd.DataFrame([features_dict])
                proba = float(model.predict_proba(X_input)[0][1])
            except Exception as e:
                log(f"⚠️ {ticker} AI 預測失敗，改用歷史勝率: {e}")
                proba = hist_win

        action = "做多(Long)" if "多" in setup_tag else "做空(Short)"
        health_status, health_note = check_strategy_health(setup_tag)
        base_kelly = max(0.0, min(0.2, (proba - 0.5) * 0.4 + max(0, score) * 0.05))
        final_kelly = 0.0 if health_status == "KILL" else round(base_kelly * global_risk_multiplier, 4)

        rows.append({
            "Ticker": ticker,
            "Direction": action,
            "Regime": regime,
            "Structure": setup_tag,
            "AI_Proba": round(proba, 4),
            "Hist_WinRate": round(hist_win, 4),
            "EV": round(score, 4),
            "Kelly_Pos": final_kelly,
            "Score": round((proba * 0.6 + hist_win * 0.4), 4),
            "Risk": health_note,
            "Health": health_status,
        })

    df_report = pd.DataFrame(rows)
    if not df_report.empty:
        df_report.sort_values(["Kelly_Pos", "AI_Proba", "Hist_WinRate"], ascending=False, inplace=True)
    df_report.to_csv("daily_decision_desk.csv", index=False, encoding="utf-8-sig")
    log("💾 報告已同步儲存至 daily_decision_desk.csv")
    return df_report


def main():
    start_time = time.time()
    is_weekend = datetime.now().weekday() >= 5

    log("\n" + "=" * 60)
    log("⚙️ HFA 全自動研究與訓練管線 (Auto-MLOps) 啟動")
    log("=" * 60)

    if is_weekend:
        log("\n⏳ 階段：資料更新 (⚠️ 今日為週末休市，跳過 API 爬蟲，節省資源！)")
    else:
        log("\n⏳ 階段：資料更新 (爬取最新 K 線與籌碼)")
        if not run_script("daily_chip_etl.py"):
            log("🛑 資料庫更新失敗，強制中斷！")
            generate_report(start_time, "FAILED")
            return

    if should_retrain():
        log("\n🧠 智慧決策：啟動兵工廠與精神時光屋 (AI 深度學習模式)")
        training_pipeline = [
            ("ml_data_generator.py", "特徵生成"),
            ("ml_trainer.py", "模型訓練"),
        ]
        for script, desc in training_pipeline:
            log(f"\n⏳ 階段：{desc}")
            if not run_script(script):
                log("🛑 AI 訓練發生異常，管線中斷！")
                generate_report(start_time, "FAILED")
                return

        log("\n🔍 開始嚴格驗證兵工廠輸出結果...")
        if not validate_outputs():
            log("🛑 裝備檢查失敗，請勿執行實戰機台！")
            generate_report(start_time, "INVALID")
            return
    else:
        log("\n⚡ 智慧決策：今日勝率穩定且非大保養日，【跳過 AI 重訓】，直接使用現役大腦！")

    log("\n" + "=" * 70)
    log(f"📊 {datetime.now().strftime('%Y-%m-%d')} 戰情決策桌生成中...")
    log("=" * 70)

    climate_status, global_risk_multiplier = analyze_market_climate()
    log(f"🌍 市場氣候：{climate_status} | 風險倍率：{global_risk_multiplier:.2f}")

    try:
        watch_list = get_dynamic_watchlist()
    except Exception:
        generate_report(start_time, "FAILED")
        return

    market_data = load_market_data(watch_list)
    df_report = build_decision_desk(watch_list, market_data, global_risk_multiplier)

    if not df_report.empty:
        TOTAL_CAPITAL = 10_000_000
        for _, row in df_report.head(20).iterrows():
            final_allocation_pct = row["Kelly_Pos"]
            target_amount = TOTAL_CAPITAL * final_allocation_pct

            log(f"🎯 標的: {row['Ticker']}")
            log(f"   ► AI 勝率預測: {row['AI_Proba']:.1%} | 歷史回測勝率: {row['Hist_WinRate']:.1%} | 綜合決策分: {row['Score']:.2f}")
            log(f"   ► 戰略結構: {row['Structure']}")
            log(f"   ► 風險評估: {row['Risk']}")
            if final_allocation_pct > 0:
                msg = f"   💰 最終資金指派: 建議配置總資金的 {final_allocation_pct:.1%} (約 ${target_amount:,.0f} 元)"
                if global_risk_multiplier < 1.0:
                    msg += " [⚠️ 已受大盤降載保護]"
                log(msg)
            else:
                log("   💰 資金控管: 數學期望值過低，建議極小資金試單或空手")
            log("-" * 50)
    else:
        log("⚠️ 今日沒有合格標的產出。")

    generate_report(start_time, "SUCCESS")

    if not is_weekend:
        log("\n⏳ 階段：呼叫自動下單機進行帳戶結算與模擬建倉...")
        run_script("live_paper_trading.py")


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    main()
