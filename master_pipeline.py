import subprocess
import time
from datetime import datetime
import os
import logging
import json
import pandas as pd
import yfinance as yf
import joblib
# 修改 master_pipeline.py 最上方的這行：
from screening import add_chip_data, extract_ai_features, inspect_stock, smart_download



# ==========================================
# 🔥 Logging 設定 (戰情日誌)
# ==========================================
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def log(msg):
    print(msg)
    logging.info(msg)

# ==========================================
# 🔧 執行腳本（強化版：含重試、超時與 UTF-8 強制防護）
# ==========================================
def run_script(script_name, retries=2, timeout=600):
    if not os.path.exists(script_name):
        log(f"⚠️ 找不到 {script_name}，跳過")
        return False

    # 🌟 系統級免疫：強迫子程式使用 UTF-8 編碼，解決 Windows 看不懂 Emoji 的當機問題
    custom_env = os.environ.copy()
    custom_env["PYTHONIOENCODING"] = "utf-8"

    for attempt in range(retries + 1):
        log(f"🚀 執行 {script_name}（第 {attempt+1} 次）")
        start = time.time()
        try:
            result = subprocess.run(
                ['python', script_name],
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding='utf-8',       # 🌟 強制主程式用 UTF-8 解碼
                env=custom_env          # 🌟 強制子程式用 UTF-8 輸出
            )
            log(result.stdout)
            elapsed = time.time() - start
            log(f"✅ 完成 {script_name}（{elapsed:.1f}s）")
            return True
        except subprocess.TimeoutExpired:
            log(f"⏰ Timeout: {script_name} 執行超時！")
        except subprocess.CalledProcessError as e:
            log(f"❌ 錯誤: {script_name} 執行失敗！")
            log(e.stderr)
        time.sleep(2) 
    return False

def validate_outputs():
    # 1. 檢查課本是否印出
    if not os.path.exists("data/ml_training_data.csv"):
        log("❌ 驗證失敗：特徵訓練教材 (ml_training_data.csv) 未產出！")
        return False

    # 2. 🌟 終極縫合：只要有產出【任何一顆】大腦，就允許放行！(防死鎖)
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
        "status": status
    }
    with open("daily_report.json", "w") as f:
        json.dump(report, f, indent=4)
    log(f"📊 已生成系統日誌 daily_report.json")

# 替換 master_pipeline.py 中的 get_recent_winrate
def get_recent_winrate():
    """從 SQL 戰績表讀取近期實戰勝率，達成完美數據閉環！"""
    try:
        DB_CONN_STR = (
            r'DRIVER={ODBC Driver 17 for SQL Server};'
            r'SERVER=localhost;'  
            r'DATABASE=股票online;'
            r'Trusted_Connection=yes;'
        )
        import pyodbc
        with pyodbc.connect(DB_CONN_STR) as conn:
            # 撈取最近 50 筆交易紀錄
            query = "SELECT TOP 50 [報酬率(%)] FROM backtest_history ORDER BY [出場時間] DESC"
            df_stats = pd.read_sql(query, conn)
            
            if not df_stats.empty:
                total_trades = len(df_stats)
                wins = len(df_stats[df_stats['報酬率(%)'] > 0])
                winrate = wins / total_trades
                log(f"📊 戰情雷達：近期 {total_trades} 筆實戰勝率為 {winrate:.1%}")
                return winrate
    except Exception as e:
        log(f"⚠️ 無法連線 SQL 讀取歷史勝率，預設回傳 0.5 穩態值 ({e})")
        
    return 0.5
def should_retrain():
    """判斷今天是否需要叫 AI 進入精神時光屋"""
    today = datetime.now().weekday()

    # 1. 🗓️ 週日大保養：每週日強制重新訓練大腦
    if today == 6:  
        log("🗓️ 系統判定：今日為週日，啟動【例行性 AI 大腦重塑】！")
        return True

    # 2. 🚨 緊急防護網：如果近期勝率跌破 40%，啟動緊急重訓
    recent_winrate = get_recent_winrate()
    if recent_winrate < 0.4:
        log(f"🚨 系統警告：近期勝率跌至 {recent_winrate:.1%}！啟動【緊急防禦性重訓】！")
        return True

    # 平日且勝率穩定時，不浪費算力重訓
    return False

def load_market_data(watch_list):
    log(f"📡 戰情中心：正在同步 {len(watch_list)} 檔標的資料 (⚡啟用智慧快取)...")
    data_dict = {}
    for ticker in watch_list:
        # 🌟 換成智慧下載器
        df = smart_download(ticker, period="1y")
        if df.empty: continue
            
        df.dropna(subset=['Close'], inplace=True)
        if df.empty: continue
            
        df = add_chip_data(df, ticker)
        data_dict[ticker] = df
    return data_dict

def analyze_signal(row, proba):
    features = []
    regime = row.get('Regime', '區間盤整')

    # 🟢 多方結構解析
    if regime in ['趨勢多頭', '區間盤整']:
        if row.get('buy_c6', 0): features.append(("突破均線", 2))
        if row.get('buy_c3', 0): features.append(("爆量發動", 1.5))
        if row.get('buy_c2', 0): features.append(("超賣", 1))
        if row.get('buy_c7', 0): features.append(("主力買", 2))
        if row.get('buy_c5', 0) or row.get('buy_c9', 0): features.append(("底背離", 2))

    # 🔴 空方結構解析
    if regime in ['趨勢空頭', '區間盤整']:
        if row.get('sell_c6', 0): features.append(("跌破均線", 2))
        if row.get('sell_c3', 0): features.append(("爆量下殺", 1.5))
        if row.get('sell_c2', 0): features.append(("超買", 1))
        if row.get('sell_c7', 0): features.append(("主力賣", 2))
        if row.get('sell_c5', 0) or row.get('sell_c9', 0): features.append(("頂背離", 2))

    total_score = sum(w for _, w in features)
    # 取前三項最明顯的特徵顯示，避免報表太長
    structure = " + ".join([f for f, _ in features[:3]]) if features else "無明顯結構"
    
    # 信心分數計算
    confidence = proba * (total_score / 8.5) if total_score > 0 else 0

    # 風險評估
    if regime == '趨勢空頭' and row.get('buy_c2', 0):
        risk = "⚠️ 高危(逆勢接刀)"
    elif regime == '趨勢多頭' and row.get('sell_c2', 0):
        risk = "⚠️ 高危(逆勢摸頭)"
    elif row.get('buy_c6', 0) or row.get('sell_c6', 0):
        risk = "🛡️ 順勢安全"
    else:
        risk = "⚡ 盤整震盪"

    return structure, confidence, risk

def generate_advanced_report(data_dict, ai_models):
    results = []
    for ticker, raw_df in data_dict.items():
        inspection = inspect_stock(ticker, preloaded_df=raw_df)
        if not inspection: 
            continue 
            
        processed_df = inspection["計算後資料"]
        latest_row = processed_df.iloc[-1]
        regime = latest_row.get('Regime', '區間盤整')
        
        # 替換 master_pipeline.py 的 generate_advanced_report 中的預測區塊
        proba = 0.0
        if regime in ai_models and ai_models[regime] is not None:
            features_dict = extract_ai_features(latest_row)
            X_input = pd.DataFrame([features_dict])
            
            # 🌟 完美閉合：強制讀取兵工廠的特徵順序，保證實戰與訓練 100% 吻合！
            feature_list_path = "models/selected_features.pkl"
            if os.path.exists(feature_list_path):
                selected_features = joblib.load(feature_list_path)
                # 重新排列欄位，缺少的補 0
                X_input = X_input.reindex(columns=selected_features).fillna(0)
            
            try:
                # 防呆：確保 AI 大腦有學過兩種結果 (勝與敗)，否則 [1] 會報錯 IndexError
                model = ai_models[regime]
                if len(model.classes_) > 1:
                    proba = model.predict_proba(X_input)[0][1]
                else:
                    proba = 0.0 # 該陣型歷史全敗，無法給出勝率
            except Exception as e:
                log(f"⚠️ {ticker} 預測發生異常: {e}")
            
                
        structure, confidence, risk = analyze_signal(latest_row, proba)
        
        # 🌟 終極拼圖：直接從雷達兵的健檢報告提取「該檔股票專屬的歷史勝率」
        try:
            hist_win_rate = float(inspection.get("系統勝率(%)", 50)) / 100.0
        except:
            hist_win_rate = 0.5 # 預設給 50%
            
        # 🌟 完美還原文件設定的黃金權重：50% AI預測 + 30% 結構信心 + 20% 歷史勝率
        final_score = (proba * 0.5) + (confidence * 0.3) + (hist_win_rate * 0.2)

        if final_score > 0: 
            results.append({
                "Ticker": ticker,
                "AI_Proba": proba,
                "Structure": structure,
                "Risk": risk,
                "Hist_Win_Rate": hist_win_rate, # ✨ 寫入歷史勝率
                "Score": final_score
            })

    if not results: return pd.DataFrame()
    return pd.DataFrame(results).sort_values("Score", ascending=False)


def main():
    start_time = time.time()
    # 🌟 取得今天是星期幾 (5 是週六，6 是週日)
    is_weekend = datetime.now().weekday() >= 5

    log("\n" + "="*60)
    log("⚙️ HFA 全自動研究與訓練管線 (Auto-MLOps) 啟動")
    log("="*60)

    # ==========================================
    # 第一階段：每天必跑的「資料更新」
    # ==========================================
    if is_weekend:
        log("\n⏳ 階段：資料更新 (⚠️ 今日為週末休市，跳過 API 爬蟲，節省資源！)")
    else:
        log("\n⏳ 階段：資料更新 (爬取最新 K 線與籌碼)")
        if not run_script("daily_chip_etl.py"):
            log("🛑 資料庫更新失敗，強制中斷！")
            generate_report(start_time, "FAILED")
            return

    # ==========================================
    # 第二階段：智慧分流 (判斷是否重訓，並拔除舊版優化器)
    # ==========================================
    if should_retrain():
        log("\n🧠 智慧決策：啟動兵工廠與精神時光屋 (AI 深度學習模式)")
        training_pipeline = [
            ("ml_data_generator.py", "特徵生成 (製作 AI 雙向訓練課本)"),
            ("ml_trainer.py", "模型訓練 (鍛造多空盤整三核心大腦)")
        ]
        for script, desc in training_pipeline:
            log(f"\n⏳ 階段：{desc}")
            if not run_script(script):
                log("🛑 AI 訓練發生異常，管線中斷！")
                generate_report(start_time, "FAILED")
                return
                
        # 驗證新大腦是否順利產出
        log("\n🔍 開始嚴格驗證兵工廠輸出結果...")
        if not validate_outputs():
            log("🛑 裝備檢查失敗，請勿執行實戰機台！")
            generate_report(start_time, "INVALID")
            return
    else:
        log("\n⚡ 智慧決策：今日勝率穩定且非大保養日，【跳過 AI 重訓】，直接使用現役大腦！")
        
    generate_report(start_time, "SUCCESS")

    # ==========================================
    # 第三階段：🌟 直接在當前視窗進行戰情播報！
    # ==========================================
    log("\n" + "="*70)
    log(f"📊 {datetime.now().strftime('%Y-%m-%d')} 戰情決策桌生成中...")
    log("="*70)

    # 載入現役大腦
    ai_models = {}
    for regime in ['趨勢多頭', '區間盤整', '趨勢空頭']:
        model_path = f"models/model_{regime}.pkl"
        if os.path.exists(model_path):
            ai_models[regime] = joblib.load(model_path)

    watch_list = ["2330.TW", "2454.TW", "2317.TW", "2382.TW", "3231.TW", "2603.TW", "1519.TW"]
    data_dict = load_market_data(watch_list)
    df_report = generate_advanced_report(data_dict, ai_models)

    if df_report.empty:
        log("📭 今日無符合進場結構之標的。")
    else:
        for i, row in df_report.head(10).iterrows():
            proba = row['AI_Proba']
            hist_win = row['Hist_Win_Rate']
            # 定義強度燈號 (綜合考量)
            strength = "🔥 強烈建議" if row['Score'] >= 0.60 else "⚡ 伺機而動" if row['Score'] >= 0.50 else "🟡 觀望"

            log(f"🎯 標的: {row['Ticker']}")
            log(f"   ► AI 勝率預測: {proba:.1%} | 歷史回測勝率: {hist_win:.1%} | 綜合決策分: {row['Score']:.2f}")
            log(f"   ► 戰略結構: {row['Structure']}")
            log(f"   ► 風險評估: {row['Risk']}")
            log(f"   ► 系統判定: {strength}")
            log("-" * 50)
            
        df_report.to_csv("daily_decision_desk.csv", index=False)
        log("💾 報告已同步儲存至 daily_decision_desk.csv")

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore') 
    main()