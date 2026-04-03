import subprocess
import time
from datetime import datetime
import os
import logging
import json
import pandas as pd
import yfinance as yf
import joblib
from performance import check_strategy_health
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

# ==========================================
# 🌟 升級版：機構級戰績評估中心 (Profit Factor & MDD)
# ==========================================
def get_system_performance():
    """從 SQL 讀取實戰戰績，計算機構級指標：勝率、獲利因子、最大回撤(MDD)"""
    try:
        DB_CONN_STR = (
            r'DRIVER={ODBC Driver 17 for SQL Server};'
            r'SERVER=localhost;'  
            r'DATABASE=股票online;'
            r'Trusted_Connection=yes;'
        )
        import pyodbc
        with pyodbc.connect(DB_CONN_STR) as conn:
            # 撈取最近 100 筆交易紀錄
            query = "SELECT TOP 100 [報酬率(%)], [淨損益金額], [結餘本金] FROM trade_history ORDER BY [出場時間] DESC"
            df_stats = pd.read_sql(query, conn)
            
            if not df_stats.empty:
                # 把時間倒轉回來 (從舊到新)，才能正確計算資金曲線與回撤
                df_stats = df_stats.iloc[::-1].reset_index(drop=True)
                
                total_trades = len(df_stats)
                wins = len(df_stats[df_stats['報酬率(%)'] > 0])
                winrate = wins / total_trades if total_trades > 0 else 0
                
                # 💰 計算獲利因子 (Profit Factor = 總賺錢金額 / 總賠錢金額)
                gross_profit = df_stats[df_stats['淨損益金額'] > 0]['淨損益金額'].sum()
                gross_loss = abs(df_stats[df_stats['淨損益金額'] < 0]['淨損益金額'].sum())
                profit_factor = gross_profit / gross_loss if gross_loss != 0 else 99.9
                
                # 📉 計算最大回撤 (Max Drawdown, MDD)
                if '結餘本金' in df_stats.columns and df_stats['結餘本金'].notna().any():
                    df_stats['Peak'] = df_stats['結餘本金'].cummax()
                    df_stats['Drawdown'] = (df_stats['結餘本金'] - df_stats['Peak']) / df_stats['Peak']
                    mdd = abs(df_stats['Drawdown'].min())
                else:
                    mdd = 0.0

                log(f"📊 [系統戰績] 近 {total_trades} 筆 | 勝率: {winrate:.1%} | 獲利因子(PF): {profit_factor:.2f} | 最大回撤(MDD): {mdd:.1%}")
                return winrate, profit_factor, mdd
                
    except Exception as e:
        log(f"⚠️ 無法連線 SQL 讀取歷史戰績，預設回傳穩態值 ({e})")
        
    return 0.5, 1.0, 0.0

# ==========================================
# 🌟 升級版：AI 動態自我修復決策 (Self-Healing MLOps)
# ==========================================
def should_retrain():
    """判斷今天是否需要叫 AI 進入精神時光屋"""
    today = datetime.now().weekday()

    # 1. 🗓️ 週日大保養：每週日強制重新訓練大腦
    if today == 6:  
        log("🗓️ 系統判定：今日為週日，啟動【例行性 AI 大腦重塑】！")
        return True

    # 2. 🚨 提取最新機構級戰績
    recent_winrate, recent_pf, recent_mdd = get_system_performance()
    
    # 3. 🛡️ 雙重極限防護網 (勝率或回撤任一破底，立刻重訓)
    if recent_winrate < 0.4:
        log(f"🚨 系統警告：近期勝率跌至 {recent_winrate:.1%} (低於 40%)！大腦可能已失效，啟動【緊急重訓】！")
        return True
        
    if recent_mdd > 0.15:
        log(f"🚨 系統警告：近期資金最大回撤達 {recent_mdd:.1%} (破 15%)！偵測到連續嚴重失血，啟動【防禦性重訓】！")
        return True
        
    if recent_pf < 0.8 and recent_pf != 0:
        log(f"⚠️ 系統警告：獲利因子降至 {recent_pf:.2f} (賺不夠賠)！提前啟動【校正性重訓】！")
        return True

    # 平日且指標健康時，不浪費算力重訓
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
    
    # 🌟 修復 2：把特徵名單的讀取移到「迴圈外」，避免讀取硬碟千百次！
    feature_list_path = "models/selected_features.pkl"
    selected_features = None
    if os.path.exists(feature_list_path):
        import joblib
        selected_features = joblib.load(feature_list_path)
        
    for ticker, raw_df in data_dict.items():
        inspection = inspect_stock(ticker, preloaded_df=raw_df)
        if not inspection: 
            continue 
            
        processed_df = inspection["計算後資料"]
        latest_row = processed_df.iloc[-1]
        regime = latest_row.get('Regime', '區間盤整')
        
        proba = 0.0
        if regime in ai_models and ai_models[regime] is not None:
            features_dict = extract_ai_features(latest_row)
            X_input = pd.DataFrame([features_dict])
            
            # 強制對齊兵工廠的特徵順序
            if selected_features is not None:
                X_input = X_input.reindex(columns=selected_features).fillna(0)
            
            try:
                model = ai_models[regime]
                if len(model.classes_) > 1:
                    proba = model.predict_proba(X_input)[0][1]
                else:
                    proba = 0.0 
            except Exception as e:
                log(f"⚠️ {ticker} 預測發生異常: {e}")
        
        structure, confidence, risk = analyze_signal(latest_row, proba)
        
        try:
            hist_win_rate = float(inspection.get("系統勝率(%)", 50)) / 100.0
        except:
            hist_win_rate = 0.5 
            
        # ==========================================
        # 🌟 修復 1：真正植入「戰術淘汰防護網 (KILL Switch)」
        # ==========================================
        setup_tag = inspection.get("陣型標籤", "傳統訊號")
        health_status, health_msg = check_strategy_health(setup_tag)
        
        # 預先抓取凱利資金配比
        kelly_pct = inspection.get("建議倉位(%)", 0)
        
        # 💀 如果戰術被判定失效，強制沒收預算，並竄改風險標籤！
        if health_status == "KILL":
            kelly_pct = 0
            risk = f"💀 戰術失效阻斷 ({health_msg})"
            
        # 計算最終綜合決策分
        final_score = (proba * 0.5) + (confidence * 0.3) + (hist_win_rate * 0.2)

        if final_score > 0: 
            results.append({
                "Ticker": ticker,
                "AI_Proba": proba,
                "Structure": structure,
                "Risk": risk,
                "Hist_Win_Rate": hist_win_rate, 
                "Score": final_score,
                "Kelly_Pos": kelly_pct # ✨ 這裡寫入的是經過防護網檢驗後的安全倉位
            })

    if not results: return pd.DataFrame()
    return pd.DataFrame(results).sort_values("Score", ascending=False)

# ==========================================
# 🌍 終極拼圖：大盤氣候濾網 (Market Climate Filter)
# ==========================================
def analyze_market_climate():
    """分析台灣加權指數 (^TWII) 判定總體系統性風險，並輸出資金降載係數"""
    log("🌍 啟動大盤氣象雷達 (^TWII) 探測總體系統風險...")
    try:
        # 抓取大盤資料 (完美利用我們之前寫好的智慧快取)
        twii_df = smart_download("^TWII", period="1y")
        if twii_df.empty:
            return "數據中斷", 1.0

        twii_df.dropna(subset=['Close'], inplace=True)
        close = twii_df['Close'].iloc[-1]
        
        # 計算月線(20)與季線(60)作為多空生命線
        ma20 = twii_df['Close'].rolling(20).mean().iloc[-1]
        ma60 = twii_df['Close'].rolling(60).mean().iloc[-1]

        # 氣候矩陣判定與「系統性資金降載係數」
        if close > ma20 and close > ma60:
            climate = "🌞 萬里無雲 (強勢多頭 - 均線之上)"
            risk_multiplier = 1.0  # 天氣大好，允許 100% 火力全開
            
        elif close < ma20 and close < ma60:
            climate = "⛈️ 狂風暴雨 (強勢空頭 - 均線之下)"
            risk_multiplier = 0.3  # 空頭崩盤，所有多單預算強制只剩 30%！
            
        elif close > ma60 and close < ma20:
            climate = "⛅ 陰晴不定 (多頭回檔 - 跌破月線)"
            risk_multiplier = 0.6  # 漲多回檔，資金降載至 60%
            
        else:
            climate = "🌫️ 大霧瀰漫 (區間震盪 - 均線糾結)"
            risk_multiplier = 0.5  # 方向不明，資金減半防雙巴

        log(f"   ► 目前大盤指數: {close:,.0f} 點")
        log(f"   ► 季線(MA60)防守點: {ma60:,.0f} 點")
        log(f"   ► 今日氣候判定: {climate}")
        if risk_multiplier < 1.0:
            log(f"   🚨 觸發防禦機制：大環境不佳，全軍部位強制降載至 {risk_multiplier * 100:.0f}%")
        else:
            log(f"   🚀 大環境極佳：系統未觸發降載，允許全火力推進！")

        return climate, risk_multiplier
        
    except Exception as e:
        log(f"⚠️ 大盤探測發生異常: {e}")
        return "未知", 1.0


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

    # 🌟 啟動大盤氣候濾網，取得環境係數
    climate_status, global_risk_multiplier = analyze_market_climate()
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
        # 設定您的總資金 (例如：5000萬台幣)
        TOTAL_CAPITAL = 50000000
        
        for i, row in df_report.head(10).iterrows():
            proba = row['AI_Proba']
            hist_win = row['Hist_Win_Rate']
            kelly_pct = row['Kelly_Pos']
            
            strength = "🔥 強烈建議" if row['Score'] >= 0.60 else "⚡ 伺機而動" if row['Score'] >= 0.50 else "🟡 觀望"

            # 🌟 終極資金公式：本金 × 個股期望值(凱利) × 大盤天氣係數(風險降載)
            final_allocation_pct = kelly_pct * global_risk_multiplier
            target_amount = TOTAL_CAPITAL * final_allocation_pct
            
            log(f"🎯 標的: {row['Ticker']}")
            log(f"   ► AI 勝率預測: {proba:.1%} | 歷史回測勝率: {hist_win:.1%} | 綜合決策分: {row['Score']:.2f}")
            log(f"   ► 戰略結構: {row['Structure']}")
            log(f"   ► 風險評估: {row['Risk']}")
            log(f"   ► 系統判定: {strength}")
            if final_allocation_pct > 0:
                msg = f"   💰 最終資金指派: 建議配置總資金的 {final_allocation_pct:.1%} (約 ${target_amount:,.0f} 元)"
                if global_risk_multiplier < 1.0:
                    msg += f" [⚠️ 已受大盤降載保護]"
                log(msg)
            else:
                log(f"   💰 資金控管: 數學期望值過低，建議極小資金試單或空手")
            log("-" * 50)
            
        df_report.to_csv("daily_decision_desk.csv", index=False)
        log("💾 報告已同步儲存至 daily_decision_desk.csv")

    # ==========================================
    # 🌟 第四階段：執行全自動結算與下單中心
    # ==========================================
    if not is_weekend:
        log("\n⏳ 階段：呼叫自動下單機進行帳戶結算與模擬建倉...")
        run_script("live_paper_trading.py")

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore') 
    main()