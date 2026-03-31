import pandas as pd
import random
from copy import deepcopy
import yfinance as yf
import warnings
warnings.filterwarnings('ignore')

from screening import inspect_stock, add_chip_data
from config import PARAMS as BASE_PARAMS

# ==========================================
# 🌌 1. 擴充版「參數宇宙」(防過度擬合版)
# ==========================================
PARAM_SPACE = {
    "RSI_PERIOD": [10, 14, 20],               
    "MACD_FAST": [10, 12, 15],                
    "MACD_SLOW": [20, 26, 30],                
    "ADX_TREND_THRESHOLD": [15, 20, 25],      
    "BB_STD": [1.5, 2.0, 2.5],                
    "VOL_BREAKOUT_MULTIPLIER": [1.1, 1.5, 2.0], 
    "SL_MIN_PCT": [0.02, 0.03, 0.04],         
    "TP_BASE_PCT": [0.08, 0.10, 0.12],        
    "TP_TREND_PCT": [0.15, 0.20, 0.25],       
    "MIN_RR_RATIO": [1.2, 1.5, 2.0]           
}

TEST_TICKERS = ["2330.TW", "2454.TW", "2317.TW", "2603.TW", "2881.TW", "1519.TW"]

def generate_random_params():
    new_params = deepcopy(BASE_PARAMS)
    for key, values in PARAM_SPACE.items():
        new_params[key] = random.choice(values)
    return new_params

def run_walk_forward_optimization(iterations=50, split_ratio=0.7, ticker_list=None):
    # 如果有傳入特定產業清單就用傳入的，否則用預設的 TEST_TICKERS
    targets = ticker_list if ticker_list else TEST_TICKERS
    print(f"\n🚀 啟動 Layer 3：AI 滾動盲測尋標引擎 (準備測試 {iterations} 組)...\n")
    
    # 往下所有的 TEST_TICKERS 都要改成 targets
    print("📥 正在下載歷史 K 線與法人籌碼，並建立【訓練集】與【測試集】...")
    batch_data = yf.download(targets, period="2y", progress=False)
    """
    機構級 Walk-Forward 引擎：
    split_ratio = 0.7 代表前 70% 拿來訓練，後 30% 拿來盲測驗證。
    """
    print(f"\n🚀 啟動 Layer 3：AI 滾動盲測尋標引擎 (準備測試 {iterations} 組)...\n")
    
    # --- A. 預先下載並切分資料 ---
    print("📥 正在下載歷史 K 線與法人籌碼，並建立【訓練集】與【測試集】...")
    batch_data = yf.download(TEST_TICKERS, period="2y", progress=False)
    
    train_dfs = {}
    test_dfs = {}
    
    for ticker in targets:
        df = batch_data.xs(ticker, axis=1, level=1).copy() if isinstance(batch_data.columns, pd.MultiIndex) else batch_data.copy()
        df.dropna(subset=['Close'], inplace=True)
        df.ffill(inplace=True)
        if df.empty or len(df) < 100: continue
        
        df = add_chip_data(df, ticker)
        
        # 🌟 核心：將資料切成兩半 (Train vs Test)
        split_idx = int(len(df) * split_ratio)
        train_dfs[ticker] = df.iloc[:split_idx].copy()
        test_dfs[ticker] = df.iloc[split_idx:].copy()

    # ==========================================
    # 🧠 階段一：訓練集 (In-Sample) 尋找最佳解
    # ==========================================
    print("\n" + "="*50)
    print("🧠 [階段一] 進入精神時光屋：利用前 70% 歷史尋找最佳參數")
    print("="*50)
    
    results_log = []
    for i in range(1, iterations + 1):
        candidate_params = generate_random_params()
        
        # 🌟 [防護 1] 滑價壓力測試：強制把滑價耗損加倍 (例如 0.3%)
        candidate_params['MARKET_SLIPPAGE'] = 0.003
        
        print(f"🔄 [訓練進度 {i}/{iterations}] 正在驗證新變異參數...", end="\r")
        
        total_ev = 0.0
        total_score = 0.0  # ✨ 新增：用來記錄扣分後的終極分數
        valid_stocks = 0
        min_single_ev = 999.0  # 🌟 用來記錄最慘的那檔股票 EV
        
        for ticker in targets:
            if ticker not in train_dfs: continue
            df = train_dfs[ticker].copy()
            
            # 使用訓練集跑大腦回測
            result = inspect_stock(ticker, preloaded_df=df, p=candidate_params)
            
            if result and not pd.isna(result.get("期望值")):
                # 抓取大腦算好的各項數據
                ev = float(result.get("期望值", 0))
                min_single_ev = min(min_single_ev, ev) # 🌟 紀錄最低 EV
                
                win_rate = float(result.get("系統勝率(%)", result.get("勝率(%)", 50))) 
                mdd = abs(float(result.get("最大虧損(%)", 0)))
                trade_count = int(result.get("交易次數", 10))

                # 🌟 基礎分數就是 EV (賺錢能力)
                score = ev

                # 🔪 1. 心理壓力懲罰 (針對低勝率)
                if win_rate < 45:
                    score -= (45 - win_rate) * 0.1  

                # 🔪 2. 破產風險懲罰 (針對高 MDD)
                if mdd > 15:
                    score -= (mdd - 15) * 0.2

                # 🔪 3. 運氣成分懲罰 (針對單檔股票交易次數太少)
                if trade_count < 5:
                    score -= 2.0  

                # 過濾掉極端異常值後，加總平均
                if -20 < score < 20: 
                    total_score += score
                    total_ev += ev
                    valid_stocks += 1
        
        # 結算這組參數的平均表現
        avg_score = total_score / valid_stocks if valid_stocks > 0 else -999.0
        avg_ev = total_ev / valid_stocks if valid_stocks > 0 else -999.0

        # 🔪 4. 廣泛有效性懲罰 (如果測試標的裡只有不到一半能賺錢，代表參數太冷門)
        if valid_stocks < len(targets) * 0.5:  # 🌟 注意這裡改成 targets 了
            avg_score -= 1.5  

        # 🌟 [防護 2] 參數穩定度：不允許單一股票出現毀滅性虧損
        if min_single_ev < -0.5:
            avg_score -= 5.0  # 只要有一檔股票 EV 跌破 -0.5%，直接重罰淘汰！

        results_log.append({
            "Train_EV": avg_ev,       # 🌟 只用來印出顯示，不參與排名
            "Score": avg_score,       # 🌟 真正用來排名的終極分數
            "Params": candidate_params
        })

    # ==========================================
    # 🏆 結算：使用加了懲罰的「終極分數 (Score)」來選出冠軍
    # ==========================================
    results_log.sort(key=lambda x: x["Score"], reverse=True)
    best_candidate = results_log[0]
    
    if best_candidate['Train_EV'] == -999.0:
        print("⚠️ 訓練失敗：無法計算出有效期望值。")
        return

    print(f"\n🏆 訓練完成！在歷史資料中，最強參數的期望值為: {best_candidate['Train_EV']:.3f}%")

    # ==========================================
    # ⚔️ 階段二：測試集 (Out-of-Sample) 盲測驗證
    # ==========================================
    print("\n" + "="*50)
    print("⚔️ [階段二] 殘酷擂台盲測：拿冠軍參數去跑它沒見過的後 30% 最新行情")
    print("="*50)
    
    test_total_ev = 0.0
    test_valid_stocks = 0
    
    for ticker in targets:
        if ticker not in test_dfs: continue
        df = test_dfs[ticker].copy()
        
        # 使用盲測集跑大腦回測
        result = inspect_stock(ticker, preloaded_df=df, p=best_candidate["Params"])
        
        if result and not pd.isna(result.get("期望值")):
            ev = float(result["期望值"])
            if -20 < ev < 20: 
                test_total_ev += ev
                test_valid_stocks += 1
                
    test_avg_ev = test_total_ev / test_valid_stocks if test_valid_stocks > 0 else -999.0
    
    # ==========================================
    # 📊 最終報告輸出
    # ==========================================
    print("\n" + "═"*55)
    print(" 📊 AI 滾動盲測 (Walk-Forward) 最終健檢報告")
    print("═"*55)
    print(f" 🔹 歷史訓練期望值 (In-Sample):    {best_candidate['Train_EV']:.3f}%")
    print(f" 🔹 盲測實戰期望值 (Out-of-Sample): {test_avg_ev:.3f}%")
    
    if test_avg_ev > 1.0:
        print(" 🟢 評價：【極度強悍】這組參數經得起盲測考驗，強烈建議使用！")
    elif test_avg_ev > 0:
        print(" 🟡 評價：【及格邊緣】實戰會折損，但依然具備正向期望值，可小注試單。")
    else:
        print(" 🔴 評價：【過度擬合】這組參數在未來行情已失效 (EV < 0)，請重新執行優化！")
    print("═"*55)
    
    print("\n💡 [最佳參數清單] (只顯示有變更的部分)：")
    for k in PARAM_SPACE.keys():
        v = best_candidate["Params"][k]
        current_val = BASE_PARAMS.get(k)
        marker = "✨ (建議修改)" if v != current_val else "✅ (維持不變)"
        print(f"  {k.ljust(22)}: {str(v).ljust(6)} {marker}")
    # 🌟 輸出前，把滑價改回正常值 (例如 0.15%)，以免實戰機台被懲罰
    best_candidate["Params"]['MARKET_SLIPPAGE'] = 0.0015
    
    return {
        "Params": best_candidate["Params"],
        "Train_EV": best_candidate['Train_EV'],
        "Test_EV": test_avg_ev
    }
if __name__ == "__main__":
    # 將次數拉高到 50 次，讓 AI 有足夠的樣本找出真理
    run_walk_forward_optimization(iterations=100)