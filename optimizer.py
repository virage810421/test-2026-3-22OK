import pandas as pd
import random
from copy import deepcopy
import yfinance as yf
import warnings
warnings.filterwarnings('ignore')  # 隱藏不必要的警告

from screening import inspect_stock, add_chip_data
from config import PARAMS as BASE_PARAMS

# ==========================================
# 🌌 1. 定義要探索的「參數宇宙 (Parameter Space)」
# AI 將會在這個範圍內隨機抽樣，尋找最佳解
# ==========================================
PARAM_SPACE = {
    "RSI_PERIOD": [10, 14, 20],               # RSI 天數
    "MACD_FAST": [10, 12, 15],                # MACD 快線
    "MACD_SLOW": [20, 26, 30],                # MACD 慢線
    "ADX_TREND_THRESHOLD": [15, 20, 25],      # 趨勢判定門檻
    "SL_MIN_PCT": [0.02, 0.03, 0.04],         # 最小停損%
    "TP_BASE_PCT": [0.08, 0.10, 0.12],        # 基礎停利%
    "MIN_RR_RATIO": [1.2, 1.5, 2.0]           # 風報比門檻
}

# ==========================================
# 🎯 2. 嚴選測試標的 (取各產業龍頭作為大盤縮影)
# ==========================================
TEST_TICKERS = ["2330.TW", "2454.TW", "2317.TW", "2603.TW", "2881.TW", "1519.TW"]

def generate_random_params():
    """產生一組隨機變異的平行宇宙參數"""
    new_params = deepcopy(BASE_PARAMS)
    for key, values in PARAM_SPACE.items():
        new_params[key] = random.choice(values)
    return new_params

def run_optimization(iterations=10):
    """
    自動優化引擎核心：
    iterations = 嘗試的參數組合數量
    """
    print(f"\n🚀 啟動 Layer 3：AI 參數尋標引擎 (準備測試 {iterations} 組平行宇宙)...\n")
    
    # --- A. 預先下載歷史資料 (節省重複抓取的時間) ---
    print("📥 正在為實驗室下載歷史 K 線與法人籌碼，請稍候...")
    batch_data = yf.download(TEST_TICKERS, period="2y", progress=False)
    
    preloaded_dfs = {}
    for ticker in TEST_TICKERS:
        df = batch_data.xs(ticker, axis=1, level=1).copy() if isinstance(batch_data.columns, pd.MultiIndex) else batch_data.copy()
        df.dropna(subset=['Close'], inplace=True)
        df.ffill(inplace=True)
        if df.empty: continue
        # 貼上法人籌碼
        df = add_chip_data(df, ticker)
        preloaded_dfs[ticker] = df

    print("✅ 實驗室資料準備完畢，開始高速回測！\n")
    results_log = []

    # --- B. 啟動平行宇宙回測 ---
    for i in range(1, iterations + 1):
        candidate_params = generate_random_params()
        print(f"🔄 [測試進度 {i}/{iterations}] 正在驗證新變異參數...", end="\r")
        
        total_ev = 0.0
        valid_stocks = 0
        
        for ticker in TEST_TICKERS:
            if ticker not in preloaded_dfs: continue
            df = preloaded_dfs[ticker].copy()
            
            # 🌟 核心魔法：將變異參數 (candidate_params) 丟給大腦進行客製化回測！
            result = inspect_stock(ticker, preloaded_df=df, p=candidate_params)
            
            if result and not pd.isna(result.get("期望值")):
                ev = float(result["期望值"])
                # 簡單過濾掉極端異常值
                if -20 < ev < 20: 
                    total_ev += ev
                    valid_stocks += 1
        
        # 結算這組參數的平均期望值
        avg_ev = total_ev / valid_stocks if valid_stocks > 0 else -999.0
        
        results_log.append({
            "Score (Avg EV)": avg_ev,
            "Params": {k: candidate_params[k] for k in PARAM_SPACE.keys()}
        })

    # --- C. 輸出排行榜 ---
    # 依照 EV 降序排列
    results_log.sort(key=lambda x: x["Score (Avg EV)"], reverse=True)
    
    print("\n\n" + "═"*45)
    print("🏆 AI 尋標完成！最強參數組合出爐：")
    print("═"*45)
    
    best = results_log[0]
    if best['Score (Avg EV)'] == -999.0:
        print("⚠️ 測試失敗：無法計算出有效期望值。")
        return

    print(f"🌟 新宇宙綜合期望值 (EV): {best['Score (Avg EV)']:.3f}%\n")
    
    for k, v in best["Params"].items():
        current_val = BASE_PARAMS.get(k)
        marker = "✨ (建議修改)" if v != current_val else "✅ (維持不變)"
        print(f"  {k.ljust(22)}: {str(v).ljust(6)} {marker}")
    
    print("\n💡 [系統建議] 您可以打開 config.py，將帶有 ✨ 的參數手動更新，完成系統進化！")
    print("═"*45)

if __name__ == "__main__":
    # 您可以把 5 改成 50 或 100，讓他跑更久找尋更精確的最佳解
    run_optimization(iterations=10)