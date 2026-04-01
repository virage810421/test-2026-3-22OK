import yfinance as yf  
import pandas as pd
import numpy as np
import random
from copy import deepcopy
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import Matern
import warnings
warnings.filterwarnings('ignore')

# 🌟 乾淨且單一的匯入
from config import PARAMS
from screening import inspect_stock, add_chip_data

# ==========================================
# 🌌 參數宇宙 (戰術指標範圍留在此處，風險底線連線至 config)
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
    "MIN_RR_RATIO": PARAMS.get('AI_RR_SEARCH_SPACE', [1.0, 1.2, 1.5, 2.0]),  # 🌟 連線至 config 中央控制            
}

TEST_TICKERS = ["2330.TW", "2454.TW", "2317.TW", "2603.TW", "2881.TW", "1519.TW"]
# ==========================================
# 🧠 核心 1：帕雷托前緣篩選 (Pareto Selection)
# ==========================================
def is_dominated(a, b):
    """加入「報酬率」作為全面輾壓的評判標準"""
    return (
        b["EV"] >= a["EV"] and
        b["WinRate"] >= a["WinRate"] and
        b["TotalReturn"] >= a["TotalReturn"] and # 🌟 新增：報酬率必須大於等於
        b["MDD"] <= a["MDD"] and 
        (b["EV"] > a["EV"] or b["WinRate"] > a["WinRate"] or b["TotalReturn"] > a["TotalReturn"] or b["MDD"] < a["MDD"])
    )

def get_pareto_frontier(results):
    pareto_front = []
    for i, res1 in enumerate(results):
        dominated = False
        for j, res2 in enumerate(results):
            if i != j and is_dominated(res1, res2):
                dominated = True
                break
        if not dominated:
            pareto_front.append(res1)
    return pareto_front

# ==========================================
# 🧠 核心 2：評估函數 (對接您現有的大腦)
# ==========================================
def evaluate_params(params, train_dfs, targets):
    total_ev = 0.0
    total_winrate = 0.0
    total_return = 0.0 # 🌟 新增：累計報酬率變數
    valid_stocks = 0
    
    for ticker in targets:
        if ticker not in train_dfs: continue
        df = train_dfs[ticker].copy()
        
        result = inspect_stock(ticker, preloaded_df=df, p=params)
        
        if result and not pd.isna(result.get("期望值")):
            ev = float(result.get("期望值", 0))
            win_rate = float(result.get("系統勝率(%)", 0))
            ret = float(result.get("累計報酬率(%)", 0)) # 🌟 新增：從大腦抓取報酬率
            
            if -20 < ev < 20: 
                total_ev += ev
                total_winrate += win_rate
                total_return += ret # 🌟 累加報酬率
                valid_stocks += 1
                
    if valid_stocks == 0:
        return {"EV": -999.0, "WinRate": 0.0, "TotalReturn": -999.0, "MDD": 999.0}
        
    avg_ev = total_ev / valid_stocks
    avg_winrate = total_winrate / valid_stocks
    avg_return = total_return / valid_stocks # 🌟 計算平均報酬率
    
    pseudo_mdd = 100.0 if avg_ev < 0 else (100.0 / (avg_ev + 1e-5)) 
    
    # 🌟 回傳時把 TotalReturn 也打包進去
    return {"EV": avg_ev, "WinRate": avg_winrate, "TotalReturn": avg_return, "MDD": pseudo_mdd}

# ==========================================
# 🚀 核心 3：貝氏最佳化主程式 (Bayesian Optimization)
# ==========================================
def run_bayesian_optimization(n_iter=30, split_ratio=0.7, ticker_list=None):
    targets = ticker_list if ticker_list else TEST_TICKERS
    print(f"\n🚀 啟動【機構級】貝氏最佳化引擎 (Bayesian Optimization)...\n")
    
    # --- 1. 下載與切分資料 ---
    # 🌟 修復 1：確保 period 為 "4y"
    batch_data = yf.download(targets, period="4y", progress=False)
    train_dfs = {}
    test_dfs = {} 
    
    for ticker in targets:
        try:
            df = batch_data.xs(ticker, axis=1, level=1).copy() if isinstance(batch_data.columns, pd.MultiIndex) else batch_data.copy()
            df.dropna(subset=['Close'], inplace=True)
            df = add_chip_data(df, ticker)
            split_idx = int(len(df) * split_ratio)
            train_dfs[ticker] = df.iloc[:split_idx].copy()   # 70% 供 AI 訓練
            test_dfs[ticker] = df.iloc[split_idx:].copy()    # 30% 鎖進保險箱，供最後盲測
        except Exception:
            continue

    # --- 2. 初始化高斯過程回歸模型 ---
    kernel = Matern(nu=2.5)
    model = GaussianProcessRegressor(kernel=kernel, alpha=1e-2, normalize_y=True)
    
    X_train = []
    y_train = []
    all_results = []
    
    optimizable_keys = list(PARAM_SPACE.keys())

    # 先隨機抽取 5 組作為 AI 的「初始先驗知識」
    print("🧠 階段一：建立初始先驗知識 (隨機探索 5 組)...")
    for _ in range(5):
        # 🌟 修復 2：將 BASE_PARAMS 改為 PARAMS
        p = deepcopy(PARAMS)
        for k, v in PARAM_SPACE.items(): p[k] = random.choice(v)
        
        metrics = evaluate_params(p, train_dfs, targets)
        if metrics["EV"] != -999.0:
            features = [p[k] for k in optimizable_keys]
            X_train.append(features)
            y_train.append(metrics["EV"]) 
            all_results.append({"Params": p, **metrics})

    # --- 3. 貝氏推論循環 (探索與開發) ---
    print("\n🧠 階段二：啟動貝氏推論 (集中火力尋找黃金參數區間)...")
    for i in range(n_iter):
        if len(X_train) > 0:
            model.fit(X_train, y_train)
            
        # 產生 100 組虛擬候選人
        candidates = []
        for _ in range(100):
            # 🌟 修復 3：這裡的 BASE_PARAMS 也一併改為 PARAMS
            p = deepcopy(PARAMS)
            for k, v in PARAM_SPACE.items(): p[k] = random.choice(v)
            candidates.append(p)
            
        X_candidates = [[c[k] for k in optimizable_keys] for c in candidates]
        preds, stds = model.predict(X_candidates, return_std=True)
        ucb = preds + 1.96 * stds
        best_idx = np.argmax(ucb)
        best_candidate = candidates[best_idx]
        
        print(f"🔄 [推論進度 {i+1}/{n_iter}] AI 鎖定高潛力參數組合進行實測...", end="\r")
        
        metrics = evaluate_params(best_candidate, train_dfs, targets)
        
        if metrics["EV"] != -999.0:
            features = [best_candidate[k] for k in optimizable_keys]
            X_train.append(features)
            y_train.append(metrics["EV"])
            all_results.append({"Params": best_candidate, **metrics})

    # --- 4. 帕雷托前緣決策 ---
    print("\n\n⚖️ 階段三：進入帕雷托前緣篩選 (排除被輾壓的劣質參數)...")
    pareto_front = get_pareto_frontier(all_results)
    
    if not pareto_front:
        print("⚠️ 找不到有效的黃金參數。")
        return None
        
    # 從帕雷托前緣中，挑選 EV 最高的作為本期冠軍
    pareto_front.sort(key=lambda x: x["EV"], reverse=True)
    champion = pareto_front[0]
    
    print("\n🔬 階段四：盲測大考 (對未知的 30% 行情進行壓力測試)...")
    test_metrics = evaluate_params(champion["Params"], test_dfs, targets)
    
    print("\n" + "═"*50)
    print("🏆 【貝氏演算法 x 帕雷托前緣】終極分析報告")
    print("═"*50)
    print(f"🔹 總測試組數: {len(all_results)} 組")
    print(f"🔹 帕雷托黃金解: {len(pareto_front)} 組 (彼此不分軒輊)")
    print("-" * 50)
    print("🥇 [訓練集 70% - 學習成果]")
    print(f"   期望值: {champion['EV']:.3f}% | 勝率: {champion['WinRate']:.3f}% | 報酬率: {champion['TotalReturn']:.3f}%")
    print("🎯 [測試集 30% - 盲測真實表現]")
    print(f"   期望值: {test_metrics['EV']:.3f}% | 勝率: {test_metrics['WinRate']:.3f}% | 報酬率: {test_metrics['TotalReturn']:.3f}%")
    print("═"*50)
    
    return {
        "Params": champion["Params"], 
        "Train_EV": champion["EV"], 
        "Test_EV": test_metrics["EV"], 
        "WinRate": test_metrics["WinRate"], 
        "TotalReturn": test_metrics["TotalReturn"]
    }
if __name__ == "__main__":
    run_bayesian_optimization(n_iter=30)