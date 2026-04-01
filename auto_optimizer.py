import time
from sector_classifier import get_stock_sector
from param_storage import save_sector_params
from config import WATCH_LIST


# ==========================================
# 🧠 總司令的 AI 大腦切換開關
# ==========================================
# True = 使用【貝氏大腦】(快速、尋找極限期望值)
# False = 使用【滾動盲測大腦】(嚴格防護、抗滑價、抗回撤)
USE_ADVANCED_BAYES = False

if USE_ADVANCED_BAYES:
    from advanced_optimizer import run_bayesian_optimization as run_engine
    print("🧠 [總部廣播] 工廠已掛載：【貝氏推論大腦 (advanced_optimizer.py)】")
else:
    from optimizer import run_walk_forward_optimization as run_engine
    print("🧠 [總部廣播] 工廠已掛載：【滾動盲測大腦 (opti1mizer.py)】")

def start_automated_training():
    """
    全自動產業優化流水線
    """
    # 1. 產業分類 (這部分維持您原本的優良邏輯)
    sector_groups = {}
    for ticker in WATCH_LIST:
        sector = get_stock_sector(ticker)
        if sector not in sector_groups:
            sector_groups[sector] = []
        sector_groups[sector].append(ticker)
    
    print(f"📊 產業分類完成，共 {len(sector_groups)} 個組別，準備開始訓練...")

    # 2. 開始循環訓練
    for sector, tickers in sector_groups.items():
        print(f"\n" + "="*40)
        print(f"📂 正在優化產業：{sector} (標的：{tickers})")
        print("="*40)
        
        try:
            # 🌟 根據開關自動呼叫對應的引擎
            # 這裡我們統一使用 run_engine 這個別名來呼叫
            if USE_ADVANCED_BAYES:
                # 貝氏引擎通常 30 次就很精準
                result = run_engine(n_iter=30, ticker_list=tickers) 
            else:
                # 舊引擎通常需要較多次數來靠運氣抓好參數
                result = run_engine(iterations=100, ticker_list=tickers)
            
            if result and result.get("Params"):
                # 3. 存入軍火庫 (JSON)
                save_sector_params(sector, result["Params"])
                print(f"✅ {sector} 優化成功！已儲存至 automated_sector_params.json")
                # 🌟 在產線上直接顯示三大核心數據，並精確到小數點後 3 位
                ev_val = result.get('Train_EV', 0)
                win_val = result.get('WinRate', 0)
                ret_val = result.get('TotalReturn', 0)
                print(f"📈 訓練成果 ➔ 期望值: {ev_val:.3f}% | 系統勝率: {win_val:.3f}% | 累計報酬率: {ret_val:.3f}%")
            else:
                print(f"⚠️ {sector} 未能產出有效參數，跳過儲存。")
                
        except Exception as e:
            print(f"❌ {sector} 訓練過程發生崩潰: {e}")
        
        time.sleep(2) # 讓電腦喘口氣

if __name__ == "__main__":
    start_automated_training()