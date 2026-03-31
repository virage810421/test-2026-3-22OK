from optimizer import run_walk_forward_optimization
from param_storage import save_sector_params
from sector_classifier import get_stock_sector
from config import WATCH_LIST

def start_automated_training():
    print("🚀 啟動【全自動產業感知與優化流水線】...")
    
    # 1. 自動分門別類 (把大清單拆成不同產業的小清單)
    sector_groups = {"TECH": [], "SHIPPING": [], "FINANCE": [], "OTHERS": []}
    for ticker in WATCH_LIST:
        cat = get_stock_sector(ticker)
        sector_groups[cat].append(ticker)

    # 2. 針對各個有股票的產業池進行獨立訓練
    for sector_name, tickers in sector_groups.items():
        if not tickers: continue  # 如果該產業沒半支股票，就跳過
        
        print("\n" + "="*60)
        print(f"🏗️ 正在針對【{sector_name}】板塊進行深度優化訓練...")
        print(f"📚 自動分類標的清單: {tickers}")
        print("="*60)
        
        # 呼叫大腦，傳入自動分好類的清單
        result = run_walk_forward_optimization(iterations=100, split_ratio=0.7, ticker_list=tickers)
        
        if result:
            train_ev = result["Train_EV"]
            test_ev = result["Test_EV"]
            best_params = result["Params"]

            # 🛡️ 斷崖偵測與勝率檢查
            if train_ev > 0 and (train_ev - test_ev) > (train_ev * 0.7):
                print(f"🚨 【過度擬合斷崖】訓練 EV: {train_ev:.3f}%, 盲測 EV: {test_ev:.3f}% ➔ 直接棄用！")
                continue
                
            if test_ev <= 0:
                print(f"🚨 【盲測失敗】實戰期望值為負 ({test_ev:.3f}%) ➔ 放棄存檔！")
                continue

            print(f"✅ 【驗證通過】參數穩定 (盲測 EV: {test_ev:.3f}%)，正在更新產業軍火庫...")
            save_sector_params(sector_name, best_params)

    print("\n🎉 全產業自動訓練結束！實戰大腦已獲取最新武器參數。")

if __name__ == "__main__":
    start_automated_training()