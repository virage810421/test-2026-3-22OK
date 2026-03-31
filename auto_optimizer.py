# auto_optimizer.py
from optimizer import run_walk_forward_optimization
from param_storage import save_sector_params

# 🌌 產業自動優化排程表
SECTOR_PIPELINE = {
    "TECH": ["2330.TW", "2454.TW", "2317.TW", "2382.TW", "3231.TW"],
    "SHIPPING": ["2603.TW", "2609.TW", "2615.TW"],
    "FINANCE": ["2881.TW", "2882.TW", "2891.TW", "2886.TW"]
}

def start_automated_training():
    print("🚀 啟動【全自動產業優化流水線】...")
    
    for sector_name, tickers in SECTOR_PIPELINE.items():
        print("\n" + "="*60)
        print(f"🏗️ 正在針對【{sector_name}】板塊進行深度優化訓練...")
        print("="*60)
        
        # 自動呼叫大腦，傳入產業專屬清單
        result = run_walk_forward_optimization(iterations=100, split_ratio=0.7, ticker_list=tickers)
        
        if result:
            train_ev = result["Train_EV"]
            test_ev = result["Test_EV"]
            best_params = result["Params"]

            # 🛡️ [防護 3] 訓練/盲測 斷崖偵測 (過度擬合防護)
            if train_ev > 0 and (train_ev - test_ev) > (train_ev * 0.7):
                print(f"🚨 【過度擬合斷崖】訓練 EV: {train_ev:.3f}%, 盲測 EV: {test_ev:.3f}% ➔ 差距過大，直接棄用！")
                continue
                
            if test_ev <= 0:
                print(f"🚨 【盲測失敗】實戰期望值為負 ({test_ev:.3f}%) ➔ 無法獲利，放棄存檔！")
                continue

            print(f"✅ 【驗證通過】參數穩定 (盲測 EV: {test_ev:.3f}%)，正在更新產業軍火庫...")
            save_sector_params(sector_name, best_params)

    print("\n🎉 全產業自動訓練結束！實戰大腦已獲取最新武器參數。")

if __name__ == "__main__":
    start_automated_training()