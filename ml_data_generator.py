import pandas as pd
import yfinance as yf
import os
import numpy as np

# 🌟 匯入系統核心晶片
from screening import inspect_stock, add_chip_data, extract_ai_features
from config import PARAMS

def generate_ml_dataset(tickers):
    print("🏭 [兵工廠] 啟動 AI 雙向訓練資料生成器...")
    
    # 🌟 終極縫合：開工前強制銷毀舊課本！確保 AI 學到的絕對是今天的最新資料！
    if os.path.exists("data/ml_training_data.csv"):
        os.remove("data/ml_training_data.csv")
        print("🗑️ 已銷毀昨日舊有訓練資料，確保數據絕對純淨。")
        
    ml_dataset = []
    
    # 🌟 防止萃取資料時大腦狂轉導致當機
    test_params = PARAMS.copy()
    test_params['IS_OPTIMIZING'] = True 
    test_params['TRIGGER_SCORE'] = 2 

    for ticker in tickers:
        print(f"📡 正在萃取 {ticker} 的歷史特徵與勝負標籤...")
        try:
            data = yf.download(ticker, period="3y", progress=False)
            if data.empty: continue
            
            df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()
            df = add_chip_data(df, ticker)
            
            result = inspect_stock(ticker, preloaded_df=df, p=test_params)
            if not result or "計算後資料" not in result: continue
            
            computed_df = result['計算後資料']
            
            # 遍歷歷史，保留最後 5 天作為「偷看未來」的視窗
            for i in range(len(computed_df) - 5):
                row = computed_df.iloc[i]
                setup_tag = str(row.get('Golden_Type', '無'))
                regime = str(row.get('Regime', '區間盤整'))
                
                # ==========================================
                # 🎯 核心修復 1：多空雙向全收！只要有陣型就抓進來訓練
                # ==========================================
                if setup_tag == "無":
                    continue
                    
                # ==========================================
                # 🎯 核心修復 2：正確呼叫特徵晶片，保留您的 16 把終極武器！
                # ==========================================
                features = extract_ai_features(row)
                
                # 將 Meta 資訊補進 features 字典中
                features['Ticker'] = ticker
                features['Date'] = computed_df.index[i]
                features['Regime'] = regime
                features['Setup'] = setup_tag
                
                # 取得未來五天的資料
                entry_price = computed_df.iloc[i+1]['Open']
                future_window = computed_df.iloc[i+1 : i+6]
                
                if future_window.empty or pd.isna(entry_price) or entry_price <= 0: continue
                
                # 替換 ml_data_generator.py 中的雙向動態計分系統
                # ==========================================
                # 🎯 核心修復 3：雙向動態計分系統 (包含嚴格停損檢驗！)
                # ==========================================
                sl_pct = PARAMS.get('SL_MIN_PCT', 0.03)
                
                if regime == '趨勢空頭' or "SHORT" in setup_tag:
                    # 🔴 空軍：未來 5 天最高價如果先觸發停損，直接判輸
                    max_high = future_window['High'].max()
                    if (max_high - entry_price) / entry_price > sl_pct:
                        win_condition = False
                    else:
                        min_low = future_window['Low'].min()
                        win_condition = (entry_price - min_low) / entry_price > sl_pct
                else:
                    # 🟢 多軍：未來 5 天最低價如果先觸發停損，直接判輸
                    min_low = future_window['Low'].min()
                    if (entry_price - min_low) / entry_price > sl_pct:
                        win_condition = False
                    else:
                        max_high = future_window['High'].max()
                        win_condition = (max_high - entry_price) / entry_price > sl_pct
                # 紀錄解答
                features['Label_Y'] = 1 if win_condition else 0
                ml_dataset.append(features)
                                   
        except Exception as e:
            print(f"⚠️ {ticker} 萃取失敗: {e}")

    final_df = pd.DataFrame(ml_dataset)
    if not final_df.empty:
        os.makedirs("data", exist_ok=True) 
        final_df.to_csv("data/ml_training_data.csv", index=False, encoding='utf-8-sig')
        print(f"\n✅ [兵工廠] 成功萃取 {len(final_df)} 筆雙向戰鬥紀錄，已存為 ml_training_data.csv！")
    else:
        print("\n⚠️ 萃取失敗，沒有產生任何有效數據。")

if __name__ == "__main__":
    training_pool = ["2330.TW", "2317.TW", "2454.TW", "2603.TW", "2881.TW", "3231.TW", "1519.TW", "2002.TW"]
    generate_ml_dataset(training_pool)