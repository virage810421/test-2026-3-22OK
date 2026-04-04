import pandas as pd
import yfinance as yf
import os
import numpy as np
# 🌟 匯入系統核心晶片
from screening import inspect_stock, add_chip_data, extract_ai_features, smart_download
from config import PARAMS


def get_dynamic_watchlist():
    """從 SQL 資料庫動態撈取目前正在監控的股票名單 (歷史課本專用)"""
    print("📡 啟動動態索敵雷達：正在從 SQL 資料庫獲取監控名單...")
    try:
        DB_CONN_STR = (
            r'DRIVER={ODBC Driver 17 for SQL Server};'
            r'SERVER=localhost;'  
            r'DATABASE=股票online;'
            r'Trusted_Connection=yes;'
        )
        import pyodbc
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            
            # ⚠️ 長官請注意：請將這裡的 [您的籌碼資料表名] 換成您真實的 Table 名稱！
            cursor.execute("SELECT DISTINCT [Ticker SYMBOL] FROM daily_chip_data") 
            
            rows = cursor.fetchall()
            dynamic_list = [row[0] for row in rows if row[0]]
            
            if dynamic_list:
                print(f"✅ 成功鎖定 {len(dynamic_list)} 檔目標，準備印製歷史課本！")
                return dynamic_list
            else:
                raise ValueError("資料庫中找不到任何股票代碼！請檢查資料表是否為空。")
                
    except Exception as e:
        print(f"🛑 致命錯誤：無法連線 SQL 或獲取動態名單！詳細原因: {e}")
        raise


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
            # 🌟 終極淨化：直接呼叫智慧快取，它會回傳整理好的 df，不需要再自己處理 data！
            df = smart_download(ticker, period="3y")
            if df.empty: 
                continue
            
            # 直接把整理好的 df 拿去貼籌碼
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
                
                # ==========================================
                # 🎯 核心修復 4：補上「真實報酬率」，讓兵工廠能算期望值！
                # ==========================================
                future_close = future_window['Close'].iloc[-1] # 取第 5 天的收盤價
                if regime == '趨勢空頭' or "SHORT" in setup_tag:
                    features['Target_Return'] = (entry_price - future_close) / entry_price
                else:
                    features['Target_Return'] = (future_close - entry_price) / entry_price

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
    import sys  # 🌟 導入系統控制晶片
    
    # ==========================================
    # 🌟 啟動動態索敵 (向 SQL 要名單)
    # ==========================================
    try:
        watch_list = get_dynamic_watchlist()
    except Exception as e:
        print("🛑 [系統中斷] 無法獲取股票名單，歷史課本印製任務強制取消！請檢查 SQL 連線。")
        sys.exit(1) # ⚠️ 正確寫法：使用 sys.exit(1) 強制結束程式，不能用 return
        
    # 🌟 防呆：如果資料庫抓到了名單，就把它餵給兵工廠！
    if watch_list:
        generate_ml_dataset(watch_list)
    else:
        print("⚠️ SQL 雖然連線成功，但抓到的名單為空，請確認資料表內有股票代碼。")
    