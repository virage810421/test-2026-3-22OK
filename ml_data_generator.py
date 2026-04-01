import pandas as pd
import yfinance as yf
from screening import inspect_stock, add_chip_data
from config import PARAMS

def generate_ml_dataset(tickers):
    print("🏭 [兵工廠] 啟動 AI 訓練資料生成器...")
    ml_dataset = []
    
    # 放寬分數限制，讓 AI 看到更多成功與失敗的案例來學習
    test_params = PARAMS.copy()
    test_params['TRIGGER_SCORE'] = 2 
    test_params['USE_SNIPER_MODE'] = False 

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
                
                # 當天有陣型，或是分數 >= 2，就抓出來當訓練樣本
                if row.get('Golden_Type', '無') != "無" or row.get('Buy_Score', 0) >= 2:
                    
                    # 🌟 1. 萃取特徵 (Features X) - 這裡包含所有原始指標
                    features = {
                        "Ticker": ticker,
                        "Date": computed_df.index[i],
                        "Regime": row.get('Regime', '未知'),
                        "Setup": row.get('Golden_Type', '無'),
                        "RSI": row.get('RSI', 50),
                        "MACD_Hist": row.get('MACD_Hist', 0),
                        "BB_Width": (row['BB_Upper'] - row['BB_Lower']) / row['MA20'] if row['MA20'] > 0 else 0,
                        "Volume_Ratio": row['Volume'] / (row['Vol_MA20'] + 1),
                        "ADX": row.get('ADX14', 0),
                        "Foreign_Net": row.get('Foreign_Net', 0),
                        "Trust_Net": row.get('Trust_Net', 0)
                    }
                    
                    # 🌟 2. 偷看未來定義勝負 (Labels Y)
                    entry_price = computed_df.iloc[i+1]['Open'] # 隔天開盤進場
                    future_window = computed_df.iloc[i+1 : i+6] # 觀察未來 5 天
                    
                    if future_window.empty or entry_price <= 0: continue
                    
                    max_high = future_window['High'].max()
                    
                    # 定義：如果未來 5 天內，最高價漲幅 > 停損跌幅 (例如 3%)，視為成功 (1)
                    win_condition = (max_high - entry_price) / entry_price > PARAMS.get('SL_MIN_PCT', 0.03)
                    features['Label_Y'] = 1 if win_condition else 0
                    
                    ml_dataset.append(features)
                    
        except Exception as e:
            print(f"⚠️ {ticker} 萃取失敗: {e}")

    final_df = pd.DataFrame(ml_dataset)
    if not final_df.empty:
        import os
        os.makedirs("data", exist_ok=True) # 確保資料夾存在
        # 👇 加上 data/ 路徑
        final_df.to_csv("data/ml_training_data.csv", index=False, encoding='utf-8-sig')
        print(f"✅ [兵工廠] 成功萃取 {len(final_df)} 筆戰鬥紀錄，已存為 ml_training_data.csv！")
    else:
        print("⚠️ 萃取失敗，沒有產生任何有效數據。")

if __name__ == "__main__":
    # 放入您想用來訓練 AI 的股票池 (建議放 20~50 檔涵蓋各產業的股票)
    training_pool = ["2330.TW", "2317.TW", "2454.TW", "2603.TW", "2881.TW", "3231.TW", "1519.TW", "2002.TW"]
    generate_ml_dataset(training_pool)