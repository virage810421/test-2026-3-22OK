import pandas as pd
import yfinance as yf
from screening import inspect_stock, add_chip_data
from config import PARAMS
import os

def generate_ml_dataset(tickers):
    print("🏭 [兵工廠] 啟動 AI 訓練資料生成器...")
    ml_dataset = []
    
    # 🌟 核心修復 1：加入 AI 旁路開關！防止萃取資料時大腦狂轉導致當機！
    test_params = PARAMS.copy()
    test_params['IS_OPTIMIZING'] = True 
    test_params['TRIGGER_SCORE'] = 2 

    for ticker in tickers:
        print(f"📡 正在萃取 {ticker} 的歷史特徵與勝負標籤...")
        try:
            data = yf.download(ticker, period="3y", progress=False)
            if data.empty: continue
            
            # 相容不同版本的 yfinance
            df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()
            df = add_chip_data(df, ticker)
            
            result = inspect_stock(ticker, preloaded_df=df, p=test_params)
            
            if not result or "計算後資料" not in result: continue
            
            computed_df = result['計算後資料']
            
            # 遍歷歷史，保留最後 5 天作為「偷看未來」的視窗
            for i in range(len(computed_df) - 5): 
                row = computed_df.iloc[i]
                setup_tag = str(row.get('Golden_Type', '無'))
                buy_score = int(row.get('Buy_Score', 0))
                
                # 🌟 核心修復 2：只萃取「做多」的訊號，防止空頭訊號污染 AI 學習方向！
                is_long_signal = ("LONG" in setup_tag) or (buy_score >= 2 and "SHORT" not in setup_tag)
                
                if is_long_signal:
                    
                    # 1. 萃取特徵 (Features X)
                    features = {
                        "Ticker": ticker,
                        "Date": computed_df.index[i],
                        "Regime": row.get('Regime', '未知'),
                        "Setup": setup_tag,
                        "RSI": row.get('RSI', 50),
                        "MACD_Hist": row.get('MACD_Hist', 0),
                        "BB_Width": (row.get('BB_Upper', 0) - row.get('BB_Lower', 0)) / row.get('MA20', 1) if row.get('MA20', 0) > 0 else 0,
                        "Volume_Ratio": row.get('Volume', 0) / (row.get('Vol_MA20', 0) + 1),
                        "ADX": row.get('ADX14', 0),
                        "Foreign_Net": row.get('Foreign_Net', 0),
                        "Trust_Net": row.get('Trust_Net', 0)
                    }
                    
                    # 2. 偷看未來定義勝負 (Labels Y)
                    entry_price = computed_df.iloc[i+1]['Open'] # 隔天開盤進場
                    future_window = computed_df.iloc[i+1 : i+6] # 觀察未來 5 天
                    
                    if future_window.empty or pd.isna(entry_price) or entry_price <= 0: continue
                    
                    max_high = future_window['High'].max()
                    
                    # 判斷是否獲利超過停損點
                    win_condition = (max_high - entry_price) / entry_price > PARAMS.get('SL_MIN_PCT', 0.03)
                    features['Label_Y'] = 1 if win_condition else 0
                    
                    ml_dataset.append(features)
                    
        except Exception as e:
            print(f"⚠️ {ticker} 萃取失敗: {e}")

    final_df = pd.DataFrame(ml_dataset)
    if not final_df.empty:
        os.makedirs("data", exist_ok=True) 
        final_df.to_csv("data/ml_training_data.csv", index=False, encoding='utf-8-sig')
        print(f"\n✅ [兵工廠] 成功萃取 {len(final_df)} 筆戰鬥紀錄，已存為 ml_training_data.csv！")
    else:
        print("\n⚠️ 萃取失敗，沒有產生任何有效數據。")

if __name__ == "__main__":
    training_pool = ["2330.TW", "2317.TW", "2454.TW", "2603.TW", "2881.TW", "3231.TW", "1519.TW", "2002.TW"]
    generate_ml_dataset(training_pool)