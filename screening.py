import yfinance as yf
import pandas as pd
import numpy as np
from advanced_chart import draw_chart

# 1. 核心檢測模組封裝 (靜默運算齒輪)

def inspect_stock(ticker, preloaded_df=None):
    """
    這是一台靜默掃描機。它不畫圖，只負責快速計算指標、打分數，然後吐出檢測報告。
    """
    try:
        # ⚡️ 批次下載切換邏輯
        if preloaded_df is not None:
            df = preloaded_df.copy()
        else:
            data = yf.download(ticker, period="2y", progress=False) 
            if data.empty: return None
            df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()
            
        if df.empty: return None
        
        # 1. 基礎指標計算 (RSI)
        delta = df['Close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['RSI'] = 100 - (100 / (1 + rs))

        # 2. 基礎指標計算 (MACD，海選原本沒有，現在補上作為計分條件)
        df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
        df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()
        df['DIF'] = df['EMA12'] - df['EMA26']
        df['MACD_Signal'] = df['DIF'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = (df['DIF'] - df['MACD_Signal']) * 2

        # 3. 基礎指標計算 (BBands 與 成交量均線)
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['MA60'] = df['Close'].rolling(window=60).mean()
        df['BB_std'] = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = df['MA20'] + (df['BB_std'] * 2)
        df['BB_Lower'] = df['MA20'] - (df['BB_std'] * 2)
        df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()

        # 清理運算初期的空值
        df.dropna(inplace=True)
       # ==========================================
        # D. ⚙️ 計分型邏輯閘 (滿分 5 分，得 3 分觸發 + 大趨勢保護傘)
        # ==========================================
        # 🛡️ 總電源：大趨勢保護傘
        trend_protect_buy = (df['Close'] > df['MA60']) & (df['MA60'] > df['MA60'].shift(1))
        trend_protect_sell = (df['Close'] < df['MA60']) & (df['MA60'] < df['MA60'].shift(1))

        # --- 【買方邏輯 (滿分 5 分)】 ---
        buy_c1 = df['Low'] <= df['BB_Lower']
        buy_c2 = df['RSI'] < 35
        buy_c3 = df['Volume'] > (df['Vol_MA20'] * 1.1)
        buy_c4 = (df['MACD_Hist'] > df['MACD_Hist'].shift(1)) & (df['DIF'] < 0)
        # ⚡️ 新增：即時底背離感測 (第 5 分)
        buy_c5 = (df['Low'] < df['Low'].shift(10)) & ((df['RSI'] > df['RSI'].shift(10)) | (df['DIF'] > df['DIF'].shift(10)))
        
        df['Buy_Score'] = buy_c1.astype(int) + buy_c2.astype(int) + buy_c3.astype(int) + buy_c4.astype(int) + buy_c5.astype(int)

        # --- 【賣方邏輯 (滿分 5 分)】 ---
        sell_c1 = df['High'] >= df['BB_Upper']
        sell_c2 = df['RSI'] > 65
        sell_c3 = df['Volume'] > (df['Vol_MA20'] * 1.1)
        sell_c4 = (df['MACD_Hist'] < df['MACD_Hist'].shift(1)) & (df['DIF'] > 0)
        # ⚡️ 新增：即時頂背離感測 (第 5 分)
        sell_c5 = (df['High'] > df['High'].shift(10)) & ((df['RSI'] < df['RSI'].shift(10)) | (df['DIF'] < df['DIF'].shift(10)))
        
        df['Sell_Score'] = sell_c1.astype(int) + sell_c2.astype(int) + sell_c3.astype(int) + sell_c4.astype(int) + sell_c5.astype(int)

        # 終極發射台 (綁定保護傘，維持 >= 3 分觸發)
        df['Buy_Signal'] = np.where((df['Buy_Score'] >= 3) & trend_protect_buy, df['Low'] * 0.98, np.nan)
        df['Sell_Signal'] = np.where((df['Sell_Score'] >= 3) & trend_protect_sell, df['High'] * 1.02, np.nan)
      
        # 4. 啟動回測引擎 (計算這套參數在該股票的良率)
        position = 0
        entry_price = 0
        trades = []
        
        for index, row in df.iterrows():
            if position == 0 and row['Buy_Signal']:
                position = 1
                entry_price = row['Close']
            elif position == 1 and row['Sell_Signal']:
                profit_pct = (row['Close'] - entry_price) / entry_price * 100
                trades.append(profit_pct)
                position = 0

        # 結算成績
        total_trades = len(trades)
        if total_trades > 0:
            win_rate = len([p for p in trades if p > 0]) / total_trades * 100
            total_profit = sum(trades)
        else:
            win_rate = 0.0
            total_profit = 0.0

       # ==========================================
        # 5. 提取「最後一天」狀態 (⚡️ 已通電：加上保護傘檢查)
        # ==========================================
        latest_row = df.iloc[-1]
        current_price = latest_row['Close']
        buy_score = int(latest_row['Buy_Score'])
        sell_score = int(latest_row['Sell_Score'])
        
        # 讀取最後一天的保護傘開關
        is_buy_protected = trend_protect_buy.iloc[-1]
        is_sell_protected = trend_protect_sell.iloc[-1]
        
        # 🎯 面板更新為滿分 5 分，3 分即可觸發強訊號
        if buy_score >= 3 and is_buy_protected:
            status = f"🔴 強買訊 ({buy_score}/5)"
        elif buy_score == 2 and is_buy_protected:
            status = f"🟡 弱買訊 ({buy_score}/5)"
        elif sell_score >= 3 and is_sell_protected:
            status = f"🟢 強賣訊 ({sell_score}/5)"
        elif sell_score == 2 and is_sell_protected:
            status = f"🟡 弱賣訊 ({sell_score}/5)"
        else:
            max_score = max(buy_score, sell_score)
            status = f"⚪ 觀望中 ({max_score}/5)"
        
        # 輸出檢測報告
        return {
            "股票代號": ticker,
            "最新收盤價": round(current_price, 2),
            "今日系統燈號": status,
            "歷史交易次數": total_trades,
            "系統勝率(%)": round(win_rate, 1),
            "累計報酬率(%)": round(total_profit, 2)
        }

    except Exception as e:
        print(f"檢測 {ticker} 時發生錯誤: {e}")
        return None

# ==========================================
# 🚀 批次海選發動機 (網路優化版)
# ==========================================
if __name__ == "__main__":

    test_targets = ["2881.TW", "2882.TW", "2884.TW", "2886.TW", "2891.TW"]
    
    print(f"\n啟動批次分析模式，正在一次性下載 {len(test_targets)} 檔股票資料，請稍候...")
    batch_data = yf.download(test_targets, period="2y", progress=True)
    print("\n✅ 資料下載完成！啟動自動化海選雷達，正在靜默掃描股票清單...\n")
    
    report_cards = []

    for ticker in test_targets:
        if len(test_targets) > 1:
            ticker_df = batch_data.xs(ticker, axis=1, level=1).copy()
        else:
            ticker_df = batch_data.copy()
            
        ticker_df.dropna(how='all', inplace=True)
        
        # ⚡️ 步驟 1：先讓「靜默掃描機 (inspect_stock)」檢查有沒有訊號
        result = inspect_stock(ticker, preloaded_df=ticker_df)
        
        if result:
            report_cards.append(result)
            
            # ⚡️ 步驟 2：繼電器開關！檢查燈號，如果不是「觀望中」，才通電呼叫畫圖機台
            if "觀望中" not in result["今日系統燈號"]:
                print(f"⚠️ 系統警報：偵測到 {ticker} 產生【{result['今日系統燈號']}】！")
                print(f"自動切換至 {ticker} 精密儀表板進行深度檢驗...")
                draw_chart(ticker, preloaded_df=ticker_df)

    # ⚡️ 步驟 3：所有股票掃描完畢，印出最終總表
    if report_cards:
        final_report = pd.DataFrame(report_cards)
        pd.set_option('display.unicode.east_asian_width', True) 
        print("\n===================== 今日海選總表 =====================")
        print(final_report.to_string(index=False))
        print("========================================================")
    else:
        print("掃描失敗，無資料輸出。")