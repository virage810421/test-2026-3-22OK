import yfinance as yf
import pandas as pd
import numpy as np
from advanced_chart import draw_chart

# ==========================================
# 1. 核心檢測模組封裝 (打包運算齒輪)
# ==========================================
def inspect_stock(ticker):
    """
    這是一個獨立的檢測模組。
    輸入：股票代號 (ticker)
    輸出：包含該檔股票「最新狀態」與「歷史回測良率」的字典資料
    """
    try:
        # 1. 獲取資料 (縮短區間以提升海選掃描速度，改為抓取近一年半)
        data = yf.download(ticker, start="2023-01-01", progress=False) 
        if data.empty:
            return None
            
        df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()

        # 2. 安裝指標感測器 (RSI, BBands)
        delta = df['Close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
        df['RSI'] = 100 - (100 / (1 + avg_gain / avg_loss))

        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['BB_std'] = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = df['MA20'] + (df['BB_std'] * 2)
        df['BB_Lower'] = df['MA20'] - (df['BB_std'] * 2)

        # 清除初期空值 (維持正確的運作順序)
        df.dropna(inplace=True)

        # 3. 邏輯閘設定 (買賣條件)
        buy_condition = (df['Low'] <= df['BB_Lower']) & (df['RSI'] < 35)
        sell_condition = (df['High'] >= df['BB_Upper']) & (df['RSI'] > 65)

        df['Buy_Signal'] = np.where(buy_condition, True, False)
        df['Sell_Signal'] = np.where(sell_condition, True, False)

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

        # 5. 提取「最後一天」的最新狀態，判斷今天是否觸發訊號
        latest_row = df.iloc[-1]
        current_price = latest_row['Close']
        
        if latest_row['Buy_Signal']:
            status = "🔴 觸發買進"  # 台股邏輯：紅漲
        elif latest_row['Sell_Signal']:
            status = "🟢 觸發賣出"  # 台股邏輯：綠跌
        else:
            status = "⚪ 觀望中"

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
# 2. 啟動自動化海選流水線
# ==========================================
if __name__ == "__main__":
    # 設定你要監控的股票清單 (例如：台積電、鴻海、聯發科、富邦金、長榮)
    watch_list = ["2330.TW", "2317.TW", "2454.TW", "2881.TW", "2603.TW"]
    
    print("啟動自動化海選雷達，正在掃描股票清單...\n")
    
    # 用來收集所有股票檢測結果的空箱子
    report_cards = []

    # 讓股票依序通過檢測模組
    for stock in watch_list:
        result = inspect_stock(stock)
        if result:
            report_cards.append(result)
            # 【測試修改】：強制指定只要掃描到台積電，就無視燈號直接彈出圖表！
            if stock == "2454.TW":  
                    print(f"🔧 執行強制通電測試：啟動 {stock} 精密儀表板...")
                    draw_chart(stock)
    # 將收集到的結果，轉換成整齊的 Pandas 報表並印出
    if report_cards:
        final_report = pd.DataFrame(report_cards)
        
        # 為了讓終端機顯示得更漂亮，稍微設定一下 Pandas 的對齊
        pd.set_option('display.unicode.east_asian_width', True) 
        
        print("===================== 今日海選總表 =====================")
        print(final_report.to_string(index=False))
        print("========================================================")
    else:
        print("掃描失敗，無資料輸出。")