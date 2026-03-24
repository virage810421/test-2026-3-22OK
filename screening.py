import yfinance as yf
import pandas as pd
import numpy as np
from advanced_chart import draw_chart
from FinMind.data import DataLoader

# ⚡️ 初始化 DataLoader 
dl = DataLoader()

# ==========================================
# 🔌 新增：籌碼資料外掛模組 (資料合併處理廠)
# ==========================================
def add_chip_data(df, ticker):
    """
    負責把 yfinance 的價格表，貼上 FinMind 的外資/投信買賣超資料
    """
    pure_ticker = ticker.split('.')[0]
    start_dt = (pd.Timestamp.today() - pd.Timedelta(days=120)).strftime("%Y-%m-%d")
    
    try:
        chip_df = dl.taiwan_stock_institutional_investors(stock_id=pure_ticker, start_date=start_dt)
        
        if chip_df.empty:
            df['Foreign_Net'] = 0
            df['Trust_Net'] = 0
            return df
            
        chip_df['Net'] = chip_df['buy'] - chip_df['sell']
        
        foreign = chip_df[chip_df['name'].str.contains('外資')].groupby('date')['Net'].sum()
        trust = chip_df[chip_df['name'].str.contains('投信')].groupby('date')['Net'].sum()
        
        # 確保有資料才轉換時間格式
        if not foreign.empty: foreign.index = pd.to_datetime(foreign.index)
        if not trust.empty: trust.index = pd.to_datetime(trust.index)

        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        
        df['Foreign_Net'] = foreign
        df['Trust_Net'] = trust
        # 🛡️ 破解時間差陷阱：如果今天的籌碼還是 NaN(還沒公佈)，就先借用昨天的資料 (limit=1 代表最多只借1天)
        df['Foreign_Net'] = df['Foreign_Net'].ffill(limit=1)
        df['Trust_Net'] = df['Trust_Net'].ffill(limit=1)
        
        # 剩下的真實空值 (例如很久以前剛上市沒資料的日子) 才補 0
        df['Foreign_Net'] = df['Foreign_Net'].fillna(0)
        df['Trust_Net'] = df['Trust_Net'].fillna(0)
        
    except Exception as e:
        print(f"⚠️ {ticker} 籌碼抓取失敗: {e}")
        df['Foreign_Net'] = 0
        df['Trust_Net'] = 0
        
    return df

# ==========================================
# 1. 核心檢測模組封裝 (升級 BBI & DMI + 有效交易次數過濾)
# ==========================================
def inspect_stock(ticker, preloaded_df=None):
    try:
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

        # 2. 基礎指標計算 (MACD)
        df['EMA12'] = df['Close'].ewm(span=12, adjust=False).mean()
        df['EMA26'] = df['Close'].ewm(span=26, adjust=False).mean()
        df['DIF'] = df['EMA12'] - df['EMA26']
        df['MACD_Signal'] = df['DIF'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = (df['DIF'] - df['MACD_Signal']) * 2

        # 3. 基礎指標計算 (BBands 與 成交量均線)
        df['MA20'] = df['Close'].rolling(window=20).mean()
        df['BB_std'] = df['Close'].rolling(window=20).std()
        df['BB_Upper'] = df['MA20'] + (df['BB_std'] * 2)
        df['BB_Lower'] = df['MA20'] - (df['BB_std'] * 2)
        df['Vol_MA20'] = df['Volume'].rolling(window=20).mean()

        # 4. 新增：BBI (多空指標) 與 乖離
        df['MA3'] = df['Close'].rolling(window=3).mean()
        df['MA6'] = df['Close'].rolling(window=6).mean()
        df['MA12'] = df['Close'].rolling(window=12).mean()
        df['MA24'] = df['Close'].rolling(window=24).mean()
        df['BBI'] = (df['MA3'] + df['MA6'] + df['MA12'] + df['MA24']) / 4
        df['BBI_BIAS'] = (df['Close'] - df['BBI']) / df['BBI'] * 100

        # 5. 新增：DMI (動向指標)
        high_diff = df['High'].diff()
        low_diff = df['Low'].diff()
        df['+DM'] = np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0)
        df['-DM'] = np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0)
        
        tr1 = df['High'] - df['Low']
        tr2 = abs(df['High'] - df['Close'].shift(1))
        tr3 = abs(df['Low'] - df['Close'].shift(1))
        df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        df['+DI14'] = 100 * (df['+DM'].rolling(14).sum() / df['TR'].rolling(14).sum())
        df['-DI14'] = 100 * (df['-DM'].rolling(14).sum() / df['TR'].rolling(14).sum())
        df['DX'] = 100 * abs(df['+DI14'] - df['-DI14']) / (df['+DI14'] + df['-DI14'])
        df['ADX14'] = df['DX'].rolling(14).mean()

        df.dropna(inplace=True)
        if df.empty: return None 
        
        # ==========================================
        # 🛡️ 基礎防護網
        # ==========================================
        latest_check = df.iloc[-1]
        
        if latest_check['Vol_MA20'] < 1000000:
            return None 
            
        if latest_check['Close'] < 10.0:
            return None 

        # ==========================================
        # D. ⚙️ 計分型邏輯閘 
        # ==========================================
        buy_trend = (df['Close'] > df['BBI']) & (df['BBI'] > df['BBI'].shift(1))
        sell_trend = (df['Close'] < df['BBI']) & (df['BBI'] < df['BBI'].shift(1))

        # --- 【買方邏輯】 ---
        buy_c1 = df['Low'] <= df['BB_Lower']
        buy_c2 = df['RSI'] < 35
        buy_c3 = (df['Volume'] > (df['Vol_MA20'] * 1.25)) & (df['Close'] > df['Open'])
        buy_c4 = (df['MACD_Hist'] > df['MACD_Hist'].shift(1)) & (df['DIF'] < 0)
        buy_c5 = (df['Low'] < df['Low'].shift(10)) & ((df['RSI'] > df['RSI'].shift(10)) | (df['DIF'] > df['DIF'].shift(10)))
        buy_c6 = (df['Close'] > df['BBI']) & (df['Close'].shift(1) <= df['BBI'].shift(1))
        buy_c7 = (df.get('Foreign_Net', 0) > 0) & (df.get('Trust_Net', 0) > 0)
        buy_c8 = (df['+DI14'] > df['-DI14']) & (df['ADX14'] >= 20) & (df['ADX14'] > df['ADX14'].shift(1))
        
        df['Buy_Score'] = buy_trend.astype(int) + buy_c1.astype(int) + buy_c2.astype(int) + buy_c3.astype(int) + buy_c4.astype(int) + buy_c5.astype(int) + buy_c6.astype(int) + buy_c7.astype(int) + buy_c8.astype(int)

        # --- 【賣方邏輯】 ---
        sell_c1 = df['High'] >= df['BB_Upper']
        sell_c2 = df['RSI'] > 65
        sell_c3 = (df['Volume'] > (df['Vol_MA20'] * 1.25)) & (df['Close'] < df['Open'])
        sell_c4 = (df['MACD_Hist'] < df['MACD_Hist'].shift(1)) & (df['DIF'] > 0)
        sell_c5 = (df['High'] > df['High'].shift(10)) & ((df['RSI'] < df['RSI'].shift(10)) | (df['DIF'] < df['DIF'].shift(10)))
        sell_c6 = (df['Close'] < df['BBI']) & (df['Close'].shift(1) >= df['BBI'].shift(1))
        sell_c7 = (df.get('Foreign_Net', 0) < 0) & (df.get('Trust_Net', 0) < 0)
        sell_c8 = (df['-DI14'] > df['+DI14']) & (df['ADX14'] >= 20) & (df['ADX14'] > df['ADX14'].shift(1))
        
        df['Sell_Score'] = sell_trend.astype(int) + sell_c1.astype(int) + sell_c2.astype(int) + sell_c3.astype(int) + sell_c4.astype(int) + sell_c5.astype(int) + sell_c6.astype(int) + sell_c7.astype(int) + sell_c8.astype(int)

        df['Buy_Signal'] = np.where(df['Buy_Score'] >= 4, df['Low'] * 0.98, np.nan)
        df['Sell_Signal'] = np.where(df['Sell_Score'] >= 4, df['High'] * 1.02, np.nan)
      
        # ==========================================
        # 4. 啟動回測引擎 
        # ==========================================
        position = 0
        entry_price = 0
        trades = []
        
        for index, row in df.iterrows():
            if position == 0 and not pd.isna(row['Buy_Signal']):
                position = 1
                entry_price = row['Close']
            elif position == 1 and not pd.isna(row['Sell_Signal']):
                profit_pct = (row['Close'] - entry_price) / entry_price * 100
                trades.append(profit_pct)
                position = 0

        total_trades = len(trades)
        if total_trades > 0:
            win_rate = len([p for p in trades if p > 0]) / total_trades * 100
            total_profit = sum(trades)
        else:
            win_rate = 0.0
            total_profit = 0.0

        # ==========================================
        # 5. 提取「最後一天」狀態 (🚀 精準追蹤有效交易助攻次數)
        # ==========================================
        latest_row = df.iloc[-1]
        current_price = latest_row['Close']
        buy_score = int(latest_row['Buy_Score'])
        sell_score = int(latest_row['Sell_Score'])
        
        # 建立真實發動訊號的過濾遮罩
        actual_buy_signals = df['Buy_Score'] >= 4
        actual_sell_signals = df['Sell_Score'] >= 4
        
        # 買方條件附帶有效助攻次數
        buy_details = []
        if buy_trend.iloc[-1]: buy_details.append(f"BBI多頭趨勢(歷{int((buy_trend & actual_buy_signals).sum())}次)")
        if buy_c1.iloc[-1]: buy_details.append(f"破下軌(歷{int((buy_c1 & actual_buy_signals).sum())}次)")
        if buy_c2.iloc[-1]: buy_details.append(f"RSI超賣(歷{int((buy_c2 & actual_buy_signals).sum())}次)")
        if buy_c3.iloc[-1]: buy_details.append(f"爆量(歷{int((buy_c3 & actual_buy_signals).sum())}次)")
        if buy_c4.iloc[-1]: buy_details.append(f"MACD轉強(歷{int((buy_c4 & actual_buy_signals).sum())}次)")
        if buy_c5.iloc[-1]: buy_details.append(f"底背離(歷{int((buy_c5 & actual_buy_signals).sum())}次)")
        if buy_c6.iloc[-1]: buy_details.append(f"🌟突破BBI(歷{int((buy_c6 & actual_buy_signals).sum())}次)") 
        if buy_c7.iloc[-1]: buy_details.append(f"🔥法人同買(歷{int((buy_c7 & actual_buy_signals).sum())}次)") 
        if buy_c8.iloc[-1]: buy_details.append(f"📈DMI趨勢成型(歷{int((buy_c8 & actual_buy_signals).sum())}次)")
        
        # 賣方條件附帶有效助攻次數
        sell_details = []
        if sell_trend.iloc[-1]: sell_details.append(f"BBI空頭趨勢(歷{int((sell_trend & actual_sell_signals).sum())}次)")
        if sell_c1.iloc[-1]: sell_details.append(f"頂上軌(歷{int((sell_c1 & actual_sell_signals).sum())}次)")
        if sell_c2.iloc[-1]: sell_details.append(f"RSI超買(歷{int((sell_c2 & actual_sell_signals).sum())}次)")
        if sell_c3.iloc[-1]: sell_details.append(f"爆量(歷{int((sell_c3 & actual_sell_signals).sum())}次)")
        if sell_c4.iloc[-1]: sell_details.append(f"MACD轉弱(歷{int((sell_c4 & actual_sell_signals).sum())}次)")
        if sell_c5.iloc[-1]: sell_details.append(f"頂背離(歷{int((sell_c5 & actual_sell_signals).sum())}次)")
        if sell_c6.iloc[-1]: sell_details.append(f"💀跌破BBI(歷{int((sell_c6 & actual_sell_signals).sum())}次)") 
        if sell_c7.iloc[-1]: sell_details.append(f"🧊法人同賣(歷{int((sell_c7 & actual_sell_signals).sum())}次)") 
        if sell_c8.iloc[-1]: sell_details.append(f"📉DMI空頭成型(歷{int((sell_c8 & actual_sell_signals).sum())}次)")
        
        trigger_str = "-"
        if buy_score >= 4:
            status = f"🔴 強買訊 ({buy_score}/9)"
            trigger_str = " + ".join(buy_details)
        elif sell_score >= 4:   
            status = f"🟢 強賣訊 ({sell_score}/9)"
            trigger_str = " + ".join(sell_details)
        elif buy_score == 3:    
            status = f"🟡 弱買訊 ({buy_score}/9)"
            trigger_str = " + ".join(buy_details)
        elif sell_score == 3:
            status = f"🟡 弱賣訊 ({sell_score}/9)"
            trigger_str = " + ".join(sell_details)
        else:
            max_score = max(buy_score, sell_score)
            status = f"⚪ 觀望中 ({max_score}/9)"
            if buy_score >= sell_score and buy_score > 0:
                trigger_str = "已亮燈: " + " + ".join(buy_details)
            elif sell_score > buy_score and sell_score > 0:
                trigger_str = "已亮燈: " + " + ".join(sell_details)
            else:
                trigger_str = "無"

        if latest_row['ADX14'] < 20:
            trigger_str += " (⚠️ 盤整中，訊號效力減弱)"

        strength_diff = buy_score - sell_score
        structure_status = "多頭佔優" if strength_diff > 2 else "空頭佔優" if strength_diff < -2 else "結構盤整"

        return {
            "Ticker SYMBOL": ticker,
            "最新收盤價": round(current_price, 2),
            "結構強度": f"{strength_diff:+d}",  # 顯示如 +4 或 -2
            "今日系統燈號": status,
            "結構診斷": structure_status,
            "觸發條件明細": trigger_str,
            "系統勝率(%)": f"{win_rate:.2f}",
            "累計報酬率(%)": f"{total_profit:.2f}"
            }

    except Exception as e:
        print(f"檢測 {ticker} 時發生錯誤: {e}")
        return None

# ==========================================
# 🚀 批次海選發動機 
# ==========================================
if __name__ == "__main__":

    test_targets = [
        "2330.TW", "2454.TW", "2303.TW", "2337.TW", 
        "2317.TW", "2382.TW", "3231.TW", "2356.TW", "2376.TW", 
        "2603.TW", "2609.TW", "2615.TW", 
        "2881.TW", "2882.TW", "2884.TW", "2886.TW", "2891.TW",
        "1503.TW", "1519.TW", "1513.TW"
    ]
    
    print(f"\n啟動批次分析模式，正在一次性下載 {len(test_targets)} 檔股票資料，請稍候...")
    batch_data = yf.download(test_targets, period="2y", progress=True)
    print("\n✅ 資料下載完成！啟動自動化海選雷達，正在靜默掃描股票清單...\n")
    
    report_cards = []

    for ticker in test_targets:
        if isinstance(batch_data.columns, pd.MultiIndex):
            ticker_df = batch_data.xs(ticker, axis=1, level=1).copy()
        else:
            ticker_df = batch_data.copy()
            
        ticker_df.dropna(how='all', inplace=True)
        ticker_df = add_chip_data(ticker_df, ticker)
        
        result = inspect_stock(ticker, preloaded_df=ticker_df)
        
        if result:
            report_cards.append(result)
            
            if "觀望中" not in result["今日系統燈號"]:
                print(f"⚠️ 系統警報：偵測到 {ticker} 產生【{result['今日系統燈號']}】！")
                print(f"自動切換至 {ticker} 精密儀表板進行深度檢驗...")
                draw_chart(ticker, preloaded_df=ticker_df)

    if report_cards:
        final_report = pd.DataFrame(report_cards)
        pd.set_option('display.unicode.east_asian_width', True) 
        print("\n===================== 今日海選總表 =====================")
        print(final_report.to_string(index=False))
        print("========================================================")
    else:
        print("掃描失敗，無資料輸出。")