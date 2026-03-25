import yfinance as yf
import pandas as pd
import numpy as np
from advanced_chart import draw_chart
from FinMind.data import DataLoader

# ==========================================
# ⚡️ 初始化 DataLoader (已綁定專屬 API Token，提升請求上限)
# ==========================================
API_TOKEN = "FinMind:eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMy0yNiAwMDo0ODo0NiIsInVzZXJfaWQiOiJob25kYSIsImVtYWlsIjoiaG9uZGEyMTMxMTMwQGdtYWlsLmNvbSIsImlwIjoiMjcuMjQwLjI1MC4xNTIifQ.CLZzVy6OK617rjvOZ7RG-Yc4pU-EBzPMqpL1CXUz6js"
dl = DataLoader(token=API_TOKEN)

# ⚡️ 初始化 DataLoader 
dl = DataLoader()
# ==========================================
# 🔌 籌碼資料外掛模組 (資料合併處理廠)
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
        # 🛡️ 破解時間差陷阱：如果今天的籌碼還是 NaN(還沒公佈)，就先借用昨天的資料
        df['Foreign_Net'] = df['Foreign_Net'].ffill(limit=1)
        df['Trust_Net'] = df['Trust_Net'].ffill(limit=1)
        
        # 剩下的真實空值才補 0
        df['Foreign_Net'] = df['Foreign_Net'].fillna(0)
        df['Trust_Net'] = df['Trust_Net'].fillna(0)
        
    except Exception as e:
        print(f"⚠️ {ticker} 籌碼抓取失敗: {e}")
        df['Foreign_Net'] = 0
        df['Trust_Net'] = 0
        
    return df


# ==========================================
# 📊 新增：基本面數據採集器 (FinMind 版)
# ==========================================
def add_fundamental_filter(ticker):
    """抓取營收與獲利能力，判斷基本面體質"""
    pure_ticker = ticker.split('.')[0]
    try:
        # 1. 抓取月營收 (判斷成長性)
        rev_df = dl.taiwan_stock_month_revenue(stock_id=pure_ticker)
        rev_yoy = rev_df.iloc[-1]['revenue_year_growth'] if not rev_df.empty else 0.0

        # 2. 抓取損益表 (判斷獲利能力)
        st_df = dl.taiwan_stock_financial_statement(stock_id=pure_ticker)
        if not st_df.empty:
            op_margin_row = st_df[st_df['type'] == 'OperatingProfitMargin']
            op_margin = op_margin_row.iloc[-1]['value'] if not op_margin_row.empty else 0.0
        else:
            op_margin = 0.0

        # 3. 定性評分邏輯
        f_score = 0
        if rev_yoy > 0: f_score += 1
        if rev_yoy > 20: f_score += 1 
        if op_margin > 0: f_score += 1 
        if op_margin < 0: f_score -= 2 # 本業虧損大扣分

        return {"營收年增率(%)": rev_yoy, "營業利益率(%)": op_margin, "基本面總分": f_score}
    except:
        return {"營收年增率(%)": 0.000, "營業利益率(%)": 0.000, "基本面總分": 0}

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
        # ==========================================
        # 🌊 新增：DZ RSI (動態區間 RSI) 計算
        # ==========================================
        # 計算 RSI 過去 14 天的移動平均與標準差
        df['RSI_MA'] = df['RSI'].rolling(window=14).mean()
        df['RSI_STD'] = df['RSI'].rolling(window=14).std()
        
        # 設定動態超買/超賣線 (使用 1.5 倍標準差)
        df['DZ_Upper'] = df['RSI_MA'] + (df['RSI_STD'] * 1.5)
        df['DZ_Lower'] = df['RSI_MA'] - (df['RSI_STD'] * 1.5)
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

        # --- 【買方邏輯優化版】(完整無斷尾) ---
        buy_c1 = df['Low'] <= df['BB_Lower']
        buy_c2 = df['RSI'] < df['DZ_Lower']
        # 👯 雙胞胎處理：c1 和 c2 只要有一個成立就拿 1 分，同時成立也只有 1 分 (合併為 c1_c2_score)
        buy_c1_c2_score = (buy_c1 | buy_c2).astype(int) 

        buy_c3 = (df['Volume'] > (df['Vol_MA20'] * 1.1)) & (df['Close'] > df['Open'])
        buy_c4 = (df['MACD_Hist'] > df['MACD_Hist'].shift(1)) & (df['DIF'] < 0)

        # 👻 幽靈 1 處理：區間底背離 (比較過去 10 天的低點，而不是單指 10 天前)
        past_10_low = df['Low'].shift(1).rolling(10).min()
        past_10_rsi_min = df['RSI'].shift(1).rolling(10).min()
        buy_c5 = (df['Low'] < past_10_low) & (df['RSI'] > past_10_rsi_min)

        buy_c6 = (df['Close'] > df['BBI']) & (df['Close'].shift(1) <= df['BBI'].shift(1))
        buy_c7 = (df.get('Foreign_Net', 0) > 0) & (df.get('Trust_Net', 0) > 0)
        buy_c8 = (df['+DI14'] > df['-DI14']) & (df['ADX14'] >= 20) & (df['ADX14'] > df['ADX14'].shift(1))

        df['Total_Net'] = df.get('Foreign_Net', 0) + df.get('Trust_Net', 0)
        window = 20
        price_new_low = df['Low'] <= df['Low'].rolling(window=window).min()

        # 👻 幽靈 2 處理：籌碼容錯 (過去 3 天內，主力曾創 20 日買超新高)
        chip_new_high_recent = (df['Total_Net'] >= df['Total_Net'].rolling(window=window).max()).rolling(window=3).max() > 0
        buy_c9 = price_new_low & chip_new_high_recent & (df['Total_Net'] > 0)

        # 🎯 這裡為您補齊斷尾的分數加總 (滿分改為 9 分)
        df['Buy_Score'] = (buy_trend.astype(int) + buy_c1_c2_score + buy_c3.astype(int) + 
                   buy_c4.astype(int) + buy_c5.astype(int) + buy_c6.astype(int) + 
                   buy_c7.astype(int) + buy_c8.astype(int) + buy_c9.astype(int))
        
        # --- 【賣方邏輯優化版】 ---
        sell_c1 = df['High'] >= df['BB_Upper']
        sell_c2 = df['RSI'] > df['DZ_Upper']
        # 👯 雙胞胎處理：突破上軌或進入動態超買區，只要有一個成立就拿 1 分
        sell_c1_c2_score = (sell_c1 | sell_c2).astype(int) 

        sell_c3 = (df['Volume'] > (df['Vol_MA20'] * 1.1)) & (df['Close'] < df['Open'])
        sell_c4 = (df['MACD_Hist'] < df['MACD_Hist'].shift(1)) & (df['DIF'] > 0)

        # 👻 幽靈 1 處理：區間頂背離 (比較過去 10 天的高點與 RSI 高點)
        past_10_high = df['High'].shift(1).rolling(10).max()
        past_10_rsi_max = df['RSI'].shift(1).rolling(10).max()
        sell_c5 = (df['High'] > past_10_high) & (df['RSI'] < past_10_rsi_max)

        sell_c6 = (df['Close'] < df['BBI']) & (df['Close'].shift(1) >= df['BBI'].shift(1))
        sell_c7 = (df.get('Foreign_Net', 0) < 0) & (df.get('Trust_Net', 0) < 0)
        sell_c8 = (df['-DI14'] > df['+DI14']) & (df['ADX14'] >= 20) & (df['ADX14'] > df['ADX14'].shift(1))

        # 👻 幽靈 2 處理：籌碼容錯 (過去 3 天內，主力曾創 20 日賣超新低/最大量)
        price_new_high = df['High'] >= df['High'].rolling(window=window).max()
        chip_new_low_recent = (df['Total_Net'] <= df['Total_Net'].rolling(window=window).min()).rolling(window=3).max() > 0
        sell_c9 = price_new_high & chip_new_low_recent & (df['Total_Net'] < 0)

        # 重新計算總分 (與買方對稱，滿分改為 9 分)
        df['Sell_Score'] = (sell_trend.astype(int) + sell_c1_c2_score + sell_c3.astype(int) + 
                    sell_c4.astype(int) + sell_c5.astype(int) + sell_c6.astype(int) + 
                    sell_c7.astype(int) + sell_c8.astype(int) + sell_c9.astype(int))

       # ===== 👇 【請把下面這段全數替換為新程式碼】 👇 =====
        # 🌊 新增：條件平滑特性 (動態滑價與讓點機制)
        # 1. 買進平滑：多頭時直接買(1.00)，空頭/盤整時要求回檔接(0.97)
        buy_adjust = np.where(buy_trend, 1.00, 0.97) 
        
        # 2. 賣出平滑：空頭時直接賣(1.00)，多頭時等溢價才賣(1.03)
        sell_adjust = np.where(sell_trend, 1.00, 1.03)
        
        #觸發買賣機制條件
        df['Buy_Signal'] = np.where(df['Buy_Score'] >= 4, df['Low'] * buy_adjust, np.nan)
        df['Sell_Signal'] = np.where(df['Sell_Score'] >= 4, df['High'] * sell_adjust, np.nan)
      
        # ==========================================
        # 4. 啟動回測引擎 (滿配版：融合交易成本、波動率、ADX與信心度)
        # ==========================================
        position = 0
        entry_price = 0          # 原始買進價
        actual_entry_cost = 0    # 包含手續費的真實總成本
        entry_trend_is_bull = False 
        entry_score = 0  
        trades = []
        
        # 💰 交易成本參數設定 (符合台灣市場真實狀況)
        FEE_RATE = 0.001425  # 券商公定手續費率 0.143%
        FEE_DISCOUNT = 0.6   # 假設一般券商給予 6 折優惠
        TAX_RATE = 0.003     # 台灣一般股票證交稅率 0.300%

        # 預先算好乘數，加速迴圈運算
        BUY_COST_MULTIPLIER = 1 + (FEE_RATE * FEE_DISCOUNT)
        SELL_NET_MULTIPLIER = 1 - (FEE_RATE * FEE_DISCOUNT) - TAX_RATE

        for index, row in df.iterrows():
            # 【空手時】尋找進場點
            if position == 0 and not pd.isna(row['Buy_Signal']):
                if row['Buy_Signal'] >= row['Low']:
                    position = 1
                    # 抓取原始買入價
                    raw_entry = min(row['Buy_Signal'], row['Open']) 
                    
                    # 🛡️ 真實成本墊高：買進時須支付手續費
                    actual_entry_cost = raw_entry * BUY_COST_MULTIPLIER
                    entry_price = raw_entry # 保留原始價格供參考
                    
                    entry_trend_is_bull = buy_trend.loc[index]
                    entry_score = row['Buy_Score'] 
            
            # 【持有單子時】尋找防線哪一個先被觸發
            elif position == 1:
                # 🛡️ 嚴格計算：使用「含成本的真實買價」來算損益百分比
                max_profit_pct = (row['High'] - actual_entry_cost) / actual_entry_cost
                max_loss_pct = (row['Low'] - actual_entry_cost) / actual_entry_cost
                
                # ---------------------------------------------------
                # 🛡️ 動態防線 2.0 系統啟動
                # ---------------------------------------------------
                volatility_pct = (row['BB_std'] * 1.5) / row['Close']
                DYNAMIC_SL = max(0.030, min(volatility_pct, 0.100)) 
                
                if entry_trend_is_bull and row['ADX14'] > 25:
                    DYNAMIC_TP = 0.250 
                    if entry_score >= 8:
                        DYNAMIC_TP = 9.990 
                else:
                    DYNAMIC_TP = 0.100
                
                # 💰 內部結算函數：負責扣除賣出手續費與證交稅
                def calculate_net_profit(raw_exit_price):
                    actual_exit_amount = raw_exit_price * SELL_NET_MULTIPLIER
                    # 真實淨利 = (真實賣出回收金額 - 真實買進總成本) / 真實買進總成本
                    return ((actual_exit_amount - actual_entry_cost) / actual_entry_cost) * 100

                # 防線 1：盤中觸發【動態停損防線】
                if max_loss_pct <= -DYNAMIC_SL:
                    # 找出觸發停損的理論價格
                    sl_price = actual_entry_cost * (1 - DYNAMIC_SL)
                    # 跌破保護：如果開盤就跳空跌破，只能用較差的開盤價停損
                    actual_sl_price = min(sl_price, row['Open'])
                    trades.append(calculate_net_profit(actual_sl_price))
                    position = 0
                    
                # 防線 2：盤中觸發【動態停利防線】
                elif max_profit_pct >= DYNAMIC_TP:
                    # 找出觸發停利的理論價格
                    tp_price = actual_entry_cost * (1 + DYNAMIC_TP)
                    # 跳空保護：如果開盤就跳空越過，以較優的開盤價停利
                    actual_tp_price = max(tp_price, row['Open'])
                    trades.append(calculate_net_profit(actual_tp_price))
                    position = 0
                    
                # 防線 3：收盤時確認觸發【系統指標賣出訊號】
                elif not pd.isna(row['Sell_Signal']):
                    actual_sell_price = min(row['Sell_Signal'], row['High'])
                    trades.append(calculate_net_profit(actual_sell_price))
                    position = 0

        # 🚨 關鍵除錯機制：期末強制平倉 (計算總帳也要扣除成本)
        if position == 1:
            final_raw_price = df.iloc[-1]['Close']
            final_exit_amount = final_raw_price * SELL_NET_MULTIPLIER
            final_profit_pct = ((final_exit_amount - actual_entry_cost) / actual_entry_cost) * 100
            trades.append(final_profit_pct)

        total_trades = len(trades)
        if total_trades > 0:
            win_rate = len([p for p in trades if p > 0]) / total_trades * 100
            total_profit = sum(trades)
        else:
            win_rate = 0.000
            total_profit = 0.000

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
        if buy_c9.iloc[-1]: buy_details.append(f"💎結構底背離(歷{int((buy_c9 & actual_buy_signals).sum())}次)")
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
        if sell_c9.iloc[-1]: sell_details.append(f"💣結構頂背離(歷{int((sell_c9 & actual_sell_signals).sum())}次)")
        trigger_str = "-"
        if buy_score >= 3:
            status = f"🔴 強買訊 ({buy_score}/10)"
            trigger_str = " + ".join(buy_details)
        elif sell_score >= 3:   
            status = f"🟢 強賣訊 ({sell_score}/10)"
            trigger_str = " + ".join(sell_details)
        elif buy_score == 2:    
            status = f"🟡 弱買訊 ({buy_score}/10)"
            trigger_str = " + ".join(buy_details)
        elif sell_score == 2:
            status = f"🟡 弱賣訊 ({sell_score}/10)"
            trigger_str = " + ".join(sell_details)
        else:
            max_score = max(buy_score, sell_score)
            status = f"⚪ 觀望中 ({max_score}/10)"
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

        # ... (前面保留原本的 strength_diff, structure_status 計算) ...

        # 🔌 新增：收集該檔股票 2 年內所有條件的「原始觸發總數」與「有效媒合總數」
        diagnostic_data = {
            "BBI多頭趨勢": [int(buy_trend.sum()), int((buy_trend & actual_buy_signals).sum())],
            "破下軌": [int(buy_c1.sum()), int((buy_c1 & actual_buy_signals).sum())],
            "RSI超賣": [int(buy_c2.sum()), int((buy_c2 & actual_buy_signals).sum())],
            "爆量": [int(buy_c3.sum()), int((buy_c3 & actual_buy_signals).sum())],
            "MACD轉強": [int(buy_c4.sum()), int((buy_c4 & actual_buy_signals).sum())],
            "底背離": [int(buy_c5.sum()), int((buy_c5 & actual_buy_signals).sum())],
            "🌟突破BBI": [int(buy_c6.sum()), int((buy_c6 & actual_buy_signals).sum())],
            "🔥法人同買": [int(buy_c7.sum()), int((buy_c7 & actual_buy_signals).sum())],
            "📈DMI趨勢成型": [int(buy_c8.sum()), int((buy_c8 & actual_buy_signals).sum())],
            "💎結構底背離": [int(buy_c9.sum()), int((buy_c9 & actual_buy_signals).sum())],
            
            "BBI空頭趨勢": [int(sell_trend.sum()), int((sell_trend & actual_sell_signals).sum())],
            "頂上軌":[int(sell_c1.sum()),int((sell_c1 & actual_sell_signals).sum())],
            "RSI超買":[int(sell_c2.sum()),int((sell_c2 & actual_sell_signals).sum())],
            "爆量":[int(sell_c3.sum()),int((sell_c3 & actual_sell_signals).sum())],
            "MACD轉弱":[int(sell_c4.sum()),int((sell_c4 & actual_sell_signals).sum())],
            "頂背離":[int(sell_c5.sum()),int((sell_c5 & actual_sell_signals).sum())],
            "💀跌破BBI":[int(sell_c6.sum()),int((sell_c6 & actual_sell_signals).sum())],
            "🧊法人同賣":[int(sell_c7.sum()),int((sell_c7 & actual_sell_signals).sum())],
            "📉DMI空頭成型":[int(sell_c8.sum()),int((sell_c8 & actual_sell_signals).sum())],
            "💣結構頂背離":[int(sell_c9.sum()),int((sell_c9 & actual_sell_signals).sum())]
        }

        # 🔌 在這裡呼叫基本面函數，取得財報資料
        f_data = add_fundamental_filter(ticker)

        # 👇 將技術面、籌碼面、基本面資料統一打包回傳
        return {
            "Ticker SYMBOL": ticker,
            "最新收盤價": round(current_price, 2),
            "結構強度": f"{strength_diff:+d}", 
            "今日系統燈號": status,
            "結構診斷": structure_status,
            "觸發條件明細": trigger_str,
            "基本面總分": f_data["基本面總分"],
            "營收年增率(%)": f"{f_data['營收年增率(%)']:.3f}",
            "營業利益率(%)": f"{f_data['營業利益率(%)']:.3f}",
            "系統勝率(%)": f"{win_rate:.3f}",       
            "累計報酬率(%)": f"{total_profit:.3f}", 
            "診斷數據": diagnostic_data 
        }

    except Exception as e:
        print(f"檢測 {ticker} 時發生錯誤: {e}")
        return None

# ==========================================
# 🚀 批次海選發動機 
# ==========================================
if __name__ == "__main__":

    test_targets = [
        "2330.TW"
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
        # 1. 🏆 全域市場氣候檢測 (方向性 vs. 盤整)
        total_count = len(report_cards)
        bull_count = len([r for r in report_cards if int(r["結構強度"]) > 2])
        bear_count = len([r for r in report_cards if int(r["結構強度"]) < -2])
        
        # 計算「趨勢強度」：如果市場有一邊倒的現象，代表趨勢明顯
        trend_intensity = max(bull_count, bear_count) / total_count
        
        # 2. 💰 資金分配參數
        TOTAL_BUDGET = 1000000    # 總預算
        MAX_POSITIONS = 5         # 同時持有上限
        base_allocation = TOTAL_BUDGET / MAX_POSITIONS

        # 3. 📉 雙向降載邏輯 (煞車系統)
        # 如果多空比例差不多 (例如各佔 30%)，代表市場在洗盤，這時應降載避免被雙巴
        if trend_intensity >= 0.600:
            risk_factor = 1.000
            market_msg = "🔥 趨勢極度明顯，全速執行單邊或對鎖策略。"
        elif trend_intensity >= 0.400:
            risk_factor = 0.600
            market_msg = "🌤️ 趨勢尚可，部位維持 60% 運行。"
        else:
            risk_factor = 0.300
            market_msg = "🌪️ 多空勢均力敵（盤整），自動降載至 30% 嚴防雙巴。"

        # 4. 🎯 雙向篩選：分別挑出「最強買訊」與「最強賣訊」
        long_candidates = [r for r in report_cards if "買訊" in r["今日系統燈號"]]
        short_candidates = [r for r in report_cards if "賣訊" in r["今日系統燈號"]]

        # 計算期望值評分並排序
        for r in (long_candidates + short_candidates):
            win_rate = float(r["系統勝率(%)"]) / 100
            total_ret = abs(float(r["累計報酬率(%)"])) # 放空報酬也是報酬，取絕對值
            r["期望值評分"] = round(total_ret * win_rate, 3)

        top_longs = sorted(long_candidates, key=lambda x: x.get("期望值評分", 0), reverse=True)
        top_shorts = sorted(short_candidates, key=lambda x: x.get("期望值評分", 0), reverse=True)

        # --- 顯示雙向資金分配報告 ---
        print("\n" + "═"*30 + " ⚔️ 雙向戰略配置報告 " + "═"*30)
        print(f"📈 多頭比例：{bull_count/total_count:.1%} | 📉 空頭比例：{bear_count/total_count:.1%}")
        print(f"🛡️ 風險狀態：{market_msg}")
        print(f"💵 建議單筆限額：${(base_allocation * risk_factor):,.0f}")
        print("-" * 83)
        
        # 顯示多空兩端的首選標的
        if top_longs:
            print(f"🚩 【作多首選】: {top_longs[0]['Ticker SYMBOL']} (評分: {top_longs[0]['期望值評分']:.3f})")
        if top_shorts:
            print(f"🏳️ 【放空首選】: {top_shorts[0]['Ticker SYMBOL']} (評分: {top_shorts[0]['期望值評分']:.3f})")
            
        print("-" * 83)
        if not top_longs and not top_shorts:
            print("📭 市場方向不明且無強烈訊號，建議空手觀望。")
        else:
            print(f"🎯 綜合排序前 {MAX_POSITIONS} 名進場建議：")
            all_ranked = sorted(top_longs + top_shorts, key=lambda x: x['期望值評分'], reverse=True)
            for i, stock in enumerate(all_ranked[:MAX_POSITIONS]):
                direction = "🔴 做多" if "買訊" in stock["今日系統燈號"] else "🟢 放空"
                print(f"  {i+1}. {stock['Ticker SYMBOL']} | {direction} | 期望評分: {stock['期望值評分']:.3f} | 建議配置: ${base_allocation * risk_factor:,.0f}")
        print("═"*83)


   
    # ==========================================
    # 👇 最終報表輸出與高階診斷 (雙向資金控管 + 三段式濾網) 👇
    # ==========================================
    if report_cards:
        # ---------------------------------------------------
        # 第一部：⚔️ 雙向戰略與基本面權重配置 (資金分配器)
        # ---------------------------------------------------
        total_st = len(report_cards)
        bull_st = len([r for r in report_cards if int(r.get("結構強度", 0)) > 2])
        bear_st = len([r for r in report_cards if int(r.get("結構強度", 0)) < -2])
        trend_intensity = max(bull_st, bear_st) / total_st if total_st > 0 else 0
        
        TOTAL_BUDGET, MAX_POS = 1000000, 5
        risk_factor = 1.0 if trend_intensity >= 0.6 else 0.6 if trend_intensity >= 0.4 else 0.3
        
        for r in report_cards:
            win_rate = float(r["系統勝率(%)"]) / 100
            total_ret = abs(float(r["累計報酬率(%)"]))
            f_weight = int(r["基本面總分"]) * 2
            
            bonus = f_weight if "買訊" in r["今日系統燈號"] else -f_weight if "賣訊" in r["今日系統燈號"] else 0
            r["期望值評分"] = round((total_ret * win_rate) + bonus, 3)

        all_ranked = sorted([r for r in report_cards if "觀望" not in r["今日系統燈號"]], 
                            key=lambda x: x.get('期望值評分', 0), reverse=True)

        print("\n" + "═"*30 + " ⚔️ 雙向戰略配置報告 " + "═"*30)
        print(f"📊 趨勢強度：{trend_intensity:.3f} | 🛡️ 部位係數：{risk_factor:.3f}")
        print("-" * 83)
        for i, stock in enumerate(all_ranked[:MAX_POS]):
            direction = "🔴 做多" if "買訊" in stock["今日系統燈號"] else "🟢 放空"
            print(f"  {i+1}. {stock['Ticker SYMBOL']} | {direction} | 評分: {stock['期望值評分']:.3f} | 建議配置: ${(TOTAL_BUDGET/MAX_POS)*risk_factor:,.0f}")
        print("═"*83)

        # ---------------------------------------------------
        # 第二部：📊 今日海選總表
        # ---------------------------------------------------
        final_report = pd.DataFrame(report_cards)
        pd.set_option('display.unicode.east_asian_width', True) 
        
        print("\n" + "="*25 + " 今日海選總表 " + "="*25)
        display_cols = [
            "Ticker SYMBOL", "最新收盤價", "結構強度", "今日系統燈號", 
            "結構診斷", "基本面總分", "營收年增率(%)", "營業利益率(%)", 
            "系統勝率(%)", "累計報酬率(%)", "觸發條件明細"
        ]
        
        actual_cols = [col for col in display_cols if col in final_report.columns]
        print(final_report[actual_cols].to_string(index=False))
        print("="*75)

        # ---------------------------------------------------
        # 第三部：🔌 [高階診斷器]：三段式邏輯過濾 (完整保留)
        # ---------------------------------------------------
        all_condition_keys = [
            "BBI多頭趨勢", "破下軌", "RSI超賣", "爆量", "MACD轉強", 
            "底背離", "🌟突破BBI", "🔥法人同買", "📈DMI趨勢成型", "💎結構底背離","BBI空頭趨勢",
            "頂上軌","RSI超買","爆量","MACD轉弱","頂背離","💀跌破BBI","🧊法人同賣","📉DMI空頭成型","💣結構頂背離"
        ]
        
        # 1. 計算全域歷史統計
        global_stats = {key: 0 for key in all_condition_keys}
        for card in report_cards:
            diag = card.get("診斷數據", {})
            for cond_name in all_condition_keys:
                if cond_name in diag:
                    global_stats[cond_name] += diag[cond_name][1]

        # 2. 找出「今日有被觸發」的條件 (從所有報告的明細中搜尋)
        today_active_set = set()
        for card in report_cards:
            detail = str(card.get("觸發條件明細", ""))
            for key in all_condition_keys:
                if key in detail:
                    today_active_set.add(key)

        # 3. 三分類邏輯拆解
        top_tier = {k: global_stats[k] for k in all_condition_keys if global_stats[k] > 0 and k in today_active_set}
        mid_tier = {k: global_stats[k] for k in all_condition_keys if global_stats[k] > 0 and k not in today_active_set}
        bottom_tier = [k for k in all_condition_keys if global_stats[k] == 0]

        print("\n🔍 [深度分析] 指標戰力分佈報告：")
        
        print("\n🔥 【已媒合條件：今日實質發動中】(系統目前的獲利箭頭)")
        if top_tier:
            for name, count in sorted(top_tier.items(), key=lambda x: x[1], reverse=True):
                print(f"  🚩 {name.ljust(15)} : 今日有訊號 (歷史累計助攻 {str(count).rjust(4)} 次)")
        else:
            print("  (今日暫無指標產生共振訊號)")

        print("\n✅ 【已媒合條件：歷史有效但今日未觸發】(潛伏中的主力)")
        if mid_tier:
            for name, count in sorted(mid_tier.items(), key=lambda x: x[1], reverse=True):
                print(f"  💤 {name.ljust(15)} : 今日無訊號 (歷史累計助攻 {str(count).rjust(4)} 次)")
        else:
            print("  (所有曾有過貢獻的指標今日全數發動中！)")

        print("\n❌ 【未媒合條件：歷史累計 0 次】(系統冗餘或參數過嚴)")
        if bottom_tier:
            for name in bottom_tier:
                print(f"  ⚪ {name.ljust(15)} : 從未成功媒合過 (⚠️ 請考慮優化或捨棄)")
        else:
            print("  (恭喜！所有指標均具備實戰貢獻紀錄)")

        print("\n" + "-"*75)
        print("💡 註：歷史助攻次數是指在『總分 ≥ 4』時，該指標出現在獲勝組合中的次數。")

    else:
        print("掃描失敗，無資料輸出。")