import yfinance as yf
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import numpy as np
import pyodbc 
from advanced_chart import draw_chart
from FinMind.data import DataLoader
from config import PARAMS


# ==========================================
# ⚡️ 初始化 DataLoader 與資料庫連線設定
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMy0yNyAyMjowNTowMCIsInVzZXJfaWQiOiJob25kYSIsImVtYWlsIjoiaG9uZGEyMTMxMTMwQGdtYWlsLmNvbSIsImlwIjoiMjcuMjQwLjI1MC4xNTIifQ.JmayRjSVQqs6SdyCdLn1Z8uWyuYgvHHjOE32UxWI-_8"
dl = DataLoader(token=API_TOKEN)

DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

# ==========================================
# 🔌 籌碼資料外掛模組 (資料合併處理廠)
# ==========================================
def add_chip_data(df, ticker):
    """
    負責把 yfinance 的價格表，貼上 FinMind 的三大法人買賣超資料
    (修正版：對齊 API 回傳的英文名稱，並新增自營商)
    """
    pure_ticker = ticker.split('.')[0]
    start_dt = (pd.Timestamp.today() - pd.Timedelta(days=120)).strftime("%Y-%m-%d")
    
    try:
        chip_df = dl.taiwan_stock_institutional_investors(stock_id=pure_ticker, start_date=start_dt)
        
        # 如果沒抓到資料，提早結束並給 0
        if chip_df is None or (isinstance(chip_df, pd.DataFrame) and chip_df.empty):
            df['Foreign_Net'], df['Trust_Net'], df['Dealers_Net'] = 0, 0, 0
            return df
            
        chip_df['Net'] = chip_df['buy'] - chip_df['sell']
        
        # 🌟 1. 改用英文名稱搜尋三大法人
        foreign = chip_df[chip_df['name'].str.contains('Foreign_Investor')].groupby('date')['Net'].sum()
        trust = chip_df[chip_df['name'].str.contains('Investment_Trust')].groupby('date')['Net'].sum()
        dealers = chip_df[chip_df['name'].str.contains('Dealer')].groupby('date')['Net'].sum()
        
        # 🌟 絕對強制的日期對齊術 (解決合併變 0 的問題)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df.index = pd.to_datetime(df.index).normalize() # 強制移除 yfinance 的隱藏時間

        if not foreign.empty: 
            foreign.index = pd.to_datetime(foreign.index).normalize()
        if not trust.empty: 
            trust.index = pd.to_datetime(trust.index).normalize()
        if not dealers.empty: 
            dealers.index = pd.to_datetime(dealers.index).normalize()

        # 🌟 2. 將三大法人併入你的 df
        df = df.join(foreign.rename('Foreign_Net'), how='left')
        df = df.join(trust.rename('Trust_Net'), how='left')
        df = df.join(dealers.rename('Dealers_Net'), how='left')

        # 空白的日子填補 0
        df['Foreign_Net'] = df['Foreign_Net'].ffill().fillna(0)
        df['Trust_Net'] = df['Trust_Net'].ffill().fillna(0)
        df['Dealers_Net'] = df['Dealers_Net'].ffill().fillna(0)
        
    except KeyError as e:
        print(f"❌ {ticker} API 結構錯誤: {e}")
        df['Foreign_Net'], df['Trust_Net'], df['Dealers_Net'] = 0, 0, 0
    except Exception as e:
        print(f"⚠️ {ticker} 籌碼處理發生錯誤: {e}")
        df['Foreign_Net'], df['Trust_Net'], df['Dealers_Net'] = 0, 0, 0
        
    return df

# ==========================================
# 📊 新增：基本面數據採集器 (FinMind 版)
# ==========================================
def add_fundamental_filter(ticker, p=PARAMS):  # 🌟 補上 p=PARAMS 讓它能讀取設定
    """抓取營收與獲利能力，判斷基本面體質"""
    pure_ticker = ticker.split('.')[0]
    try:
        rev_df = dl.taiwan_stock_month_revenue(stock_id=pure_ticker)
        rev_yoy = rev_df.iloc[-1]['revenue_year_growth'] if not rev_df.empty else 0.0

        st_df = dl.taiwan_stock_financial_statement(stock_id=pure_ticker)
        if not st_df.empty:
            op_margin_row = st_df[st_df['type'] == 'OperatingProfitMargin']
            op_margin = op_margin_row.iloc[-1]['value'] if not op_margin_row.empty else 0.0
        else:
            op_margin = 0.0

        f_score = 0
        # 🌟 將寫死的數字替換成 Config 參數
        if rev_yoy > p.get('FUNDAMENTAL_YOY_BASE', 0): f_score += 1
        if rev_yoy > p.get('FUNDAMENTAL_YOY_EXCELLENT', 20): f_score += 1 
        if op_margin > p.get('FUNDAMENTAL_OPM_BASE', 0): f_score += 1 
        if op_margin < p.get('FUNDAMENTAL_OPM_BASE', 0): f_score -= 2 

        return {"營收年增率(%)": rev_yoy, "營業利益率(%)": op_margin, "基本面總分": f_score}
    except:
        return {"營收年增率(%)": 0.000, "營業利益率(%)": 0.000, "基本面總分": 0}


# ==========================================
# 🧰 工具層 (Tool Layer)：純粹的計算機，不綁定任何股票狀態
# ==========================================
def apply_slippage(price, direction, slippage):
    return price * (1 + slippage * direction)

def get_exit_price(entry_price, open_price, sl_pct, direction):
    stop_price = entry_price * (1 - sl_pct * direction)
    # 跳空判斷
    if (direction == 1 and open_price < stop_price) or \
       (direction == -1 and open_price > stop_price):
        return open_price
    else:
        return stop_price
    
# 👇 🌟 新增這段：停利精準計算器
def get_tp_price(entry_price, open_price, tp_pct, direction):
    target_price = entry_price * (1 + tp_pct * direction)
    # 停利跳空判斷：多單開在目標價之上，或空單開在目標價之下 (幸運多賺)
    if (direction == 1 and open_price > target_price) or \
       (direction == -1 and open_price < target_price):
        return open_price
    return target_price

def calculate_pnl(direction, entry_price, exit_price, shares, fee_rate, tax_rate):
    invested = entry_price * shares
    if direction == 1:
        entry_cost = invested * (1 + fee_rate)
        exit_value = exit_price * shares * (1 - fee_rate - tax_rate)
        pnl = exit_value - entry_cost
    else:
        entry_value = invested * (1 - fee_rate - tax_rate)
        exit_cost = exit_price * shares * (1 + fee_rate)
        pnl = entry_value - exit_cost
    return pnl, invested


# ==========================================
# 1. 核心檢測模組封裝 (全面參數化)
# ==========================================
def inspect_stock(ticker, preloaded_df=None, p=PARAMS):
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
        avg_gain = gain.ewm(alpha=1/p['RSI_PERIOD'], min_periods=p['RSI_PERIOD'], adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/p['RSI_PERIOD'], min_periods=p['RSI_PERIOD'], adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['RSI'] = 100 - (100 / (1 + rs))
        
        df['RSI_MA'] = df['RSI'].rolling(window=p['RSI_PERIOD']).mean()
        df['RSI_STD'] = df['RSI'].rolling(window=p['RSI_PERIOD']).std()
        df['DZ_Upper'] = df['RSI_MA'] + (df['RSI_STD'] * 1.5)
        df['DZ_Lower'] = df['RSI_MA'] - (df['RSI_STD'] * 1.5)

        # 2. 基礎指標計算 (MACD)
        df['EMA12'] = df['Close'].ewm(span=p['MACD_FAST'], adjust=False).mean()
        df['EMA26'] = df['Close'].ewm(span=p['MACD_SLOW'], adjust=False).mean()
        df['DIF'] = df['EMA12'] - df['EMA26']
        df['MACD_Signal'] = df['DIF'].ewm(span=p['MACD_SIGNAL'], adjust=False).mean()
        df['MACD_Hist'] = (df['DIF'] - df['MACD_Signal']) * 2

        # 3. 基礎指標計算 (BBands)
        df['MA20'] = df['Close'].rolling(window=p['BB_WINDOW']).mean()
        df['BB_std'] = df['Close'].rolling(window=p['BB_WINDOW']).std()
        df['BB_Upper'] = df['MA20'] + (df['BB_std'] * p['BB_STD'])
        df['BB_Lower'] = df['MA20'] - (df['BB_std'] * p['BB_STD'])
        df['Vol_MA20'] = df['Volume'].rolling(window=p['VOL_WINDOW']).mean()

        # 4. BBI
        bbi_cols = []
        for days in p['BBI_PERIODS']:
            col_name = f'MA{days}'
            df[col_name] = df['Close'].rolling(window=days).mean()
            bbi_cols.append(df[col_name])
        
        df['BBI'] = sum(bbi_cols) / len(p['BBI_PERIODS'])
        

        # 5. DMI (動向指標) - 參數化
        high_diff = df['High'].diff()
        low_diff = -df['Low'].diff() 
        
        df['+DM'] = np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0)
        df['-DM'] = np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0)
        
        tr1 = df['High'] - df['Low']
        tr2 = abs(df['High'] - df['Close'].shift(1))
        tr3 = abs(df['Low'] - df['Close'].shift(1))
        df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        df['+DI14'] = 100 * (df['+DM'].rolling(p['DMI_PERIOD']).sum() / df['TR'].rolling(p['DMI_PERIOD']).sum())
        df['-DI14'] = 100 * (df['-DM'].rolling(p['DMI_PERIOD']).sum() / df['TR'].rolling(p['DMI_PERIOD']).sum())
        df['DX'] = 100 * abs(df['+DI14'] - df['-DI14']) / (df['+DI14'] + df['-DI14'])
        df['ADX14'] = df['DX'].rolling(p['DMI_PERIOD']).mean()

        # ==========================================
        # 🌊 升級模組：ATR 動態公差與無未來函數背離 (Rolling Window)
        # ==========================================
        df['ATR'] = df['TR'].ewm(alpha=1/p['DMI_PERIOD'], adjust=False).mean()
        df['Total_Net'] = df.get('Foreign_Net', 0) + df.get('Trust_Net', 0) + df.get('Dealers_Net', 0)

        def detect_divergence(price_series, indicator_series, atr_series, is_top=True, distance=5, atr_mult=1.0, threshold=None):
            """
            【終極修正版】無未來函數的滾動視窗背離偵測
            嚴格只使用 t-1 之前的資料尋找前波高低點，杜絕資料洩漏 (Look-ahead Bias)。
            """
            p_vals = price_series if isinstance(price_series, np.ndarray) else price_series.values
            i_vals = indicator_series if isinstance(indicator_series, np.ndarray) else indicator_series.values
            a_vals = atr_series if isinstance(atr_series, np.ndarray) else atr_series.values
            
            div_signals = np.zeros(len(p_vals), dtype=bool)
            lookback = 20  # 強制設定回看視窗為 20 天，尋找前一波高低點
            
            for i in range(lookback, len(p_vals)):
                # 擷取過去的視窗 (排除今天與昨天，確保找出來的是「前一波」的波段點)
                past_p = p_vals[i-lookback : i-1]
                past_i = i_vals[i-lookback : i-1]
                
                if len(past_p) == 0: continue
                
                if is_top:
                    # 找前一波的最高點
                    prev_idx = np.argmax(past_p)
                    prev_p = past_p[prev_idx]
                    prev_i = past_i[prev_idx]
                    
                    # 條件 1：價格創新高，但指標沒創新高 (頂背離)
                    cond_div = (p_vals[i] > prev_p) and (i_vals[i] < prev_i)
                    # 條件 2：指標必須達到超買區門檻
                    cond_thresh = True if threshold is None else (i_vals[i] > threshold)
                    # 條件 3：中間必須有合理的回檔 (用 ATR 衡量，確保是兩個獨立的山頭)
                    retrace_valid = (prev_p - np.min(p_vals[i-lookback+prev_idx : i])) > (a_vals[i] * atr_mult)
                    
                    if cond_div and cond_thresh and retrace_valid:
                        div_signals[i] = True
                        
                else:
                    # 找前一波的最低點
                    prev_idx = np.argmin(past_p)
                    prev_p = past_p[prev_idx]
                    prev_i = past_i[prev_idx]
                    
                    # 條件 1：價格創新低，但指標沒創新低 (底背離)
                    cond_div = (p_vals[i] < prev_p) and (i_vals[i] > prev_i)
                    # 條件 2：指標必須達到超賣區門檻
                    cond_thresh = True if threshold is None else (i_vals[i] < threshold)
                    # 條件 3：中間必須有合理的反彈 (確保是兩個獨立的谷底)
                    retrace_valid = (np.max(p_vals[i-lookback+prev_idx : i]) - prev_p) > (a_vals[i] * atr_mult)
                    
                    if cond_div and cond_thresh and retrace_valid:
                        div_signals[i] = True
            
            return pd.Series(div_signals, index=df.index)

        # 🚨 [致命問題 2 修復]：拔除 dropna() 造成的資料洩漏
        # 捨棄原本會把歷史斷層填平的 dropna，改成只切除最前面指標算不出來的 60 天
        df = df.iloc[60:].copy() 
        if df.empty: return None
        
        # ==========================================
        # 🛡️ 基礎防護網 (參數化)
        # ==========================================
        latest_check = df.iloc[-1]
        if latest_check['Vol_MA20'] < p['MIN_VOL_MA20']: return None 
        if latest_check['Close'] < p['MIN_PRICE']: return None 

        # ==========================================
        # 🌟 第一層：定義市場狀態 (Regime Filter)
        # 判斷現在的大環境，決定該用什麼戰術
        # ==========================================
        adx_strong = df['ADX14'] >= p['ADX_TREND_THRESHOLD']
        is_bull_trend = (df['Close'] > df['BBI']) & adx_strong
        is_bear_trend = (df['Close'] < df['BBI']) & adx_strong
        is_ranging = ~(is_bull_trend | is_bear_trend)

        df['Regime'] = np.where(is_bull_trend, '趨勢多頭', 
                       np.where(is_bear_trend, '趨勢空頭', '區間盤整'))

        # ==========================================
        # 🌟 第二層：計算基礎條件 (維持底層指標計算)
        # ==========================================
        _F = pd.Series(False, index=df.index) 
        buy_trend = (df['Close'] > df['BBI']) & (df['BBI'] > df['BBI'].shift(1))
        sell_trend = (df['Close'] < df['BBI']) & (df['BBI'] < df['BBI'].shift(1))

        buy_c1 = (df['Low'] <= df['BB_Lower']) if p.get('USE_BBANDS', True) else _F
        buy_c2 = (df['RSI'] < df['DZ_Lower']) if p.get('USE_RSI', True) else _F
        buy_c3 = ((df['Volume'] > (df['Vol_MA20'] * p['VOL_BREAKOUT_MULTIPLIER'])) & (df['Close'] > df['Open'])) if p.get('USE_VOL_BREAKOUT', True) else _F
        buy_c4 = ((df['MACD_Hist'] > df['MACD_Hist'].shift(1)) & (df['DIF'] < 0)) if p.get('USE_MACD', True) else _F
        buy_c5 = detect_divergence(df['Low'].values, df['RSI'].values, df['ATR'].values, is_top=False, distance=5, atr_mult=0.8, threshold=45) if p.get('USE_DIVERGENCE_RSI', True) else _F
        buy_c6 = ((df['Close'] > df['BBI']) & (df['Close'].shift(1) <= df['BBI'].shift(1))) if p.get('USE_BBI_BREAKOUT', True) else _F
        buy_c7 = ((df.get('Foreign_Net', 0) > 0) & (df.get('Trust_Net', 0) > 0)) if p.get('USE_CHIPS', True) else _F
        buy_c8 = ((df['+DI14'] > df['-DI14']) & (df['ADX14'] >= p['ADX_TREND_THRESHOLD']) & (df['ADX14'] > df['ADX14'].shift(1))) if p.get('USE_DMI', True) else _F
        buy_c9_base = detect_divergence(df['Low'].values, df['Total_Net'].values, df['ATR'].values, is_top=False, distance=5, atr_mult=0.5) if p.get('USE_DIVERGENCE_CHIPS', True) else _F
        buy_c9 = buy_c9_base & (df['Total_Net'] > 0) 

        sell_c1 = (df['High'] >= df['BB_Upper']) if p.get('USE_BBANDS', True) else _F
        sell_c2 = (df['RSI'] > df['DZ_Upper']) if p.get('USE_RSI', True) else _F
        sell_c3 = ((df['Volume'] > (df['Vol_MA20'] * p['VOL_BREAKOUT_MULTIPLIER'])) & (df['Close'] < df['Open'])) if p.get('USE_VOL_BREAKOUT', True) else _F
        sell_c4 = ((df['MACD_Hist'] < df['MACD_Hist'].shift(1)) & (df['DIF'] > 0)) if p.get('USE_MACD', True) else _F
        sell_c5 = detect_divergence(df['High'].values, df['RSI'].values, df['ATR'].values, is_top=True, distance=5, atr_mult=0.8, threshold=55) if p.get('USE_DIVERGENCE_RSI', True) else _F
        sell_c6 = ((df['Close'] < df['BBI']) & (df['Close'].shift(1) >= df['BBI'].shift(1))) if p.get('USE_BBI_BREAKOUT', True) else _F
        sell_c7 = ((df.get('Foreign_Net', 0) < 0) & (df.get('Trust_Net', 0) < 0)) if p.get('USE_CHIPS', True) else _F
        sell_c8 = ((df['-DI14'] > df['+DI14']) & (df['ADX14'] >= p['ADX_TREND_THRESHOLD']) & (df['ADX14'] > df['ADX14'].shift(1))) if p.get('USE_DMI', True) else _F
        sell_c9_base = detect_divergence(df['High'].values, df['Total_Net'].values, df['ATR'].values, is_top=True, distance=5, atr_mult=0.5) if p.get('USE_DIVERGENCE_CHIPS', True) else _F
        sell_c9 = sell_c9_base & (df['Total_Net'] < 0) 

        # ==========================================
        # 🌟 第三層：Setup 分流 (將陣型與市場狀態綁定)
        # ==========================================
        buy_c3_mem = buy_c3 | buy_c3.shift(1)  
        buy_c7_mem = buy_c7 | buy_c7.shift(1)  
        
        # 1. 突破發動 Setup (只在【趨勢多頭】或【盤整轉多】時允許)
        setup_breakout = buy_c7_mem & buy_c3_mem & buy_c6 
        valid_breakout = setup_breakout & (is_bull_trend | is_ranging)
        
        # 2. 超跌反彈 Setup (嚴禁在空頭趨勢中接刀，只在盤整區操作)
        setup_reversal = buy_c1 & buy_c2 & buy_c5 
        valid_reversal = setup_reversal & is_ranging
        
        # 3. 籌碼潛伏 Setup (只在盤整或跌勢末端允許)
        setup_divergence = buy_c9 & buy_c4 & buy_trend 
        valid_divergence = setup_divergence & (is_ranging | is_bear_trend)

        # -- 空方 Setup --
        sell_c3_mem = sell_c3 | sell_c3.shift(1)
        sell_c7_mem = sell_c7 | sell_c7.shift(1)

        sell_setup_breakdown = sell_c7_mem & sell_c3_mem & sell_c6 
        valid_sell_breakdown = sell_setup_breakdown & (is_bear_trend | is_ranging)
        
        sell_setup_reversal  = sell_c1 & sell_c2 & sell_c5 
        valid_sell_reversal = sell_setup_reversal & is_ranging
        
        sell_setup_divergence = sell_c9 & sell_c4 & sell_trend 
        valid_sell_divergence = sell_setup_divergence & (is_ranging | is_bull_trend)

        # ==========================================
        # 🌟 第四層：扣板機觸發 (Trigger) + 計分引擎
        # ==========================================
        # 昨天的 Setup 背景是否成立？
        prev_valid_breakout = valid_breakout.shift(1).fillna(False)
        prev_valid_reversal = valid_reversal.shift(1).fillna(False)
        prev_valid_divergence = valid_divergence.shift(1).fillna(False)

        prev_valid_sell_breakdown = valid_sell_breakdown.shift(1).fillna(False)
        prev_valid_sell_reversal = valid_sell_reversal.shift(1).fillna(False)
        prev_valid_sell_divergence = valid_sell_divergence.shift(1).fillna(False)

        # 今天的 Trigger：實質的價格表態確認！
        trigger_long = df['Close'] > df['High'].shift(1)  # 今天收盤必須大於昨天最高價
        trigger_short = df['Close'] < df['Low'].shift(1)  # 今天收盤必須小於昨天最低價

        # 終極訊號：Setup (背景) + Trigger (確認)
        final_long_breakout = prev_valid_breakout & trigger_long
        final_long_reversal = prev_valid_reversal & trigger_long
        final_long_divergence = prev_valid_divergence & trigger_long

        final_short_breakdown = prev_valid_sell_breakdown & trigger_short
        final_short_reversal = prev_valid_sell_reversal & trigger_short
        final_short_divergence = prev_valid_sell_divergence & trigger_short

        # A. 傳統累加算分 (保留給非狙擊模式使用)
        base_buy_score = (buy_trend.astype(int) + buy_c1.astype(int) + buy_c2.astype(int) + 
                          buy_c3.astype(int) + buy_c4.astype(int) + buy_c5.astype(int) + 
                          buy_c6.astype(int) + buy_c7.astype(int) + buy_c8.astype(int) + buy_c9.astype(int))
        
        base_sell_score = (sell_trend.astype(int) + sell_c1.astype(int) + sell_c2.astype(int) + 
                           sell_c3.astype(int) + sell_c4.astype(int) + sell_c5.astype(int) + 
                           sell_c6.astype(int) + sell_c7.astype(int) + sell_c8.astype(int) + sell_c9.astype(int))

        # B. 根據模式設定最終分數
        if p.get('USE_SNIPER_MODE', True):
            df['Buy_Score'] = np.where(final_long_breakout | final_long_reversal | final_long_divergence, 10, 0)
            df['Sell_Score'] = np.where(final_short_breakdown | final_short_reversal | final_short_divergence, 10, 0)
        else:
            df['Buy_Score'] = base_buy_score
            df['Sell_Score'] = base_sell_score

        # ==========================================
        # 🌟 方向攔截器 (沒收不要的陣型，精準拔除)
        # ==========================================
        if not p.get('ALLOW_LONG', True):
            df['Buy_Score'] = 0
            final_long_breakout = final_long_breakout & False
            final_long_reversal = final_long_reversal & False
            final_long_divergence = final_long_divergence & False

        if not p.get('ALLOW_SHORT', True):
            df['Sell_Score'] = 0
            final_short_breakdown = final_short_breakdown & False
            final_short_reversal = final_short_reversal & False
            final_short_divergence = final_short_divergence & False

        # ==========================================
        # 🌟 終極陣型標籤機 (最後才貼標籤)
        # ==========================================
        df['Golden_Type'] = np.where(final_long_breakout, "🔥主力點火(已確認)", 
                            np.where(final_long_reversal, "🩸恐慌抄底(已確認)", 
                            np.where(final_long_divergence, "🕵️籌碼潛伏(已確認)", 
                            np.where(final_short_breakdown, "🧊主力倒貨(已確認)",
                            np.where(final_short_reversal, "💀貪婪摸頭(已確認)",
                            np.where(final_short_divergence, "💣偷偷出貨(已確認)", "無"))))))
        

        # ==========================================
        # 🌟 訊號標記 (移除幽靈讓點機制，直接記錄觸發當下的收盤價)
        # ==========================================
        df['Buy_Signal'] = np.where(df['Buy_Score'] >= p['TRIGGER_SCORE'], df['Close'], np.nan)
        df['Sell_Signal'] = np.where(df['Sell_Score'] >= p['TRIGGER_SCORE'], df['Close'], np.nan)
      
        # ==========================================
        # 4. 啟動回測引擎 (終極模組化架構 + Direction/Position 分離)
        # ==========================================

        # --- 回測環境初始化 ---
        position = 0             
        direction = 0
        entry_price = 0          
        entry_trend_is_bull = False 
        entry_score = 0  
        entry_date = None 
        trades = []
        
        sim_balance = 10000000      
        TRADE_SHARES = 0  # 🌟 拔除寫死的股數，讓系統在進場時動態計算！         
        
        # 🌟 新增：用來紀錄進場後的「極端價格」 (多單記最高價，空單記最低價)
        max_reached_price = 0.0  
        
        # 🌟 將寫死的 0.0015 換成總控制台的參數
        SLIPPAGE = p['MARKET_SLIPPAGE'] 
        exit_fee_rate = p['FEE_RATE'] * p['FEE_DISCOUNT']

        db_conn = None
        db_cursor = None
        try:
            db_conn = pyodbc.connect(DB_CONN_STR)
            db_cursor = db_conn.cursor()
        except Exception as e:
            print(f"⚠️ 資料庫連線失敗: {e}")

        df['Prev_Buy_Score'] = df['Buy_Score'].shift(1)
        df['Prev_Sell_Score'] = df['Sell_Score'].shift(1)
        df['Prev_Trend'] = buy_trend.shift(1)

        for index, row in df.iterrows():
            if pd.isna(row['Prev_Buy_Score']):
                continue

            safe_open = row['Open'] if row['Open'] > 0 else 0.0001
            safe_close = row['Close'] if row['Close'] > 0 else 0.0001

            # ====================
            # 進場 (含機構級 Risk Parity 資金控管)
            # ====================
            if position == 0:
                # 🌟 解除硬限制，改由 config.py 的 TRIGGER_SCORE 動態決定進場門檻
                if row['Prev_Buy_Score'] >= p['TRIGGER_SCORE']:
                    direction = 1
                elif row['Prev_Sell_Score'] >= p['TRIGGER_SCORE']:
                    direction = -1
                else:
                    continue

                position = 1
                entry_price = apply_slippage(safe_open, direction, SLIPPAGE)
                entry_date = index
                entry_score = row['Prev_Buy_Score'] if direction == 1 else row['Prev_Sell_Score']
                entry_trend_is_bull = row['Prev_Trend']
                
                max_reached_price = entry_price 

                # ==========================================
                # 🧠 AI 精算師：依據波動率動態計算購買股數
                # ==========================================
                # 1. 估算這檔股票目前的合理停損 % 數
                volatility_pct = (row['BB_std'] * 1.5) / safe_close
                entry_sl_pct = max(p['SL_MIN_PCT'], min(volatility_pct, p['SL_MAX_PCT']))
                
                # 2. 定義單筆交易「絕對不能超過的虧損上限」(例如總資金的 1.5%)
                risk_allowance = sim_balance * 0.015 
                
                # 3. 反推能買多少股：股數 = 允許虧損金額 / (進場價 * 停損百分比)
                raw_shares = risk_allowance / (entry_price * entry_sl_pct)
                
                # 4. 台股最佳化買法 (優先買整張，買不起才買零股)
                if raw_shares >= 1000:
                    TRADE_SHARES = int(raw_shares // 1000) * 1000
                else:
                    TRADE_SHARES = max(1, int(raw_shares))
                    
                # 5. 防呆機制：再怎麼重壓，單筆總金額不能超過帳戶剩餘現金的 33%
                max_affordable_shares = int((sim_balance * 0.33) / entry_price)
                TRADE_SHARES = min(TRADE_SHARES, max_affordable_shares)
            # ====================
            # 出場 (含 Trailing Stop 移動停利)
            # ====================
            else:
                volatility_pct = (row['BB_std'] * 1.5) / safe_close
                DYNAMIC_SL = max(p['SL_MIN_PCT'], min(volatility_pct, p['SL_MAX_PCT']))

                trend_is_with_me = (direction == 1 and entry_trend_is_bull) or (direction == -1 and not entry_trend_is_bull)
                adx = row['ADX14'] if not pd.isna(row['ADX14']) else 0
                
                # 基礎目標價 (若無觸發移動停利，則在這邊獲利了結)
                DYNAMIC_TP = p['TP_TREND_PCT'] if (trend_is_with_me and adx > p['ADX_TREND_THRESHOLD']) else p['TP_BASE_PCT']

                # 🚨 拔除原本 9.99 的危險寫法，改用動態追蹤！
                
                is_exit = False
                actual_exit_price = 0

                # 🌟 核心：更新極端價格，並計算移動停利線 (Trailing Stop Line)
                if direction == 1:
                    max_reached_price = max(max_reached_price, row['High'])
                    # 多單防守線：從最高價回檔 DYNAMIC_SL 就出場
                    trailing_stop_line = max_reached_price * (1 - DYNAMIC_SL)
                    
                    # 最終防守線：取「初始停損」與「移動防守線」的最高者 (確保防線只進不退)
                    stop_price = max(get_exit_price(entry_price, safe_open, DYNAMIC_SL, 1), trailing_stop_line)
                    
                    tp_price = entry_price * (1 + DYNAMIC_TP)
                    
                    # 1. 檢查是否打到移動停利 / 初始停損
                    if row['Low'] <= stop_price:
                        actual_exit_price = stop_price
                        is_exit = True
                    # 2. 檢查是否暴漲直接達標 (傳統停利)
                    elif row['High'] >= tp_price and entry_score < 10: 
                        actual_exit_price = get_tp_price(entry_price, safe_open, DYNAMIC_TP, 1)
                        is_exit = True
                        
                else:
                    max_reached_price = min(max_reached_price, row['Low']) if max_reached_price > 0 else row['Low']
                    # 空單防守線：從最低價反彈 DYNAMIC_SL 就出場
                    trailing_stop_line = max_reached_price * (1 + DYNAMIC_SL)
                    
                    stop_price = min(get_exit_price(entry_price, safe_open, DYNAMIC_SL, -1), trailing_stop_line)
                    tp_price = entry_price * (1 - DYNAMIC_TP)
                    
                    if row['High'] >= stop_price:
                        actual_exit_price = stop_price
                        is_exit = True
                    elif row['Low'] <= tp_price and entry_score < 10:
                        actual_exit_price = get_tp_price(entry_price, safe_open, DYNAMIC_TP, -1)
                        is_exit = True

                # 反轉 (保持不變)
                if not is_exit:
                    if (direction == 1 and row['Prev_Sell_Score'] == 10) or \
                       (direction == -1 and row['Prev_Buy_Score'] == 10):
                        actual_exit_price = safe_open
                        is_exit = True

                # 結算與紀錄
                if is_exit:
                    # 加上出場滑價 (-direction 巧妙讓多單扣錢，空單加錢)
                    actual_exit_price = apply_slippage(actual_exit_price, -direction, SLIPPAGE)

                    pnl, invested = calculate_pnl(
                        direction,
                        entry_price,
                        actual_exit_price,
                        TRADE_SHARES,
                        exit_fee_rate,
                        p['TAX_RATE']
                    )

                    profit_pct = (pnl / invested) * 100
                    sim_balance += pnl
                    trades.append(profit_pct)

                    # 寫入 SQL 資料庫
                    if db_cursor:
                        try:
                            dir_str = "做多(Long)" if direction == 1 else "放空(Short)"
                            db_cursor.execute('''
                                INSERT INTO backtest_history 
                                ([Ticker SYMBOL], [方向], [進場時間], [出場時間], [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金])
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (ticker, dir_str, entry_date, index, round(entry_price, 2), 
                                  round(actual_exit_price, 2), round(profit_pct, 3), round(pnl, 0), round(sim_balance, 0)))
                            db_conn.commit()
                        except Exception:
                            pass 

                    position = 0
                    direction = 0

        # ==========================================
        # 結算最後一筆未平倉部位
        # ==========================================
        if position != 0:
            final_raw_price = df.iloc[-1]['Close']
            final_slip_price = apply_slippage(final_raw_price, -direction, SLIPPAGE)
            
            pnl, invested = calculate_pnl(
                direction, 
                entry_price, 
                final_slip_price, 
                TRADE_SHARES, 
                exit_fee_rate, 
                p['TAX_RATE']
            )
            
            profit_pct = (pnl / invested) * 100
            sim_balance += pnl
            trades.append(profit_pct)
            
            if db_cursor:
                try:
                    dir_str = "做多(Long)" if direction == 1 else "放空(Short)"
                    db_cursor.execute('''
                        INSERT INTO backtest_history 
                        ([Ticker SYMBOL], [方向], [進場時間], [出場時間], [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (ticker, dir_str, entry_date, df.index[-1], round(entry_price, 2), 
                          round(final_slip_price, 2), round(profit_pct, 3), round(pnl, 0), round(sim_balance, 0)))
                    db_conn.commit()
                except Exception as e:
                    print(f"⚠️ 最後結算寫入 SQL 失敗 ({ticker}): {e}")
                    
        if db_conn:
            db_conn.close()

        # ==========================================
        # 原本只有算勝率跟總報酬，現在加入期望值運算
        # ==========================================
        total_trades = len(trades)
        if total_trades > 0:
            # 1. 區分賺錢與賠錢的單子
            win_trades = [p for p in trades if p > 0]
            loss_trades = [p for p in trades if p <= 0]
            
            # 2. 計算勝率 (小數點格式，用於公式計算)
            win_rate_decimal = len(win_trades) / total_trades
            win_rate = win_rate_decimal * 100  # 這是顯示用的百分比 (例如 60.0)
            
            # 3. 計算平均賺與平均賠
            avg_win = sum(win_trades) / len(win_trades) if win_trades else 0
            avg_loss = sum(loss_trades) / len(loss_trades) if loss_trades else 0
            
            # 🌟 4. 計算期望值
            expected_value = (win_rate_decimal * avg_win) + ((1 - win_rate_decimal) * avg_loss)
            total_profit = sum(trades)
        else:
            win_rate = 0.000
            total_profit = 0.000
            expected_value = 0.000 # 沒交易紀錄就是 0

        # ==========================================
        # 5. 提取狀態
        # ==========================================
        latest_row = df.iloc[-1]
        current_price = latest_row['Close']
        buy_score = int(latest_row['Buy_Score'])
        sell_score = int(latest_row['Sell_Score'])
        
        actual_buy_signals = df['Buy_Score'] >= p['TRIGGER_SCORE']
        actual_sell_signals = df['Sell_Score'] >= p['TRIGGER_SCORE']
        
        buy_details = []
        if buy_trend.iloc[-1]: buy_details.append(f"BBI多頭趨勢(歷{int((buy_trend & actual_buy_signals).sum())}次)")
        if buy_c1.iloc[-1]: buy_details.append(f"破下軌(歷{int((buy_c1 & actual_buy_signals).sum())}次)")
        if buy_c2.iloc[-1]: buy_details.append(f"RSI超賣(歷{int((buy_c2 & actual_buy_signals).sum())}次)")
        if buy_c3.iloc[-1]: buy_details.append(f"爆量(歷{int((buy_c3 & actual_buy_signals).sum())}次)")
        if buy_c4.iloc[-1]: buy_details.append(f"MACD轉強(歷{int((buy_c4 & actual_buy_signals).sum())}次)")
        if buy_c5.iloc[-1]: buy_details.append(f"底背離(歷{int((buy_c5 & actual_buy_signals).sum())}次)")
        if buy_c6.iloc[-1]: buy_details.append(f"🌟突破BBI(歷{int((buy_c6 & actual_buy_signals).sum())}次)") 
        if buy_c7.iloc[-1]: buy_details.append(f"🔥昨日法人同買(歷{int((buy_c7 & actual_buy_signals).sum())}次)") 
        if buy_c8.iloc[-1]: buy_details.append(f"📈DMI趨勢成型(歷{int((buy_c8 & actual_buy_signals).sum())}次)")
        if buy_c9.iloc[-1]: buy_details.append(f"💎結構底背離(歷{int((buy_c9 & actual_buy_signals).sum())}次)")
        
        sell_details = []
        if sell_trend.iloc[-1]: sell_details.append(f"BBI空頭趨勢(歷{int((sell_trend & actual_sell_signals).sum())}次)")
        if sell_c1.iloc[-1]: sell_details.append(f"頂上軌(歷{int((sell_c1 & actual_sell_signals).sum())}次)")
        if sell_c2.iloc[-1]: sell_details.append(f"RSI超買(歷{int((sell_c2 & actual_sell_signals).sum())}次)")
        if sell_c3.iloc[-1]: sell_details.append(f"爆量(歷{int((sell_c3 & actual_sell_signals).sum())}次)")
        if sell_c4.iloc[-1]: sell_details.append(f"MACD轉弱(歷{int((sell_c4 & actual_sell_signals).sum())}次)")
        if sell_c5.iloc[-1]: sell_details.append(f"頂背離(歷{int((sell_c5 & actual_sell_signals).sum())}次)")
        if sell_c6.iloc[-1]: sell_details.append(f"💀跌破BBI(歷{int((sell_c6 & actual_sell_signals).sum())}次)") 
        if sell_c7.iloc[-1]: sell_details.append(f"🧊昨日法人同賣(歷{int((sell_c7 & actual_sell_signals).sum())}次)") 
        if sell_c8.iloc[-1]: sell_details.append(f"📉DMI空頭成型(歷{int((sell_c8 & actual_sell_signals).sum())}次)")
        if sell_c9.iloc[-1]: sell_details.append(f"💣結構頂背離(歷{int((sell_c9 & actual_sell_signals).sum())}次)")
        
        # 🌟 讀取我們在第四層做好的終極標籤
        golden_tag = latest_row.get('Golden_Type', '無')
        trigger_str = "-"

        if buy_score >= 3:
            # 如果有黃金陣型就顯示陣型，沒有就顯示傳統強買訊
            tag_name = golden_tag if golden_tag != "無" else "強買訊"
            status = f"🔴 {tag_name} ({buy_score}/10)"
            trigger_str = " + ".join(buy_details)
        elif sell_score >= 3:   
            tag_name = golden_tag if golden_tag != "無" else "強賣訊"
            status = f"🟢 {tag_name} ({sell_score}/10)"
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

        if latest_row['ADX14'] < p['ADX_TREND_THRESHOLD']:
            trigger_str += " (⚠️ 盤整中，訊號效力減弱)"

        strength_diff = buy_score - sell_score
        structure_status = "多頭佔優" if strength_diff > 2 else "空頭佔優" if strength_diff < -2 else "結構盤整"

        diagnostic_data = {
            "BBI多頭趨勢": [int(buy_trend.sum()), int((buy_trend & actual_buy_signals).sum())],
            "破下軌": [int(buy_c1.sum()), int((buy_c1 & actual_buy_signals).sum())],
            "RSI超賣": [int(buy_c2.sum()), int((buy_c2 & actual_buy_signals).sum())],
            "爆量": [int(buy_c3.sum()), int((buy_c3 & actual_buy_signals).sum())],
            "MACD轉強": [int(buy_c4.sum()), int((buy_c4 & actual_buy_signals).sum())],
            "底背離": [int(buy_c5.sum()), int((buy_c5 & actual_buy_signals).sum())],
            "🌟突破BBI": [int(buy_c6.sum()), int((buy_c6 & actual_buy_signals).sum())],
            "🔥昨日法人同買": [int(buy_c7.sum()), int((buy_c7 & actual_buy_signals).sum())],
            "📈DMI趨勢成型": [int(buy_c8.sum()), int((buy_c8 & actual_buy_signals).sum())],
            "💎結構底背離": [int(buy_c9.sum()), int((buy_c9 & actual_buy_signals).sum())],
            
            "BBI空頭趨勢": [int(sell_trend.sum()), int((sell_trend & actual_sell_signals).sum())],
            "頂上軌":[int(sell_c1.sum()),int((sell_c1 & actual_sell_signals).sum())],
            "RSI超買":[int(sell_c2.sum()),int((sell_c2 & actual_sell_signals).sum())],
            "爆量":[int(sell_c3.sum()),int((sell_c3 & actual_sell_signals).sum())],
            "MACD轉弱":[int(sell_c4.sum()),int((sell_c4 & actual_sell_signals).sum())],
            "頂背離":[int(sell_c5.sum()),int((sell_c5 & actual_sell_signals).sum())],
            "💀跌破BBI":[int(sell_c6.sum()),int((sell_c6 & actual_sell_signals).sum())],
            "🧊昨日法人同賣":[int(sell_c7.sum()),int((sell_c7 & actual_sell_signals).sum())],
            "📉DMI空頭成型":[int(sell_c8.sum()),int((sell_c8 & actual_sell_signals).sum())],
            "💣結構頂背離":[int(sell_c9.sum()),int((sell_c9 & actual_sell_signals).sum())]
        }

        f_data = add_fundamental_filter(ticker)
        
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
            "期望值": round(expected_value, 3),
            "診斷數據": diagnostic_data,  
            "計算後資料": df             
        }

    except Exception as e:
        print(f"檢測 {ticker} 時發生錯誤: {e}")
        return None

# ==========================================
# 🚀 批次海選發動機 
# ==========================================
if __name__ == "__main__":

    test_targets = [
    # 權值與趨勢
    "2330.TW", "2454.TW", "2317.TW", "2303.TW", "2308.TW",
    # AI 伺服器
    "2382.TW", "3231.TW"
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
                draw_chart(
                    ticker, 
                    preloaded_df=ticker_df, 
                    win_rate=result["系統勝率(%)"], 
                    total_profit=result["累計報酬率(%)"],
                    expected_value=result["期望值"] # 🌟 把這顆球丟過去！
                )

    # ==========================================
    # 👇 最終報表輸出與高階診斷 (終極二合一版) 👇
    # ==========================================
    if report_cards:
        # --- 1. 市場氣候與戰略資金配置 ---
        total_count = len(report_cards)
        bull_count = len([r for r in report_cards if int(r.get("結構強度", 0)) > 2])
        bear_count = len([r for r in report_cards if int(r.get("結構強度", 0)) < -2])
        
        trend_intensity = max(bull_count, bear_count) / total_count if total_count > 0 else 0
        
        # 讀取參數表的預算設定
        TOTAL_BUDGET = PARAMS.get("TOTAL_BUDGET", 1000000)    
        MAX_POSITIONS = PARAMS.get("MAX_POSITIONS", 5)         
        base_allocation = TOTAL_BUDGET / MAX_POSITIONS

        if trend_intensity >= 0.600:
            risk_factor = 1.000
            market_msg = "🔥 趨勢極度明顯，全速執行單邊或對鎖策略。"
        elif trend_intensity >= 0.400:
            risk_factor = 0.600
            market_msg = "🌤️ 趨勢尚可，部位維持 60% 運行。"
        else:
            risk_factor = 0.300
            market_msg = "🌪️ 多空勢均力敵（盤整），自動降載至 30% 嚴防雙巴。"

        # --- 2. 綜合評分計算 (融合勝率、報酬率與基本面加權) ---
        for r in report_cards:
            win_rate = float(r["系統勝率(%)"]) / 100
            total_ret = abs(float(r["累計報酬率(%)"])) 
            f_weight = int(r["基本面總分"]) * 2
            
            # 給予基本面順風車獎勵
            bonus = f_weight if "買訊" in r["今日系統燈號"] else -f_weight if "賣訊" in r["今日系統燈號"] else 0
            r["期望值評分"] = round((total_ret * win_rate) + bonus, 3)

        long_candidates = [r for r in report_cards if "買訊" in r["今日系統燈號"]]
        short_candidates = [r for r in report_cards if "賣訊" in r["今日系統燈號"]]

        top_longs = sorted(long_candidates, key=lambda x: x.get("期望值評分", 0), reverse=True)
        top_shorts = sorted(short_candidates, key=lambda x: x.get("期望值評分", 0), reverse=True)
        all_ranked = sorted([r for r in report_cards if "觀望" not in r["今日系統燈號"]], key=lambda x: x.get('期望值評分', 0), reverse=True)

        # --- 3. 印出雙向戰略配置報告 ---
        print("\n" + "═"*30 + " ⚔️ 雙向戰略配置報告 " + "═"*30)
        print(f"📈 多頭比例：{bull_count/total_count:.1%} | 📉 空頭比例：{bear_count/total_count:.1%}")
        print(f"📊 趨勢強度：{trend_intensity:.3f} | 🛡️ 風險狀態：{market_msg}")
        print(f"💵 建議單筆限額：${(base_allocation * risk_factor):,.0f}")
        print("-" * 83)
        
        if top_longs:
            print(f"🚩 【作多首選】: {top_longs[0]['Ticker SYMBOL']} (評分: {top_longs[0]['期望值評分']:.3f})")
        if top_shorts:
            print(f"🏳️ 【放空首選】: {top_shorts[0]['Ticker SYMBOL']} (評分: {top_shorts[0]['期望值評分']:.3f})")
            
        print("-" * 83)
        if not all_ranked:
            print("📭 市場方向不明且無強烈訊號，建議空手觀望。")
        else:
            print(f"🎯 綜合排序前 {MAX_POSITIONS} 名進場建議：")
            for i, stock in enumerate(all_ranked[:MAX_POSITIONS]):
                direction = "🔴 做多" if "買訊" in stock["今日系統燈號"] else "🟢 放空"
                print(f"  {i+1}. {stock['Ticker SYMBOL']} | {direction} | 期望評分: {stock['期望值評分']:.3f} | 建議配置: ${(base_allocation * risk_factor):,.0f}")
        print("═"*83)

        # --- 4. 印出今日海選總表 ---
        final_report = pd.DataFrame(report_cards)
        pd.set_option('display.unicode.east_asian_width', True) 
        
        print("\n" + "="*25 + " 今日海選總表 " + "="*25)
        display_cols = [
            "Ticker SYMBOL", "最新收盤價", "結構強度", "今日系統燈號", 
            "結構診斷", "基本面總分", "營收年增率(%)", "營業利益率(%)", 
            "系統勝率(%)", "累計報酬率(%)", "期望值評分", "觸發條件明細"
        ]
        
        actual_cols = [col for col in display_cols if col in final_report.columns]
        print(final_report[actual_cols].to_string(index=False))
        print("="*75)

        # --- 5. 印出指標戰力分佈報告 ---
        all_condition_keys = [
            "BBI多頭趨勢", "破下軌", "RSI超賣", "爆量", "MACD轉強", 
            "底背離", "🌟突破BBI", "🔥昨日法人同買", "📈DMI趨勢成型", "💎結構底背離",
            "BBI空頭趨勢", "頂上軌", "RSI超買", "爆量", "MACD轉弱", 
            "頂背離", "💀跌破BBI", "🧊昨日法人同賣", "📉DMI空頭成型", "💣結構頂背離"
        ]
        
        global_stats = {key: 0 for key in all_condition_keys}
        for card in report_cards:
            diag = card.get("診斷數據", {})
            for cond_name in all_condition_keys:
                if cond_name in diag:
                    global_stats[cond_name] += diag[cond_name][1]

        today_active_set = set()
        for card in report_cards:
            detail = str(card.get("觸發條件明細", ""))
            for key in all_condition_keys:
                if key in detail:
                    today_active_set.add(key)

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
        print("💡 註：歷史助攻次數是指在『總分 ≥ 3』時，該指標出現在獲勝組合中的次數。")

    else:
        print("掃描失敗，無資料輸出。")