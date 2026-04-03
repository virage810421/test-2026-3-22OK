import yfinance as yf
import os
from datetime import datetime
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import numpy as np
import pyodbc 
from advanced_chart import draw_chart
from FinMind.data import DataLoader
from config import PARAMS
from strategies import get_active_strategy  # 🌟 讓回測引擎也能呼叫四大艦隊
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='sklearn')
warnings.filterwarnings('ignore', category=ResourceWarning)
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
warnings.filterwarnings('ignore', message=".*scikit-learn configuration.*")
# ==========================================
# ⚡️ 初始化 DataLoader 與資料庫連線設定
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wNC0wMSAxMDo1MDoyOCIsInVzZXJfaWQiOiJob25kYSIsImlwIjoiMTI1LjIyNC4xNDguMjM2IiwiZXhwIjoxNzc1NjE2NjI4fQ.803KvO4-3l9K0lDcfzAoTGl78i-YUIrBX75useoNq_Q"
dl = DataLoader(token=API_TOKEN)

DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)


# ==========================================
# 🚀 智慧快取下載器 (Smart Cache Engine) - 終極假日記憶版
# ==========================================
def smart_download(ticker, period="1y"):
    """
    一天只抓一次 API，其餘時間秒讀快取！
    假日自動讀取週五快取，達成真正的零消耗！
    """
    os.makedirs("data/kline_cache", exist_ok=True)
    cache_file = f"data/kline_cache/{ticker}_{period}.csv"
    
    today = datetime.now().date()
    is_weekend = today.weekday() >= 5 # 5是週六，6是週日
    
    # 1. 檢查快取
    if os.path.exists(cache_file):
        file_mtime_date = datetime.fromtimestamp(os.path.getmtime(cache_file)).date()
        days_diff = (today - file_mtime_date).days
        
        # 🌟 核心防禦：如果是今天抓的，或者是「週末且快取是3天內的(週五)」，直接秒讀！
        if days_diff == 0 or (is_weekend and days_diff <= 3):
            return pd.read_csv(cache_file, index_col=0, parse_dates=True)
            
    # 2. 如果真的沒有或過期，才向 API 請求新資料
    try:
        data = yf.download(ticker, period=period, progress=False)
        if data.empty: 
            return pd.DataFrame()
            
        df = data.xs(ticker, axis=1, level=1).copy() if isinstance(data.columns, pd.MultiIndex) else data.copy()
        df.to_csv(cache_file)
        return df
    except Exception as e:
        print(f"⚠️ {ticker} 網路下載失敗: {e}")
        if os.path.exists(cache_file):
            print(f"♻️ 啟用備用方案：讀取 {ticker} 舊有快取資料。")
            return pd.read_csv(cache_file, index_col=0, parse_dates=True)
        return pd.DataFrame()


def extract_ai_features(row):
    """
    確保『兵工廠訓練』跟『實戰機台』看到的數值 100% 相同。
    """
    features = {}
    
    # 1. 浮動式連續數值 (基礎環境)
    ma20 = row.get('MA20', 0)
    # 🌟 終極防呆：分母加上 0.001 避免除以零
    features['BB_Width'] = (row.get('BB_Upper', 0) - row.get('BB_Lower', 0)) / (ma20 + 0.001) if ma20 != 0 else 0
    
    vol_ma20 = row.get('Vol_MA20', 0)
    # 🌟 終極防呆：分母加上 0.001 避免除以零
    features['Volume_Ratio'] = row.get('Volume', 0) / (vol_ma20 + 0.001) if vol_ma20 != 0 else 1
    
    features['RSI'] = row.get('RSI', 50)
    features['MACD_Hist'] = row.get('MACD_Hist', 0)
    features['ADX'] = row.get('ADX14', 25)

    features['Foreign_Net'] = row.get('Foreign_Net', 0)
    features['Trust_Net'] = row.get('Trust_Net', 0)
    
    # ==========================================
    # 2. 🟢 多方戰術開關 (做多市場語言)
    # ==========================================
    features['Buy_RSI_Oversold_超賣'] = int(row.get('buy_c2', 0))
    features['Buy_Vol_Spike_爆量'] = int(row.get('buy_c3', 0))
    features['Buy_MACD_Strong_轉強'] = int(row.get('buy_c4', 0))
    features['Buy_Price_Div_底背離'] = int(row.get('buy_c5', 0))
    features['Buy_BBI_Breakout_突破BBI'] = int(row.get('buy_c6', 0))
    features['Buy_Smart_Money_法人同買'] = int(row.get('buy_c7', 0))
    features['Buy_DMI_Trend_多頭成型'] = int(row.get('buy_c8', 0))
    features['Buy_Chip_Div_結構底背離'] = int(row.get('buy_c9', 0))
    
    # ==========================================
    # 3. 🔴 空方戰術開關 (放空市場語言)
    # ==========================================
    features['Sell_RSI_Overbought_超買'] = int(row.get('sell_c2', 0))
    features['Sell_Vol_Spike_爆量下殺'] = int(row.get('sell_c3', 0))
    features['Sell_MACD_Weak_轉弱'] = int(row.get('sell_c4', 0))
    features['Sell_Price_Div_頂背離'] = int(row.get('sell_c5', 0))
    features['Sell_BBI_Breakdown_跌破BBI'] = int(row.get('sell_c6', 0))
    features['Sell_Smart_Money_法人同賣'] = int(row.get('sell_c7', 0))
    features['Sell_DMI_Trend_空頭成型'] = int(row.get('sell_c8', 0))
    features['Sell_Chip_Div_結構頂背離'] = int(row.get('sell_c9', 0))
   
    # 假突破給 1，假跌破給 -1，正常給 0
    features['Trap_假突破'] = 1 if row.get('Fake_Breakout', False) else (-1 if row.get('Bear_Trap', False) else 0)
    
    # 壓縮與吸籌 (0或1)
    features['Vol_Squeeze_壓縮'] = int(row.get('Vol_Squeeze', False))
    features['Absorption_吸籌'] = int(row.get('Absorption', False))
    
    # FOMO(散戶追高)給 1，恐慌(散戶人踩人)給 -1，正常給 0
    features['Emotion_情緒'] = 1 if row.get('FOMO', False) else (-1 if row.get('Panic', False) else 0)
    
    # 趨勢持續力 (0~5天)
    features['Up_Days_5_連漲天數'] = row.get('Up_Days_5', 0)
    
    return features


# ==========================================
# 🔌 籌碼資料外掛模組 (本地 SQL 直連版 - 零 API 消耗)
# ==========================================
def add_chip_data(df, ticker):
    """
    負責從本地 SQL 資料庫 (daily_chip_data) 讀取三大法人買賣超資料，
    並完美貼上 yfinance 的價格表。
    """
    try:
        # 🌟 直接向 SQL Server 請求該股票的歷史籌碼
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = "SELECT [日期], [外資買賣超], [投信買賣超], [自營商買賣超] FROM daily_chip_data WHERE [Ticker SYMBOL] = ?"
            chip_df = pd.read_sql(query, conn, params=(ticker,))
            
        # 如果資料庫裡還沒有這檔股票的籌碼，直接給 0
        if chip_df.empty:
            df['Foreign_Net'], df['Trust_Net'], df['Dealers_Net'] = 0, 0, 0
            return df
            
        # 🌟 整理 SQL 撈出來的資料，將日期設為索引
        chip_df['日期'] = pd.to_datetime(chip_df['日期']).dt.normalize()
        chip_df.set_index('日期', inplace=True)
        
        # 🌟 絕對強制的日期對齊術 (解決與 yfinance 合併變 0 的問題)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df.index = pd.to_datetime(df.index).normalize()

        # 🌟 將三大法人併入主 DataFrame (重新命名為系統認識的英文欄位)
        df = df.join(chip_df['外資買賣超'].rename('Foreign_Net'), how='left')
        df = df.join(chip_df['投信買賣超'].rename('Trust_Net'), how='left')
        df = df.join(chip_df['自營商買賣超'].rename('Dealers_Net'), how='left')

        # 空白的日子 (例如假日或沒開盤) 填補 0
        df['Foreign_Net'] = df['Foreign_Net'].ffill().fillna(0)
        df['Trust_Net'] = df['Trust_Net'].ffill().fillna(0)
        df['Dealers_Net'] = df['Dealers_Net'].ffill().fillna(0)
        
    except Exception as e:
        print(f"⚠️ {ticker} 本地籌碼庫讀取失敗: {e}")
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
        
        # ✨ 疫苗 3：終極除蟲防線，在計算任何指標前，把沒有收盤價的無效 K 線直接剃除！
        df.dropna(subset=['Close'], inplace=True)
        df.ffill(inplace=True)
        if df.empty: return None
        
        
        # 1. 基礎指標計算 (RSI)
        delta = df['Close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.ewm(alpha=1/p['RSI_PERIOD'], min_periods=p['RSI_PERIOD'], adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/p['RSI_PERIOD'], min_periods=p['RSI_PERIOD'], adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['RSI'] = np.where(avg_loss == 0, 100, 100 - (100 / (1 + rs)))
        
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
        # 🌟 修復：補上這個被遺忘的通道寬度零件，讓第 18 把武器能正常運作！
        df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['MA20']
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
        # 🌟 第 17~21 把特種武器擴充 (微結構與情緒)
        # ==========================================
        # 17. 假突破/假跌破 (Trap - 無未來函數版：今天確認昨天是假的)
        prev_breakout_up = df['Close'].shift(1) > df['High'].shift(2)
        df['Fake_Breakout'] = prev_breakout_up & (df['Close'] < df['Low'].shift(1))
        
        prev_breakout_down = df['Close'].shift(1) < df['Low'].shift(2)
        df['Bear_Trap'] = prev_breakout_down & (df['Close'] > df['High'].shift(1))

        # 18. 波動壓縮艙 (Vol Squeeze - 寬度低於近期平均 80%)
        df['BB_Width_MA20'] = df['BB_Width'].rolling(20).mean()
        df['Vol_Squeeze'] = df['BB_Width'] < (df['BB_Width_MA20'] * 0.8)

        # 19. 主力吸籌探測 (Absorption - 爆量但K線實體極短)
        df['Price_Range_Pct'] = (df['High'] - df['Low']) / df['Close']
        df['Absorption'] = (df['Volume'] > df['Vol_MA20'] * 1.5) & (df['Price_Range_Pct'] < 0.015)

        # 20. 極端情緒 (FOMO / Panic - 單日暴漲暴跌且爆量)
        df['Daily_Return'] = df['Close'].pct_change()
        df['FOMO'] = (df['Daily_Return'] > 0.04) & (df['Volume'] > df['Vol_MA20'] * 1.5)
        df['Panic'] = (df['Daily_Return'] < -0.04) & (df['Volume'] > df['Vol_MA20'] * 1.5)

        # 21. 趨勢連漲力 (Up Days - 近5日有幾天收紅)
        df['Up_Days_5'] = (df['Close'] > df['Close'].shift(1)).astype(int).rolling(5).sum()
        # 👇 🌟 新增這段：把物理開關正式焊死進 DataFrame，讓 AI 晶片抓得到！
        # ==========================================
        df['buy_c1'] = buy_c1; df['buy_c2'] = buy_c2; df['buy_c3'] = buy_c3
        df['buy_c4'] = buy_c4; df['buy_c5'] = buy_c5; df['buy_c6'] = buy_c6
        df['buy_c7'] = buy_c7; df['buy_c8'] = buy_c8; df['buy_c9'] = buy_c9
        
        df['sell_c1'] = sell_c1; df['sell_c2'] = sell_c2; df['sell_c3'] = sell_c3
        df['sell_c4'] = sell_c4; df['sell_c5'] = sell_c5; df['sell_c6'] = sell_c6
        df['sell_c7'] = sell_c7; df['sell_c8'] = sell_c8; df['sell_c9'] = sell_c9
       # ==========================================
        # 🚀 階段 2：市場語言 (Market Language) - 直接寫入 DataFrame
        # ==========================================
        is_bull_regime = (df['Regime'] == '趨勢多頭')
        is_bear_regime = (df['Regime'] == '趨勢空頭')
        is_ranging = (df['Regime'] == '區間盤整')

        # 🟢 多方語言：直接生成 df 欄位，供給 AI 特徵晶片讀取！
        df['buy_vol_spike'] = buy_c3 | buy_c3.shift(1)       
        df['buy_smart_money'] = buy_c7 | buy_c7.shift(1)     
        df['buy_price_break'] = buy_c6                       
        df['buy_oversold'] = buy_c1 & buy_c2                 
        df['buy_chip_diverge'] = buy_c9 & buy_c4 & buy_trend 

        # 🔴 空方語言
        df['sell_vol_spike'] = sell_c3 | sell_c3.shift(1)
        df['sell_smart_money'] = sell_c7 | sell_c7.shift(1)
        df['sell_price_breakdown'] = sell_c6
        df['sell_overbought'] = sell_c1 & sell_c2
        df['sell_chip_diverge'] = sell_c9 & sell_c4 & sell_trend

        # ==========================================
        # 🚀 階段 3：訊號陣型組合 (Setup)
        # ==========================================
        # 直接使用剛寫入 df 的語言特徵來組合陣型
        setup_breakout_long = (df['buy_smart_money'].astype(int) + df['buy_vol_spike'].astype(int) + df['buy_price_break'].astype(int) >= 2) & (is_bull_regime | is_ranging)
        setup_reversal_long = df['buy_oversold'] & is_ranging
        setup_chip_long = df['buy_chip_diverge'] & (is_ranging | is_bear_regime)

        setup_breakout_short = (df['sell_smart_money'].astype(int) + df['sell_vol_spike'].astype(int) + df['sell_price_breakdown'].astype(int) >= 2) & (is_bear_regime | is_ranging)
        setup_reversal_short = df['sell_overbought'] & is_ranging
        setup_chip_short = df['sell_chip_diverge'] & (is_ranging | is_bull_regime)
        # ==========================================
        # 🚀 階段 4：扣板機觸發 (Trigger) 與 輸出訊號 (Signal)
        # ==========================================
        trigger_long = df['Close'] > df['High'].shift(1)  # 今天過昨高，確認發動
        trigger_short = df['Close'] < df['Low'].shift(1)  # 今天破昨低，確認發動

        final_long_breakout = setup_breakout_long.shift(1).fillna(False) & trigger_long
        final_long_reversal = setup_reversal_long.shift(1).fillna(False) & trigger_long
        final_long_chip = setup_chip_long.shift(1).fillna(False) & trigger_long

        final_short_breakout = setup_breakout_short.shift(1).fillna(False) & trigger_short
        final_short_reversal = setup_reversal_short.shift(1).fillna(False) & trigger_short
        final_short_chip = setup_chip_short.shift(1).fillna(False) & trigger_short

        # 🌟 輸出 Golden_Type 供給 ML 特徵晶片與倉位系統使用！
        if not p.get('ALLOW_LONG', True):
            final_long_breakout, final_long_reversal, final_long_chip = False, False, False
        if not p.get('ALLOW_SHORT', True):
            final_short_breakout, final_short_reversal, final_short_chip = False, False, False

        df['Golden_Type'] = np.where(final_long_breakout, "BREAKOUT_LONG", 
                            np.where(final_long_reversal, "REVERSAL_LONG", 
                            np.where(final_long_chip, "CHIP_LONG", 
                            np.where(final_short_breakout, "BREAKOUT_SHORT",
                            np.where(final_short_reversal, "REVERSAL_SHORT",
                            np.where(final_short_chip, "CHIP_SHORT", "無"))))))

        # ==========================================
        # 🚀 階段 4.5：維持戰鬥力儀表板 (保留分數，僅供報表顯示使用)
        # ==========================================
        df['Buy_Score'] = (buy_trend.astype(int) + buy_c1.astype(int) + buy_c2.astype(int) + 
                          buy_c3.astype(int) + buy_c4.astype(int) + buy_c5.astype(int) + 
                          buy_c6.astype(int) + buy_c7.astype(int) + buy_c8.astype(int) + buy_c9.astype(int))
        
        df['Sell_Score'] = (sell_trend.astype(int) + sell_c1.astype(int) + sell_c2.astype(int) + 
                           sell_c3.astype(int) + sell_c4.astype(int) + sell_c5.astype(int) + 
                           sell_c6.astype(int) + sell_c7.astype(int) + sell_c8.astype(int) + sell_c9.astype(int))
        
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
        df['Prev_Golden_Type'] = df['Golden_Type'].shift(1) # ✨ 新增：記住昨天的陣型
        df['Prev_Regime'] = df['Regime'].shift(1)           # ✨ 新增：記住昨天的市場狀態

        for index, row in df.iterrows():
            if pd.isna(row['Prev_Buy_Score']):
                continue

            safe_open = row['Open'] if row['Open'] > 0 else 0.0001
            safe_close = row['Close'] if row['Close'] > 0 else 0.0001

           # ====================
            # 🌟 Layer 1 升級：結構式進場 (分數降級，陣型為王)
            # ====================
            if position == 0:
                prev_setup = row['Prev_Golden_Type']
                prev_buy_score = row['Prev_Buy_Score']
                prev_sell_score = row['Prev_Sell_Score']
                
                # 🌟 嚴格紀律：沒有陣型，不管分數多高都強制觀望，直接跳過！
                if prev_setup == "無":
                    continue

                if prev_setup in ["BREAKOUT_LONG", "REVERSAL_LONG", "CHIP_LONG", "TREND_LONG"]:
                    temp_direction = 1
                    # 🎯 狙擊發動：有陣型，且戰力高達 7 分以上，升級為狙擊武器！
                    if prev_buy_score >= 7: 
                        prev_setup = "SNIPER_黃金狙擊"
                        
                elif prev_setup in ["BREAKOUT_SHORT", "REVERSAL_SHORT", "CHIP_SHORT", "TREND_SHORT"]:
                    temp_direction = -1
                    if prev_sell_score >= 7: 
                        prev_setup = "SNIPER_黃金狙擊"
                else:
                    continue
                
                # 2. 預先試算進場價與風險 (Risk: 停損距)
                temp_entry_price = apply_slippage(safe_open, temp_direction, SLIPPAGE)
                volatility_pct = (row['BB_std'] * 1.5) / safe_close
                if pd.isna(volatility_pct): volatility_pct = p['SL_MAX_PCT'] # 🛡️ 防止 NaN 感染
                temp_sl_pct = max(p['SL_MIN_PCT'], min(volatility_pct, p['SL_MAX_PCT']))
                
                # 3. 預先試算預期報酬 (Reward: 停利距)
                temp_trend_is_bull = row['Prev_Trend']
                trend_is_with_me = (temp_direction == 1 and temp_trend_is_bull) or (temp_direction == -1 and not temp_trend_is_bull)
                adx = row['ADX14'] if not pd.isna(row['ADX14']) else 0
                temp_tp_pct = p['TP_TREND_PCT'] if (trend_is_with_me and adx > p['ADX_TREND_THRESHOLD']) else p['TP_BASE_PCT']
                
                # ✨ 4. 風報比 (RR) 核心濾網
                rr_ratio = temp_tp_pct / temp_sl_pct
                if rr_ratio < p.get('MIN_RR_RATIO', 1.5):  
                    # 💡 拒絕交易：如果潛在獲利沒有停損風險的 1.5 倍，訊號再好也不做！
                    continue

                # ==========================================
                # ✅ 通過所有考驗，正式進場！
                # ==========================================
                direction = temp_direction
                position = direction
                entry_price = temp_entry_price
                entry_date = index
                entry_score = row['Prev_Buy_Score'] if direction == 1 else row['Prev_Sell_Score']
                entry_trend_is_bull = temp_trend_is_bull
                max_reached_price = entry_price 
                
                # ✨ 記憶進場時的歸因標籤 (供出場寫入資料庫用)
                entry_regime = row['Prev_Regime']
                entry_setup = row['Prev_Golden_Type'] if row['Prev_Golden_Type'] != "無" else "傳統訊號"
                entry_rr = rr_ratio
                entry_sl_record = temp_sl_pct
                entry_tp_record = temp_tp_pct
                
                # 🧠 AI 精算師：動態信心權重 + 流動性微結構過濾
                base_risk_allowance = sim_balance * 0.015 
                
                # 🧠 最佳化防凍結機制：如果是跑 Optimizer，暫時關閉 AI 算力，純測物理指標！
                if p.get('IS_OPTIMIZING', False):
                    conviction_mult = 2.0 if "SNIPER" in entry_setup else 1.0
                else:
                    active_strategy_bt = get_active_strategy(entry_setup)
                    conviction_mult = active_strategy_bt.get_conviction_multiplier(row, entry_regime)
                    
                target_risk = base_risk_allowance * conviction_mult
                raw_shares = target_risk / (entry_price * temp_sl_pct)
                
                # 2. 流動性天花板：單筆買入不得超過該股票近 20 日均量的 5%
                max_liquidity_shares = (row.get('Vol_MA20', 1000) * 1000) * 0.05
                raw_shares = min(raw_shares, max_liquidity_shares)
                
                # 3. 資金天花板：最多只能用掉帳戶總額的 33%
                max_affordable_shares = int((sim_balance * 0.33) / entry_price)
                raw_shares = min(raw_shares, max_affordable_shares)
                
                # 4. 零股/整張最佳化
                if raw_shares >= 1000:
                    TRADE_SHARES = int(raw_shares // 1000) * 1000
                else:
                    TRADE_SHARES = max(1, int(raw_shares))
                
                # 計算實際承擔的風險金額
                entry_risk_amount = TRADE_SHARES * entry_price * temp_sl_pct
            # ====================
            # 🌟 Layer 1 升級：跟策略綁定的智能出場
            # ====================
            else:
                volatility_pct = (row['BB_std'] * 1.5) / safe_close
                if pd.isna(volatility_pct): volatility_pct = p['SL_MAX_PCT'] # 🛡️ 防止 NaN 感染
                
                # ✨ 補上變數定義，消除底線報錯
                trend_is_with_me = (direction == 1 and entry_trend_is_bull) or (direction == -1 and not entry_trend_is_bull)
                adx_val = row['ADX14'] if not pd.isna(row['ADX14']) else 0
                adx_is_strong = adx_val > p.get('ADX_TREND_THRESHOLD', 20)
                
                # ✨ 模組化出場邏輯接管：直接呼叫 strategies.py，確保訓練與實戰 100% 邏輯一致！
                active_strategy = get_active_strategy(entry_setup)
                DYNAMIC_SL, DYNAMIC_TP, ignore_tp = active_strategy.get_exit_rules(
                    p, volatility_pct, trend_is_with_me, adx_is_strong, entry_score
                )

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
                    elif row['High'] >= tp_price and entry_score < 10 and not ignore_tp: 
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
                    elif row['Low'] <= tp_price and entry_score < 10 and not ignore_tp:
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
                    # 範例邏輯：根據當前的市場狀態給予不同名稱
                    current_regime = row['Regime'] 

                    if current_regime == "趨勢多頭":
                        current_strategy_name = "強勢多頭攻堅"
                    elif current_regime == "趨勢空頭":
                        current_strategy_name = "空頭防禦撤退"
                    elif current_regime == "區間盤整":
                        current_strategy_name = "區間震盪低買"
                    else:
                        current_strategy_name = "動態防禦" # 預設備用名稱
                    # ✨ 寫入 SQL 資料庫 (擴充歸因欄位)
                    if db_cursor:
                        try:
                            dir_str = "做多(Long)" if direction == 1 else "放空(Short)"
                            strategy_name = "結構式風控策略"
                            
                            db_cursor.execute('''
                                INSERT INTO backtest_history 
                                ([策略名稱], [Ticker SYMBOL], [方向], [進場時間], [出場時間], 
                                 [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金],
                                 [市場狀態], [進場陣型], [期望值], [預期停損(%)], [預期停利(%)], [風報比(RR)], [風險金額])
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                strategy_name, ticker, dir_str, entry_date, index, 
                                round(entry_price, 2), round(actual_exit_price, 2), round(profit_pct, 3), round(pnl, 0), round(sim_balance, 0),
                                entry_regime, entry_setup, 0.0, round(entry_sl_record*100, 2), round(entry_tp_record*100, 2), round(entry_rr, 2), round(entry_risk_amount, 0)
                            ))
                            db_conn.commit()
                        except Exception as e:
                            print(f"寫入資料庫失敗: {e}")

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
            
            # ✨ 最後結算寫入 SQL (擴充歸因欄位)
            if db_cursor:
                try:
                    dir_str = "做多(Long)" if direction == 1 else "放空(Short)"
                    strategy_name = "結構式風控策略"
                    
                    db_cursor.execute('''
                        INSERT INTO backtest_history 
                        ([策略名稱], [Ticker SYMBOL], [方向], [進場時間], [出場時間], 
                         [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金],
                         [市場狀態], [進場陣型], [期望值], [預期停損(%)], [預期停利(%)], [風報比(RR)], [風險金額])
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        strategy_name, ticker, dir_str, entry_date, df.index[-1], 
                        round(entry_price, 2), round(final_slip_price, 2), round(profit_pct, 3), round(pnl, 0), round(sim_balance, 0),
                        entry_regime, entry_setup, 0.0, round(entry_sl_record*100, 2), round(entry_tp_record*100, 2), round(entry_rr, 2), round(entry_risk_amount, 0)
                    ))
                    db_conn.commit()
                except Exception as e:
                    print(f"⚠️ 最後結算寫入 SQL 失敗 ({ticker}): {e}")
                    
        if db_conn:
            db_conn.close()

     
        # ==========================================
        # 🌟 升級：期望值運算與凱利公式 (Kelly Criterion)
        # ==========================================
        total_trades = len(trades)
        if total_trades > 0:
            win_trades = [p for p in trades if p > 0]
            loss_trades = [p for p in trades if p <= 0]
            
            win_rate_decimal = len(win_trades) / total_trades
            win_rate = win_rate_decimal * 100  
            
            avg_win = sum(win_trades) / len(win_trades) if win_trades else 0
            avg_loss = abs(sum(loss_trades) / len(loss_trades)) if loss_trades else 1 # 取絕對值並防呆
            
            expected_value = (win_rate_decimal * avg_win) - ((1 - win_rate_decimal) * avg_loss)
            total_profit = sum(trades)
            
            # 🧠 凱利公式計算：f* = (bp - q) / b = p - (q / b)
            # p = 勝率, q = 敗率, b = 賠率 (平均獲利 / 平均虧損)
            reward_risk_ratio = avg_win / avg_loss if avg_loss != 0 else 0
            
            if reward_risk_ratio > 0 and expected_value > 0:
                kelly_fraction = win_rate_decimal - ((1 - win_rate_decimal) / reward_risk_ratio)
            else:
                kelly_fraction = 0
                
            # 🛡️ 機構級風控：使用半凱利 (Half-Kelly) 降低波動，並強制規定單檔上限為總資金的 30%
            safe_kelly = max(0, kelly_fraction * 0.5)
            suggested_position = min(0.30, safe_kelly)
            
        else:
            win_rate = 0.000
            total_profit = 0.000
            expected_value = 0.000 
            suggested_position = 0.000 # 沒戰績就不給建議倉位

        # ==========================================
        # 5. 提取狀態
        # ==========================================
        latest_row = df.iloc[-1]
        current_price = latest_row['Close']
        buy_score = int(latest_row['Buy_Score'])
        sell_score = int(latest_row['Sell_Score'])
        
        actual_buy_signals = df['Buy_Score'] >= p['TRIGGER_SCORE']
        actual_sell_signals = df['Sell_Score'] >= p['TRIGGER_SCORE']
        
        # ==========================================
        # 5. 提取狀態與升級版儀表板輸出
        # ==========================================
        latest_row = df.iloc[-1]
        current_price = latest_row['Close']
        buy_score = int(latest_row['Buy_Score'])
        sell_score = int(latest_row['Sell_Score'])
        
        actual_buy_signals = df['Buy_Score'] >= p['TRIGGER_SCORE']
        actual_sell_signals = df['Sell_Score'] >= p['TRIGGER_SCORE']
        
        # 🌟 升級版：使用高階市場語言輸出觸發明細
        buy_details = []
        if latest_row['buy_vol_spike']: buy_details.append("🔥爆量發動")
        if latest_row['buy_smart_money']: buy_details.append("🐋主力進駐")
        if latest_row['buy_price_break']: buy_details.append("📈突破均線")
        if latest_row['buy_oversold']: buy_details.append("🩸極度超賣")
        if latest_row['buy_chip_diverge']: buy_details.append("💎籌碼底背離")
        
        sell_details = []
        if latest_row['sell_vol_spike']: sell_details.append("🧊爆量下殺")
        if latest_row['sell_smart_money']: sell_details.append("💀主力倒貨")
        if latest_row['sell_price_breakdown']: sell_details.append("📉跌破均線")
        if latest_row['sell_overbought']: sell_details.append("🔥極度超買")
        if latest_row['sell_chip_diverge']: sell_details.append("💣籌碼頂背離")
        
        # ==========================================
        # 🎯 雷達兵：四大艦隊陣型判定與狙擊發動
        # ==========================================
        latest = df.iloc[-1]
        
        is_chip_driven = latest.get('Foreign_Net', 0) > 0 and latest.get('Trust_Net', 0) > 0 
        vol_multiplier = p.get('VOL_BREAKOUT_MULTIPLIER', 1.5)
        is_breakout = latest.get('Volume', 0) > (latest.get('Vol_MA20', latest.get('Volume', 0)) * vol_multiplier)
        is_reversal = latest.get('Close', 0) <= latest.get('BB_Lower', 0) or latest.get('RSI', 50) < 30
        adx_threshold = p.get('ADX_TREND_THRESHOLD', 20)
        is_trend = latest.get('ADX14', 0) > adx_threshold and latest.get('Close', 0) > latest.get('BBI', 0)

        # 🌟 1. 分數降級：現在分數只用來顯示「戰鬥力」，不再具有發動交易的權限
        max_score = max(buy_score, sell_score)
        trigger_str = " + ".join(buy_details) if buy_score > sell_score else " + ".join(sell_details)
        if not trigger_str: trigger_str = "無"

        # 🌟 2. 狙擊發動條件：必須有陣型成立，且「戰鬥力高達 7 分以上」，自動升級為狙擊武器！
        has_formation = is_chip_driven or is_breakout or is_reversal or is_trend
        is_sniper_target = has_formation and (max_score >= 7)

        # 🏷️ 3. 階梯式貼標籤
        setup_tag = "無陣型"
        if is_sniper_target:
            setup_tag = "SNIPER_黃金狙擊"  # 觸發 strategies.py 的狙擊武器
        elif is_chip_driven:
            setup_tag = "CHIP_法人籌碼"      
        elif is_breakout:
            setup_tag = "BREAKOUT_帶量突破"  
        elif is_reversal:
            setup_tag = "REVERSAL_均值回歸"  
        elif is_trend:
            setup_tag = "TREND_順勢多頭"     

        # 🌟 4. 狀態燈號生成：加入買賣訊關鍵字，讓實戰機台能聽懂指令！
        if setup_tag != "無陣型":
            if buy_score > sell_score:
                status = f"🔴 {setup_tag} 買訊 (戰力: {max_score}/10)"
            else:
                status = f"🟢 {setup_tag} 賣訊 (戰力: {max_score}/10)"
        else:
            status = f"⚪ 觀望中 (戰力: {max_score}/10)"

        if latest_row['ADX14'] < p['ADX_TREND_THRESHOLD']:
            trigger_str += " (⚠️ 盤整中，動能稍弱)"

        strength_diff = buy_score - sell_score
        structure_status = "多頭佔優" if strength_diff > 2 else "空頭佔優" if strength_diff < -2 else "結構盤整"
        # 🌟 補回：被誤刪的診斷數據字典 (用於產出戰鬥力分佈報告)
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

        # 🌟 補回：取得基本面營收與獲利分數
        f_data = add_fundamental_filter(ticker, p)

        return {
            "Ticker SYMBOL": ticker,
            "最新收盤價": round(current_price, 2),
            "結構強度": f"{strength_diff:+d}", 
            "今日系統燈號": status,
            "結構診斷": structure_status,
            "陣型標籤": setup_tag,
            "觸發條件明細": trigger_str,
            "基本面總分": f_data["基本面總分"],
            "營收年增率(%)": f"{f_data['營收年增率(%)']:.3f}",
            "營業利益率(%)": f"{f_data['營業利益率(%)']:.3f}",
            "系統勝率(%)": f"{win_rate:.3f}",       
            "累計報酬率(%)": f"{total_profit:.3f}", 
            "期望值": round(expected_value, 3),
            "建議倉位(%)": suggested_position,        # ✨ 新增：輸出凱利建議倉位
            "交易次數": total_trades,                 # ✨ 補上這個！讓 Optimizer 的懲罰機制生效
            "最大虧損(%)": 0,                         # ✨ 補上預設值防呆 (若未來有算 MDD 可替換)
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
                    preloaded_df=result["計算後資料"],  # ✨ 正解：傳入包含所有指標的計算後資料
                    win_rate=result["系統勝率(%)"], 
                    total_profit=result["累計報酬率(%)"],
                    expected_value=result["期望值"] 
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

        # --- 2. 🌟 真實期望值排序 (捨棄舊版假評分，完全使用回測引擎的 EV) ---
        long_candidates = [r for r in report_cards if "買訊" in r["今日系統燈號"]]
        short_candidates = [r for r in report_cards if "賣訊" in r["今日系統燈號"]]

        # 排序邏輯：直接依賴「大腦」算出來的「真實期望值」進行降序排列
        top_longs = sorted(long_candidates, key=lambda x: x.get("期望值", 0), reverse=True)
        top_shorts = sorted(short_candidates, key=lambda x: x.get("期望值", 0), reverse=True)
        all_ranked = sorted([r for r in report_cards if "觀望" not in r["今日系統燈號"]], key=lambda x: x.get('期望值', 0), reverse=True)
        # --- 3. 印出雙向戰略配置報告 ---
        print("\n" + "═"*30 + " ⚔️ 雙向戰略配置報告 " + "═"*30)
        print(f"📈 多頭比例：{bull_count/total_count:.1%} | 📉 空頭比例：{bear_count/total_count:.1%}")
        print(f"📊 趨勢強度：{trend_intensity:.3f} | 🛡️ 風險狀態：{market_msg}")
        print(f"💵 建議單筆限額：${(base_allocation * risk_factor):,.0f}")
        print("-" * 83)
        
        if top_longs:
            print(f"🚩 【作多首選】: {top_longs[0]['Ticker SYMBOL']} (真實期望值: {top_longs[0]['期望值']:.3f}%)")
        if top_shorts:
            print(f"🏳️ 【放空首選】: {top_shorts[0]['Ticker SYMBOL']} (真實期望值: {top_shorts[0]['期望值']:.3f}%)")
      
        print("-" * 83)
        if not all_ranked:
            print("📭 市場方向不明且無強烈訊號，建議空手觀望。")
        else:
            print(f"🎯 綜合排序前 {MAX_POSITIONS} 名進場建議：")
            for i, stock in enumerate(all_ranked[:MAX_POSITIONS]):
                direction = "🔴 做多" if "買訊" in stock["今日系統燈號"] else "🟢 放空"
                # ✨ 提取陣型名稱，讓清單更具實戰意義
                setup_name = stock['今日系統燈號'].split(' ')[1] if len(stock['今日系統燈號'].split(' ')) > 1 else "傳統訊號"
                print(f"  {i+1}. {stock['Ticker SYMBOL']} | {direction} | 陣型: {setup_name.ljust(8)} | 期望值(EV): {stock['期望值']:.3f}% | 建議配置: ${(base_allocation * risk_factor):,.0f}")
        print("═"*83)

        # --- 4. 📊 印出今日海選總表 (機構級版) ---
        final_report = pd.DataFrame(report_cards)
        pd.set_option('display.unicode.east_asian_width', True) 
        
        print("\n" + "="*30 + " 📊 今日海選總表 (真實數據透視) " + "="*30)
  
        # 🌟 重新排列欄位，把「真實期望值」跟「勝率」往前擺，徹底捨棄假評分
        display_cols = [
            "Ticker SYMBOL", "最新收盤價", "今日系統燈號", "期望值", 
            "系統勝率(%)", "累計報酬率(%)", "結構診斷", 
            "基本面總分", "營收年增率(%)", "觸發條件明細"
        ]
        
        actual_cols = [col for col in display_cols if col in final_report.columns]
        print(final_report[actual_cols].to_string(index=False))
        print("="*95)

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