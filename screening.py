import yfinance as yf
import pandas as pd
import numpy as np
import pyodbc 
from advanced_chart import draw_chart
from FinMind.data import DataLoader
from scipy.signal import find_peaks
from config import PARAMS
from datetime import datetime

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
def add_fundamental_filter(ticker):
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
        if rev_yoy > 0: f_score += 1
        if rev_yoy > 20: f_score += 1 
        if op_margin > 0: f_score += 1 
        if op_margin < 0: f_score -= 2 

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
        df['BBI_BIAS'] = (df['Close'] - df['BBI']) / df['BBI'] * 100

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
        # 🌊 升級模組：ATR 動態公差與科學背離偵測
        # ==========================================
        df['ATR'] = df['TR'].ewm(alpha=1/p['DMI_PERIOD'], adjust=False).mean()
        df['Total_Net'] = df.get('Foreign_Net', 0) + df.get('Trust_Net', 0)

        def detect_divergence(price_series, indicator_series, atr_series, is_top=True, distance=7, atr_mult=1.0, threshold=None):
            dynamic_prominence = atr_series * atr_mult
            if is_top:
                peaks, _ = find_peaks(price_series, distance=distance, prominence=dynamic_prominence)
            else:
                peaks, _ = find_peaks(-price_series, distance=distance, prominence=dynamic_prominence)
            
            div_signals = np.zeros(len(price_series), dtype=bool)
            for i in range(1, len(peaks)):
                p1, p2 = peaks[i-1], peaks[i]
                if is_top:
                    cond_price = price_series[p2] > price_series[p1] 
                    cond_indicator = indicator_series[p2] < indicator_series[p1] 
                    cond_thresh = True if threshold is None else indicator_series[p2] > threshold
                else:
                    cond_price = price_series[p2] < price_series[p1] 
                    cond_indicator = indicator_series[p2] > indicator_series[p1] 
                    cond_thresh = True if threshold is None else indicator_series[p2] < threshold
                
                if cond_price and cond_indicator and cond_thresh:
                    div_signals[p2] = True 
                    
            return pd.Series(div_signals, index=df.index)

        df.dropna(inplace=True)
        if df.empty: return None 
        
        # ==========================================
        # 🛡️ 基礎防護網 (參數化)
        # ==========================================
        latest_check = df.iloc[-1]
        if latest_check['Vol_MA20'] < p['MIN_VOL_MA20']: return None 
        if latest_check['Close'] < p['MIN_PRICE']: return None 

        # ==========================================
        # D. ⚙️ 計分型邏輯閘 
        # ==========================================
        buy_trend = (df['Close'] > df['BBI']) & (df['BBI'] > df['BBI'].shift(1))
        sell_trend = (df['Close'] < df['BBI']) & (df['BBI'] < df['BBI'].shift(1))

        buy_c1 = df['Low'] <= df['BB_Lower']
        buy_c2 = df['RSI'] < df['DZ_Lower']
        buy_c1_c2_score = (buy_c1 | buy_c2).astype(int) 

        buy_c3 = (df['Volume'] > (df['Vol_MA20'] * p['VOL_BREAKOUT_MULTIPLIER'])) & (df['Close'] > df['Open'])
        buy_c4 = (df['MACD_Hist'] > df['MACD_Hist'].shift(1)) & (df['DIF'] < 0)
        buy_c5 = detect_divergence(df['Low'].values, df['RSI'].values, df['ATR'].values, is_top=False, distance=5, atr_mult=0.8, threshold=45)
        buy_c6 = (df['Close'] > df['BBI']) & (df['Close'].shift(1) <= df['BBI'].shift(1))
        buy_c7 = (df.get('Foreign_Net', 0) > 0) & (df.get('Trust_Net', 0) > 0)
        buy_c8 = (df['+DI14'] > df['-DI14']) & (df['ADX14'] >= p['ADX_TREND_THRESHOLD']) & (df['ADX14'] > df['ADX14'].shift(1))

        buy_c9_base = detect_divergence(df['Low'].values, df['Total_Net'].values, df['ATR'].values, is_top=False, distance=5, atr_mult=0.5)
        buy_c9 = buy_c9_base & (df['Total_Net'] > 0) 

        df['Buy_Score'] = (buy_trend.astype(int) + buy_c1_c2_score + buy_c3.astype(int) + 
                           buy_c4.astype(int) + buy_c5.astype(int) + buy_c6.astype(int) + 
                           buy_c7.astype(int) + buy_c8.astype(int) + buy_c9.astype(int))
        
        sell_c1 = df['High'] >= df['BB_Upper']
        sell_c2 = df['RSI'] > df['DZ_Upper']
        sell_c1_c2_score = (sell_c1 | sell_c2).astype(int) 

        sell_c3 = (df['Volume'] > (df['Vol_MA20'] * p['VOL_BREAKOUT_MULTIPLIER'])) & (df['Close'] < df['Open'])
        sell_c4 = (df['MACD_Hist'] < df['MACD_Hist'].shift(1)) & (df['DIF'] > 0)
        sell_c5 = detect_divergence(df['High'].values, df['RSI'].values, df['ATR'].values, is_top=True, distance=5, atr_mult=0.8, threshold=55)
        sell_c6 = (df['Close'] < df['BBI']) & (df['Close'].shift(1) >= df['BBI'].shift(1))
        sell_c7 = (df.get('Foreign_Net', 0) < 0) & (df.get('Trust_Net', 0) < 0)
        sell_c8 = (df['-DI14'] > df['+DI14']) & (df['ADX14'] >= p['ADX_TREND_THRESHOLD']) & (df['ADX14'] > df['ADX14'].shift(1))

        sell_c9_base = detect_divergence(df['High'].values, df['Total_Net'].values, df['ATR'].values, is_top=True, distance=5, atr_mult=0.5)
        sell_c9 = sell_c9_base & (df['Total_Net'] < 0) 

        df['Sell_Score'] = (sell_trend.astype(int) + sell_c1_c2_score + sell_c3.astype(int) + 
                             sell_c4.astype(int) + sell_c5.astype(int) + sell_c6.astype(int) + 
                             sell_c7.astype(int) + sell_c8.astype(int) + sell_c9.astype(int))

        # 動態滑價 (參數化)
        buy_adjust = np.where(buy_trend, 1.00, p['BUY_PULLBACK_RATE']) 
        sell_adjust = np.where(sell_trend, 1.00, p['SELL_PREMIUM_RATE'])
        
        # ✅ 改為參考昨天的收盤價來算掛單價
        df['Buy_Signal'] = np.where(df['Buy_Score'] >= p['TRIGGER_SCORE'], df['Close'].shift(1) * buy_adjust, np.nan)
        df['Sell_Signal'] = np.where(df['Sell_Score'] >= p['TRIGGER_SCORE'], df['Close'].shift(1) * sell_adjust, np.nan)
      
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
        TRADE_SHARES = 2000         
        
        SLIPPAGE = 0.0015 
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
            # 進場
            # ====================
            if position == 0:
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
                
            # ====================
            # 出場
            # ====================
            else:
                volatility_pct = (row['BB_std'] * 1.5) / safe_close
                DYNAMIC_SL = max(p['SL_MIN_PCT'], min(volatility_pct, p['SL_MAX_PCT']))

                trend_is_with_me = (direction == 1 and entry_trend_is_bull) or (direction == -1 and not entry_trend_is_bull)

                adx = row['ADX14'] if not pd.isna(row['ADX14']) else 0

                DYNAMIC_TP = p['TP_TREND_PCT'] if (trend_is_with_me and adx > p['ADX_TREND_THRESHOLD']) else p['TP_BASE_PCT']

                if entry_score >= 8:
                    DYNAMIC_TP = 9.99

                # === 停損判斷 ===
                stop_price = get_exit_price(entry_price, safe_open, DYNAMIC_SL, direction)

                # === 停利判斷 ===
                tp_price = entry_price * (1 + DYNAMIC_TP * direction)

                is_exit = False
                actual_exit_price = 0

                # 停損
                if (direction == 1 and row['Low'] <= stop_price) or \
                   (direction == -1 and row['High'] >= stop_price):
                    actual_exit_price = stop_price
                    is_exit = True

                # 🎯 停利 
                elif (direction == 1 and row['High'] >= tp_price) or \
                     (direction == -1 and row['Low'] <= tp_price):
                    # 🌟 呼叫工具層：達標就賣在目標價，跳空暴漲則賣在開盤價！
                    actual_exit_price = get_tp_price(entry_price, safe_open, DYNAMIC_TP, direction)
                    is_exit = True

                # 反轉
                elif (direction == 1 and row['Prev_Sell_Score'] >= p['TRIGGER_SCORE']) or \
                     (direction == -1 and row['Prev_Buy_Score'] >= p['TRIGGER_SCORE']):
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
        if buy_c7.iloc[-1]: buy_details.append(f"🔥法人同買(歷{int((buy_c7 & actual_buy_signals).sum())}次)") 
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
            "底背離", "🌟突破BBI", "🔥法人同買", "📈DMI趨勢成型", "💎結構底背離",
            "BBI空頭趨勢", "頂上軌", "RSI超買", "爆量", "MACD轉弱", 
            "頂背離", "💀跌破BBI", "🧊法人同賣", "📉DMI空頭成型", "💣結構頂背離"
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
        print("💡 註：歷史助攻次數是指在『總分 ≥ 4』時，該指標出現在獲勝組合中的次數。")

    else:
        print("掃描失敗，無資料輸出。")