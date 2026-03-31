import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

# 建立專屬的資料庫資料夾
CACHE_DIR = "kline_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def get_smart_klines(ticker_list):
    """
    聰明 K 線下載器：只抓缺漏的日期，並與本地存檔完美拼圖
    回傳：字典格式 { '2330.TW': DataFrame, '2454.TW': DataFrame }
    """
    result_dfs = {}
    today = datetime.now().date()
    
    tickers_to_fetch_2y = []
    tickers_to_update = {}
    oldest_update_date = today
    
    # ==========================================
    # 1. 檢查每檔股票的本地存檔狀況
    # ==========================================
    for ticker in ticker_list:
        file_path = os.path.join(CACHE_DIR, f"{ticker}.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)
            df.index = pd.to_datetime(df.index).normalize()
            
            # 🚨 致命陷阱防護：把「今天」的未收盤資料從存檔中剔除，強迫重抓最新報價
            df = df[df.index.date < today]
            
            if df.empty:
                tickers_to_fetch_2y.append(ticker)
            else:
                last_completed_date = df.index.max().date()
                start_date = last_completed_date + timedelta(days=1)
                
                if start_date <= today:
                    tickers_to_update[ticker] = df
                    if start_date < oldest_update_date:
                        oldest_update_date = start_date
                else:
                    result_dfs[ticker] = df
        else:
            tickers_to_fetch_2y.append(ticker)

    # ==========================================
    # 2. 批次下載：完全沒資料的新同學 (一口氣抓 2 年)
    # ==========================================
    if tickers_to_fetch_2y:
        print(f"📥 初次建檔：正在下載 {len(tickers_to_fetch_2y)} 檔股票的 2 年歷史...")
        batch_2y = yf.download(tickers_to_fetch_2y, period="2y", progress=False)
        for ticker in tickers_to_fetch_2y:
            df = batch_2y.xs(ticker, axis=1, level=1).copy() if isinstance(batch_2y.columns, pd.MultiIndex) else batch_2y.copy()
            df.dropna(subset=['Close'], inplace=True)
            if not df.empty:
                df.index = pd.to_datetime(df.index).normalize()
                df.to_csv(os.path.join(CACHE_DIR, f"{ticker}.csv")) # 存檔
                result_dfs[ticker] = df

    # ==========================================
    # 3. 批次補洞：只抓「最後存檔日」到「今天」的缺漏資料
    # ==========================================
    if tickers_to_update:
        print(f"🔄 增量更新：補齊 {len(tickers_to_update)} 檔股票從 {oldest_update_date} 至今的 K 線...")
        batch_update = yf.download(list(tickers_to_update.keys()), start=oldest_update_date, progress=False)
        for ticker, old_df in tickers_to_update.items():
            new_df = batch_update.xs(ticker, axis=1, level=1).copy() if isinstance(batch_update.columns, pd.MultiIndex) else batch_update.copy()
            new_df.dropna(subset=['Close'], inplace=True)
            
            if not new_df.empty:
                new_df.index = pd.to_datetime(new_df.index).normalize()
                # 🧩 完美拼圖：舊歷史 + 新資料，並去除重複
                combined = pd.concat([old_df, new_df])
                combined = combined[~combined.index.duplicated(keep='last')]
                combined.sort_index(inplace=True)
                
                combined.to_csv(os.path.join(CACHE_DIR, f"{ticker}.csv")) # 更新存檔
                result_dfs[ticker] = combined
            else:
                result_dfs[ticker] = old_df
                
    return result_dfs