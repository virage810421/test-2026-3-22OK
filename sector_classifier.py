import yfinance as yf
import json
import os

CACHE_FILE = "sector_cache.json"

def get_stock_sector(ticker):
    """自動連線查詢股票所屬產業，並進行分類歸檔"""
    # 1. 先查快取，避免重複浪費網路資源
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    else:
        cache = {}

    if ticker in cache:
        return cache[ticker]

    # 2. 如果是新股票，呼叫 yfinance 去查它的祖宗十八代
    try:
        print(f"🔍 正在網路查詢 {ticker} 的公司業務與所屬產業...")
        info = yf.Ticker(ticker).info
        sector = info.get('sector', '')
        industry = info.get('industry', '')

        # 3. 核心分類邏輯：把美式英文分類轉成我們的三大陣營
        if 'Technology' in sector or 'Semiconductor' in industry or 'Electronic' in industry:
            cat = "TECH"
        elif 'Financial' in sector or 'Bank' in industry or 'Insurance' in industry:
            cat = "FINANCE"
        elif 'Marine Shipping' in industry or 'Transportation' in sector:
            cat = "SHIPPING"
        else:
            cat = "OTHERS" # 無法歸類到三大陣營的，放入通用池

        # 4. 存入快取記憶體
        cache[ticker] = cat
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=4, ensure_ascii=False)
        return cat
        
    except Exception as e:
        print(f"⚠️ 無法取得 {ticker} 產業資訊: {e}")
        return "OTHERS"