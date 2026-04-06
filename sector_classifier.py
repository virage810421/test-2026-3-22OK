import yfinance as yf
import json
import os
import time

# =========================================================
# 快取檔設定
# =========================================================
CACHE_FILE = "sector_cache.json"

# =========================================================
# 讀取快取
# =========================================================
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ 快取檔讀取失敗，將重新建立: {e}")
            return {}
    return {}

# =========================================================
# 寫入快取
# =========================================================
def save_cache(cache):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"⚠️ 快取檔寫入失敗: {e}")

# =========================================================
# 股票產業分類函式
# =========================================================
def get_stock_sector(ticker):
    """
    自動連線查詢股票所屬產業，並進行分類歸檔
    回傳:
        TECH / FINANCE / SHIPPING / OTHERS
    """
    cache = load_cache()

    # 1. 先查快取
    if ticker in cache:
        print(f"📦 使用快取: {ticker} -> {cache[ticker]}")
        return cache[ticker]

    # 2. 若快取沒有，再查網路
    try:
        print(f"🔍 正在網路查詢 {ticker} 的公司業務與所屬產業...")
        stock = yf.Ticker(ticker)
        info = stock.info

        sector = str(info.get("sector", "")).strip()
        industry = str(info.get("industry", "")).strip()

        print(f"   sector   = {sector}")
        print(f"   industry = {industry}")

        # 3. 分類邏輯
        if (
            "Technology" in sector
            or "Semiconductor" in industry
            or "Electronic" in industry
            or "Electronics" in industry
        ):
            cat = "TECH"

        elif (
            "Financial" in sector
            or "Bank" in industry
            or "Insurance" in industry
            or "Capital Markets" in industry
        ):
            cat = "FINANCE"

        elif (
            "Marine Shipping" in industry
            or "Shipping" in industry
            or "Transportation" in sector
            or "Logistics" in industry
        ):
            cat = "SHIPPING"

        else:
            cat = "OTHERS"

        # 4. 寫入快取
        cache[ticker] = cat
        save_cache(cache)

        print(f"✅ 分類完成: {ticker} -> {cat}")
        return cat

    except Exception as e:
        print(f"⚠️ 無法取得 {ticker} 產業資訊: {e}")
        return "OTHERS"

# =========================================================
# 批量分類函式
# =========================================================
def classify_tickers(ticker_list, sleep_sec=0.5):
    result = {}
    for ticker in ticker_list:
        category = get_stock_sector(ticker)
        result[ticker] = category
        time.sleep(sleep_sec)  # 避免查太快被擋
    return result

# =========================================================
# 主程式測試
# =========================================================
if __name__ == "__main__":
    test_tickers = [
        "2330.TW",   # 台積電
        "2317.TW",   # 鴻海
        "2881.TW",   # 富邦金
        "2603.TW",   # 長榮
        "0050.TW"    # ETF，可能會進 OTHERS
    ]

    result = classify_tickers(test_tickers)

    print("\n================ 分類結果 ================ ")
    for ticker, category in result.items():
        print(f"{ticker} -> {category}")