import pyodbc
import pandas as pd

# =========================================================
# ⚙️ 資料庫連線設定
# =========================================================
DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)

# 建立一個記憶體快取，只要啟動時去 SQL 拿一次資料，後續查詢全部 0 秒回傳
_SQL_SECTOR_CACHE = None

# =========================================================
# 🎯 核心對照字典 (特殊股票強制覆寫)
# =========================================================
# 景氣循環股通常混在光電與半導體中，我們用代號直接強制標記，確保風控獨立
CYCLICAL_TICKERS = {"2409.TW", "3481.TW", "6116.TW", "2344.TW", "2408.TW", "2337.TW"}

def load_sql_sectors():
    """從 SQL 讀取官方產業名稱，並轉換為 6 大艦隊標籤"""
    global _SQL_SECTOR_CACHE
    
    # 如果已經載入過，就直接回傳，極大化效能
    if _SQL_SECTOR_CACHE is not None:
        return _SQL_SECTOR_CACHE

    _SQL_SECTOR_CACHE = {}
    
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            # 撈取不重複的股票與產業名稱
            sql = """
                SELECT DISTINCT [Ticker SYMBOL], [產業類別名稱]
                FROM stock_revenue_industry_tw
                WHERE [產業類別名稱] IS NOT NULL
            """
            df = pd.read_sql(sql, conn)

            for _, row in df.iterrows():
                ticker = str(row['Ticker SYMBOL']).strip()
                ind_name = str(row['產業類別名稱']).strip()

                # 🌟 1. 優先判定：特殊景氣循環股強制獨立
                if ticker in CYCLICAL_TICKERS:
                    cat = "CYCLICAL"
                    
                # 🌟 2. 金融艦隊
                elif ind_name in ["金融保險業"]:
                    cat = "FINANCE"
                    
                # 🌟 3. 航運艦隊
                elif ind_name in ["航運業"]:
                    cat = "SHIPPING"
                    
                # 🌟 4. 生技艦隊 (新增！)
                elif ind_name in ["生技醫療業", "農業科技業"]:
                    cat = "BIO"
                    
                # 🌟 5. 科技艦隊
                elif ind_name in ["半導體業", "電腦及週邊設備業", "光電業", "通信網路業", 
                                  "電子零組件業", "電子通路業", "資訊服務業", "其他電子業", "數位雲端"]:
                    cat = "TECH"
                    
                # 🌟 6. 傳產與其他艦隊 (水泥、鋼鐵、電機、食品等...)
                else:
                    cat = "OTHERS"

                # 存入記憶體快取
                _SQL_SECTOR_CACHE[ticker] = cat

        print(f"✅ 成功從 SQL 載入並分類 {len(_SQL_SECTOR_CACHE)} 檔股票產業標籤！")

    except Exception as e:
        print(f"⚠️ SQL 產業分類讀取失敗: {e}")
        _SQL_SECTOR_CACHE = {}

    return _SQL_SECTOR_CACHE

# =========================================================
# 🏷️ 單一查詢函式 (無縫接軌您原本的主程式)
# =========================================================
def get_stock_sector(ticker):
    """
    極速版：直接從 SQL 載入的記憶體中查詢分類
    """
    cache = load_sql_sectors()
    
    # 如果 SQL 裡面真的找不到這檔（例如剛上市沒營收的），預設丟去 OTHERS
    return cache.get(ticker, "OTHERS")

# =========================================================
# 🏷️ 批量查詢函式
# =========================================================
def classify_tickers(ticker_list, sleep_sec=0):
    """
    極速批量查詢（因為是從記憶體讀，完全不需要 sleep 等待了！）
    """
    cache = load_sql_sectors()
    result = {}
    for ticker in ticker_list:
        result[ticker] = cache.get(ticker, "OTHERS")
    return result

# =========================================================
# 🚀 裝備測試驗收區
# =========================================================
if __name__ == "__main__":
    print("==================================================")
    print("啟動極速本地 SQL 貼標機測試...")
    print("==================================================")
    
    test_tickers = [
        "2330.TW",   # 台積電 (半導體 -> TECH)
        "2881.TW",   # 富邦金 (金融保險 -> FINANCE)
        "2603.TW",   # 長榮 (航運業 -> SHIPPING)
        "6472.TW",   # 保瑞 (生技醫療 -> BIO)
        "2409.TW",   # 友達 (強制覆寫 -> CYCLICAL)
        "2002.TW",   # 中鋼 (鋼鐵工業 -> OTHERS)
    ]

    # 瞬間完成分類
    result = classify_tickers(test_tickers)

    print("\n================ 分類結果 ================ ")
    for ticker, category in result.items():
        print(f"🎯 {ticker.ljust(8)} 👉 歸建【{category}】艦隊")