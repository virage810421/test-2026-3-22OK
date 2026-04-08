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
CYCLICAL_TICKERS = {"2409.TW", "3481.TW", "6116.TW", "2344.TW", "2408.TW", "2337.TW"}

# 依優先序嘗試的來源表。
# 你目前 console 報錯的是 stock_revenue_industry_tw 不存在，
# 所以這裡改成「先試主表，失敗就退回 monthly_revenue_simple / fundamentals_clean」。
SECTOR_SOURCE_TABLES = [
    "stock_revenue_industry_tw",
    "monthly_revenue_simple",
    "fundamentals_clean",
]


def _normalize_ticker(ticker: str) -> str:
    ticker = str(ticker).strip().upper()
    if not ticker:
        return ""
    if ticker.endswith(".TW") or ticker.endswith(".TWO"):
        return ticker
    if ticker.isdigit():
        return f"{ticker}.TW"
    return ticker


def _load_sector_df_from_table(conn, table_name: str) -> pd.DataFrame:
    sql = f"""
        SELECT DISTINCT [Ticker SYMBOL], [產業類別名稱]
        FROM {table_name}
        WHERE [Ticker SYMBOL] IS NOT NULL
          AND [產業類別名稱] IS NOT NULL
    """
    return pd.read_sql(sql, conn)


def _map_industry_to_category(ticker: str, ind_name: str) -> str:
    ticker = _normalize_ticker(ticker)
    ind_name = str(ind_name).strip()

    if ticker in CYCLICAL_TICKERS:
        return "CYCLICAL"
    if ind_name in ["金融保險業"]:
        return "FINANCE"
    if ind_name in ["航運業"]:
        return "SHIPPING"
    if ind_name in ["生技醫療業", "農業科技業"]:
        return "BIO"
    if ind_name in [
        "半導體業", "電腦及週邊設備業", "光電業", "通信網路業",
        "電子零組件業", "電子通路業", "資訊服務業", "其他電子業", "數位雲端"
    ]:
        return "TECH"
    return "OTHERS"


def load_sql_sectors():
    """從 SQL 讀取官方產業名稱，並轉換為 6 大艦隊標籤"""
    global _SQL_SECTOR_CACHE

    if _SQL_SECTOR_CACHE is not None:
        return _SQL_SECTOR_CACHE

    _SQL_SECTOR_CACHE = {}

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            chosen_table = None
            df = pd.DataFrame()

            for table_name in SECTOR_SOURCE_TABLES:
                try:
                    df = _load_sector_df_from_table(conn, table_name)
                    if not df.empty:
                        chosen_table = table_name
                        break
                except Exception:
                    continue

            if df.empty:
                print("⚠️ SQL 產業分類讀取失敗：找不到可用來源表(stock_revenue_industry_tw / monthly_revenue_simple / fundamentals_clean)")
                _SQL_SECTOR_CACHE = {}
                return _SQL_SECTOR_CACHE

            for _, row in df.iterrows():
                ticker = _normalize_ticker(row["Ticker SYMBOL"])
                ind_name = str(row["產業類別名稱"]).strip()
                if not ticker or not ind_name:
                    continue
                _SQL_SECTOR_CACHE[ticker] = _map_industry_to_category(ticker, ind_name)

        print(f"✅ 成功從 SQL 載入並分類 {len(_SQL_SECTOR_CACHE)} 檔股票產業標籤！來源表：{chosen_table}")

    except Exception as e:
        print(f"⚠️ SQL 產業分類讀取失敗: {e}")
        _SQL_SECTOR_CACHE = {}

    return _SQL_SECTOR_CACHE


# =========================================================
# 🏷️ 單一查詢函式 (無縫接軌您原本的主程式)
# =========================================================
def get_stock_sector(ticker):
    """極速版：直接從 SQL 載入的記憶體中查詢分類"""
    cache = load_sql_sectors()
    return cache.get(_normalize_ticker(ticker), "OTHERS")


# =========================================================
# 🏷️ 批量查詢函式
# =========================================================
def classify_tickers(ticker_list, sleep_sec=0):
    """極速批量查詢（因為是從記憶體讀，完全不需要 sleep 等待了！）"""
    cache = load_sql_sectors()
    result = {}
    for ticker in ticker_list:
        result[ticker] = cache.get(_normalize_ticker(ticker), "OTHERS")
    return result


# =========================================================
# 🚀 裝備測試驗收區
# =========================================================
if __name__ == "__main__":
    print("==================================================")
    print("啟動極速本地 SQL 貼標機測試...")
    print("==================================================")

    test_tickers = [
        "2330.TW",
        "2881.TW",
        "2603.TW",
        "6472.TW",
        "2408.TW",
    ]

    result = classify_tickers(test_tickers)
    for k, v in result.items():
        print(f"{k} -> {v}")
