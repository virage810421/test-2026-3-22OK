import os
import time
import requests
import urllib3
import pandas as pd
import pyodbc

from io import StringIO
from datetime import datetime, timedelta
from FinMind.data import DataLoader
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================
# ⚙️ 基本設定
# =========================================================
DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)

TABLE_NAME = "daily_chip_data"
CSV_FILENAME = "daily_chip_data_backup.csv"

DAYS_TO_FETCH = 5
SKIP_WEEKEND = False
SLEEP_BETWEEN_STOCKS = 0.8
ENABLE_BACKUP_SOURCE = True
SAVE_EVERY_N_STOCKS = 50

# =========================================================
# 🔐 FinMind Token (由 config.py 中央保險箱統一控管)
# =========================================================
try:
    from config import FINMIND_API_TOKEN as API_TOKEN
except ImportError:
    print("⚠️ 警告：找不到 config.py 或 FINMIND_API_TOKEN！請確認中央設定檔是否正確。")
    API_TOKEN = ""
# =========================================================
# 🗓️ 台股休市日（可自行往後補）
# =========================================================
TW_MARKET_HOLIDAYS = {
    "2026-01-01",
    "2026-02-16",
    "2026-02-17",
    "2026-02-18",
    "2026-02-19",
    "2026-02-20",
    "2026-02-27",
    "2026-04-03",
    "2026-04-06",
    "2026-05-01",
    "2026-06-19",
    "2026-09-25",
    "2026-10-09",
}

CSV_COLUMNS = [
    "日期",
    "Ticker SYMBOL",
    "外資買賣超",
    "投信買賣超",
    "自營商買賣超",
    "三大法人合計",
    "資料來源"
]

# =========================================================
# 🌐 Requests Session
# =========================================================
def build_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session

SESSION = build_session()

def safe_get_json(url, timeout=20):
    try:
        res = SESSION.get(url, timeout=timeout, verify=True)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.SSLError:
        print(f"⚠️ SSL 驗證失敗，改用 verify=False 重試: {url}")
        res = SESSION.get(url, timeout=timeout, verify=False)
        res.raise_for_status()
        return res.json()

def safe_get_text(url, timeout=20, encoding=None):
    try:
        res = SESSION.get(url, timeout=timeout, verify=True)
        res.raise_for_status()
    except requests.exceptions.SSLError:
        print(f"⚠️ SSL 驗證失敗，改用 verify=False 重試: {url}")
        res = SESSION.get(url, timeout=timeout, verify=False)
        res.raise_for_status()

    if encoding:
        res.encoding = encoding
    return res.text

# =========================================================
# 🗓️ 交易日判斷
# =========================================================
def is_tw_trading_day(date_str: str) -> bool:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return False

    if d.weekday() >= 5:
        return False

    if date_str in TW_MARKET_HOLIDAYS:
        return False

    return True

# =========================================================
# 📡 股票清單：上市 + 上櫃
# =========================================================
def get_official_stock_list():
    print("📡 正在向【證交所 / 櫃買中心】請求全市場名單...")

    twse_url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    tpex_urls = [
        "https://www.tpex.org.tw/openapi/v1/t187ap03_O",
        "https://www.tpex.org.tw/www/zh-tw/regular_emerging/stocks/summary?response=json"
    ]

    all_stocks = []

    # 上市
    try:
        twse_data = safe_get_json(twse_url)
        if isinstance(twse_data, list):
            twse_stocks = [str(item.get("公司代號", "")).strip() for item in twse_data]
            twse_stocks = [s for s in twse_stocks if len(s) == 4 and s.isdigit()]
            all_stocks.extend(twse_stocks)
            print(f"✅ 上市股票：{len(twse_stocks)} 檔")
    except Exception as e:
        print(f"❌ 上市名單抓取失敗: {e}")

    # 上櫃
    tpex_success = False
    for url in tpex_urls:
        try:
            data = safe_get_json(url)
            tpex_stocks = []

            if isinstance(data, list):
                for item in data:
                    code = str(
                        item.get("公司代號", "") or
                        item.get("SecuritiesCompanyCode", "") or
                        item.get("代號", "")
                    ).strip()
                    if len(code) == 4 and code.isdigit():
                        tpex_stocks.append(code)

            elif isinstance(data, dict):
                possible_rows = []
                for key in ["tables", "data", "aaData", "records"]:
                    if key in data and isinstance(data[key], list):
                        possible_rows = data[key]
                        break

                for row in possible_rows:
                    if isinstance(row, dict):
                        code = str(
                            row.get("公司代號", "") or
                            row.get("SecuritiesCompanyCode", "") or
                            row.get("代號", "")
                        ).strip()
                        if len(code) == 4 and code.isdigit():
                            tpex_stocks.append(code)

            tpex_stocks = sorted(list(set(tpex_stocks)))
            if tpex_stocks:
                all_stocks.extend(tpex_stocks)
                print(f"✅ 上櫃股票：{len(tpex_stocks)} 檔")
                tpex_success = True
                break

        except Exception as e:
            print(f"⚠️ 上櫃名單來源失敗: {url} | {e}")

    if not tpex_success:
        print("❌ 上櫃名單抓取失敗")

    pure_stocks = sorted(list(set(all_stocks)))
    if pure_stocks:
        print(f"🎯 成功取得全市場股票：{len(pure_stocks)} 檔")
        return pure_stocks

    print("⚠️ 官方名單全部失敗，改用備援清單")
    return ["2330", "2317", "2454"]

# =========================================================
# 🧱 SQL Table 檢查
# =========================================================
def ensure_chip_table(cursor):
    cursor.execute(f"""
        IF OBJECT_ID(N'dbo.{TABLE_NAME}', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.{TABLE_NAME} (
                [日期] DATE NOT NULL,
                [Ticker SYMBOL] NVARCHAR(20) NOT NULL,
                [外資買賣超] FLOAT NULL,
                [投信買賣超] FLOAT NULL,
                [自營商買賣超] FLOAT NULL,
                [三大法人合計] FLOAT NULL,
                [資料來源] NVARCHAR(20) NULL,
                [更新時間] DATETIME NULL,
                CONSTRAINT PK_{TABLE_NAME} PRIMARY KEY ([日期], [Ticker SYMBOL])
            )
        END
    """)

    alter_sqls = [
        f"IF COL_LENGTH('dbo.{TABLE_NAME}', N'資料來源') IS NULL ALTER TABLE dbo.{TABLE_NAME} ADD [資料來源] NVARCHAR(20) NULL",
        f"IF COL_LENGTH('dbo.{TABLE_NAME}', N'更新時間') IS NULL ALTER TABLE dbo.{TABLE_NAME} ADD [更新時間] DATETIME NULL"
    ]
    for sql in alter_sqls:
        cursor.execute(sql)

# =========================================================
# 📂 CSV 工具
# =========================================================
def normalize_date_str(value):
    if pd.isna(value):
        return None

    value = str(value).strip()
    if not value:
        return None

    # 處理像 2026-04-06 00:00:00
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    # 已經是 yyyy-mm-dd
    if len(value) >= 10:
        return value[:10]

    return value

def normalize_chip_dataframe(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=CSV_COLUMNS)

    df = df.copy()

    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["日期"] = df["日期"].apply(normalize_date_str)
    df["Ticker SYMBOL"] = df["Ticker SYMBOL"].astype(str).str.strip()

    for col in ["外資買賣超", "投信買賣超", "自營商買賣超", "三大法人合計"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["資料來源"] = df["資料來源"].astype(str).replace("nan", None)

    df = df[CSV_COLUMNS].copy()
    df = df.dropna(subset=["日期", "Ticker SYMBOL"])
    df = df.where(pd.notnull(df), None)

    return df

def load_existing_csv():
    if not os.path.exists(CSV_FILENAME):
        print(f"📂 地端 CSV 不存在：{CSV_FILENAME}")
        return pd.DataFrame(columns=CSV_COLUMNS)

    try:
        df = pd.read_csv(
            CSV_FILENAME,
            encoding="utf-8-sig",
            dtype={"日期": str, "Ticker SYMBOL": str}
        )
        df = normalize_chip_dataframe(df)
        df = df.drop_duplicates(subset=["日期", "Ticker SYMBOL"]).reset_index(drop=True)
        print(f"📂 已讀取地端 CSV：{CSV_FILENAME} | {len(df)} 筆")
        return df
    except Exception as e:
        print(f"⚠️ 讀取地端 CSV 失敗，改視為空白檔案：{e}")
        return pd.DataFrame(columns=CSV_COLUMNS)

def save_csv(df):
    df = normalize_chip_dataframe(df)
    df = df.drop_duplicates(subset=["日期", "Ticker SYMBOL"]).reset_index(drop=True)
    df.to_csv(CSV_FILENAME, index=False, encoding="utf-8-sig")
    print(f"💾 CSV 已更新：{CSV_FILENAME} | 共 {len(df)} 筆")

def flush_new_rows_to_csv(local_csv_df, new_rows, note="批次存檔"):
    if not new_rows:
        return local_csv_df

    temp_df = pd.DataFrame(new_rows, columns=CSV_COLUMNS)
    temp_df = normalize_chip_dataframe(temp_df)

    merged_df = pd.concat([local_csv_df, temp_df], ignore_index=True)
    merged_df = normalize_chip_dataframe(merged_df)
    merged_df = merged_df.drop_duplicates(subset=["日期", "Ticker SYMBOL"]).reset_index(drop=True)

    save_csv(merged_df)
    print(f"💾 [{note}] 已批次存檔 {len(temp_df)} 筆新資料")
    return merged_df

# =========================================================
# 🗄️ SQL 既有 Key
# =========================================================
def load_sql_existing_keys():
    existing_keys = set()

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            ensure_chip_table(cursor)
            conn.commit()

            cursor.execute(f"""
                SELECT CONVERT(VARCHAR(10), [日期], 23), [Ticker SYMBOL]
                FROM dbo.{TABLE_NAME}
                WHERE [日期] IS NOT NULL
                  AND [Ticker SYMBOL] IS NOT NULL
            """)

            rows = cursor.fetchall()
            for row in rows:
                existing_keys.add((str(row[0]).strip(), str(row[1]).strip()))

        print(f"🗄️ 已讀取 SQL 既有資料鍵值：{len(existing_keys)} 筆")
        return existing_keys

    except Exception as e:
        print(f"⚠️ 讀取 SQL 既有資料失敗，視為空表：{e}")
        return set()

def build_union_key_set(csv_df, sql_keys):
    csv_keys = set()
    if csv_df is not None and not csv_df.empty:
        csv_keys = set(
            (str(r["日期"]).strip(), str(r["Ticker SYMBOL"]).strip())
            for _, r in csv_df.iterrows()
        )
    return csv_keys.union(sql_keys)

# =========================================================
# 🧠 FinMind 初始化
# =========================================================
def init_finmind_loader(api_token: str):
    if not api_token:
        raise ValueError("尚未設定 FINMIND_API_TOKEN")

    dl = DataLoader()

    # 相容不同版本 FinMind
    try:
        if hasattr(dl, "login_by_token"):
            dl.login_by_token(api_token=api_token)
            print("🔐 FinMind Token 已登入")
        else:
            print("ℹ️ FinMind 套件版本未提供 login_by_token()，將直接使用 DataLoader()")
    except Exception as e:
        print(f"⚠️ FinMind login_by_token 失敗，改直接使用 DataLoader(): {e}")

    return dl

# =========================================================
# 🧠 FinMind 來源
# =========================================================
def fetch_chip_from_finmind(dl, stock_id, start_dt):
    try:
        # 常見新版本方法
        chip_df = dl.taiwan_stock_institutional_investors(
            stock_id=stock_id,
            start_date=start_dt
        )
    except TypeError:
        # 保底相容
        chip_df = dl.taiwan_stock_institutional_investors(
            stock_id=stock_id,
            start_date=start_dt,
            end_date=datetime.now().strftime("%Y-%m-%d")
        )

    if chip_df is None or chip_df.empty:
        return None

    chip_df = chip_df.copy()
    chip_df["date"] = chip_df["date"].astype(str).str[:10]
    chip_df["buy"] = pd.to_numeric(chip_df["buy"], errors="coerce").fillna(0)
    chip_df["sell"] = pd.to_numeric(chip_df["sell"], errors="coerce").fillna(0)
    chip_df["Net"] = chip_df["buy"] - chip_df["sell"]

    foreign = chip_df[chip_df["name"] == "Foreign_Investor"].groupby("date")["Net"].sum()
    trust = chip_df[chip_df["name"] == "Investment_Trust"].groupby("date")["Net"].sum()
    dealers = chip_df[chip_df["name"].isin(["Dealer_self", "Dealer_Hedging"])].groupby("date")["Net"].sum()

    rows = []
    dates = sorted(chip_df["date"].dropna().unique())

    for date_str in dates:
        if not is_tw_trading_day(date_str):
            continue

        f_net = float(foreign.get(date_str, 0))
        t_net = float(trust.get(date_str, 0))
        d_net = float(dealers.get(date_str, 0))
        total_net = f_net + t_net + d_net

        rows.append({
            "日期": date_str,
            "Ticker SYMBOL": f"{stock_id}.TW",
            "外資買賣超": f_net,
            "投信買賣超": t_net,
            "自營商買賣超": d_net,
            "三大法人合計": total_net,
            "資料來源": "FinMind"
        })

    return rows if rows else None

# =========================================================
# 🛟 官方備援
# =========================================================
def roc_year(ad_year):
    return ad_year - 1911

def try_parse_csv_text_to_df(text):
    if not text or not text.strip():
        return pd.DataFrame()

    lines = [line.strip("\ufeff").strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return pd.DataFrame()

    # 優先找最可能是真正表頭的列
    header_idx = None
    for i, line in enumerate(lines):
        line_no_space = line.replace(" ", "")
        if (
            "證券代號" in line_no_space or
            "代號" in line_no_space
        ) and line.count(",") >= 3:
            header_idx = i
            break

    if header_idx is not None:
        csv_text = "\n".join(lines[header_idx:])
    else:
        candidates = [line for line in lines if line.count(",") >= 3]
        csv_text = "\n".join(candidates)

    if not csv_text.strip():
        return pd.DataFrame()

    for enc in [None]:
        try:
            df = pd.read_csv(StringIO(csv_text), engine="python", on_bad_lines="skip")
            if not df.empty:
                return df
        except Exception:
            pass

    return pd.DataFrame()

def parse_twse_backup_csv(date_obj):
    date_str = date_obj.strftime("%Y%m%d")
    if not is_tw_trading_day(date_obj.strftime("%Y-%m-%d")):
        return pd.DataFrame()

    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALLBUT0999&response=csv"

    try:
        text = safe_get_text(url, timeout=20, encoding="utf-8")
        if "沒有符合條件的資料" in text:
            return pd.DataFrame()

        df = try_parse_csv_text_to_df(text)
        if df.empty:
            return pd.DataFrame()

        df.columns = [str(c).strip().replace(" ", "") for c in df.columns]

        stock_col = None
        for cand in ["證券代號", "代號", "股票代號"]:
            if cand in df.columns:
                stock_col = cand
                break
        if stock_col is None:
            return pd.DataFrame()

        col_foreign = None
        col_trust = None
        col_dealer = None

        for c in df.columns:
            s = str(c)
            if "外陸資買賣超股數(不含外資自營商)" in s or "外資及陸資買賣超股數(不含外資自營商)" in s:
                col_foreign = c
            elif "投信買賣超股數" in s:
                col_trust = c
            elif "自營商買賣超股數(自行買賣+避險)" in s or "自營商買賣超股數" in s:
                col_dealer = c

        if not all([col_foreign, col_trust, col_dealer]):
            return pd.DataFrame()

        result = df[[stock_col, col_foreign, col_trust, col_dealer]].copy()
        result.columns = ["stock_id", "foreign", "trust", "dealer"]

        for col in ["foreign", "trust", "dealer"]:
            result[col] = (
                result[col].astype(str)
                .str.replace(",", "", regex=False)
                .str.replace(" ", "", regex=False)
            )
            result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)

        result["stock_id"] = result["stock_id"].astype(str).str.strip()
        result = result[result["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
        result["date"] = date_obj.strftime("%Y-%m-%d")
        result["suffix"] = ".TW"
        return result.reset_index(drop=True)

    except Exception as e:
        print(f"⚠️ 上市官方備援失敗 {date_obj.strftime('%Y-%m-%d')}: {e}")
        return pd.DataFrame()

def parse_tpex_backup_csv(date_obj):
    if not is_tw_trading_day(date_obj.strftime("%Y-%m-%d")):
        return pd.DataFrame()

    roc_y = roc_year(date_obj.year)
    roc_m = str(date_obj.month).zfill(2)
    roc_d = str(date_obj.day).zfill(2)

    urls = [
        f"https://www.tpex.org.tw/www/zh-tw/three-major-institutions/afterTrading/dailyTrading?date={roc_y}/{roc_m}/{roc_d}&id=&response=csv",
        f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&d={roc_y}/{roc_m}/{roc_d}&se=EW"
    ]

    for url in urls:
        try:
            text = safe_get_text(url, timeout=20, encoding="utf-8")
            if "查無資料" in text or "沒有符合條件的資料" in text:
                continue

            df = try_parse_csv_text_to_df(text)
            if df.empty:
                continue

            df.columns = [str(c).strip().replace(" ", "") for c in df.columns]

            stock_col = None
            for cand in ["代號", "證券代號", "股票代號"]:
                if cand in df.columns:
                    stock_col = cand
                    break
            if stock_col is None:
                continue

            col_foreign = None
            col_trust = None
            col_dealer = None

            for c in df.columns:
                s = str(c)
                if "外資及陸資買賣超股數" in s or "外陸資買賣超股數" in s:
                    col_foreign = c
                elif "投信買賣超股數" in s:
                    col_trust = c
                elif "自營商買賣超股數" in s:
                    col_dealer = c

            if not all([col_foreign, col_trust, col_dealer]):
                continue

            result = df[[stock_col, col_foreign, col_trust, col_dealer]].copy()
            result.columns = ["stock_id", "foreign", "trust", "dealer"]

            for col in ["foreign", "trust", "dealer"]:
                result[col] = (
                    result[col].astype(str)
                    .str.replace(",", "", regex=False)
                    .str.replace(" ", "", regex=False)
                )
                result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)

            result["stock_id"] = result["stock_id"].astype(str).str.strip()
            result = result[result["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
            result["date"] = date_obj.strftime("%Y-%m-%d")
            result["suffix"] = ".TWO"
            return result.reset_index(drop=True)

        except Exception as e:
            print(f"⚠️ 上櫃官方備援失敗 {date_obj.strftime('%Y-%m-%d')} | {url} | {e}")

    return pd.DataFrame()

def fetch_backup_chip_all_markets(start_dt):
    start_date_obj = datetime.strptime(start_dt, "%Y-%m-%d").date()
    end_date_obj = datetime.now().date()

    backup_map = {}
    cur = start_date_obj

    while cur <= end_date_obj:
        cur_str = cur.strftime("%Y-%m-%d")
        if is_tw_trading_day(cur_str):
            df_twse = parse_twse_backup_csv(cur)
            df_tpex = parse_tpex_backup_csv(cur)
            df_all = pd.concat([df_twse, df_tpex], ignore_index=True)

            if not df_all.empty:
                for _, row in df_all.iterrows():
                    stock_id = str(row["stock_id"]).strip()
                    date_str = str(row["date"]).strip()
                    suffix = str(row.get("suffix", ".TW")).strip()

                    backup_map[(stock_id, date_str)] = {
                        "日期": date_str,
                        "Ticker SYMBOL": f"{stock_id}{suffix}",
                        "外資買賣超": float(row["foreign"]),
                        "投信買賣超": float(row["trust"]),
                        "自營商買賣超": float(row["dealer"]),
                        "三大法人合計": float(row["foreign"]) + float(row["trust"]) + float(row["dealer"]),
                        "資料來源": "OFFICIAL"
                    }

        cur += timedelta(days=1)

    return backup_map

def fetch_chip_from_backup(stock_id, need_dates, backup_map):
    rows = []

    for date_str in sorted(list(need_dates)):
        key = (stock_id, date_str)
        if key in backup_map:
            rows.append(backup_map[key])

    return rows if rows else None

# =========================================================
# 📤 SQL UPSERT
# =========================================================
def upsert_chip_rows(cursor, rows):
    if not rows:
        return 0

    sql = f"""
        IF EXISTS (
            SELECT 1
            FROM dbo.{TABLE_NAME}
            WHERE [日期] = ? AND [Ticker SYMBOL] = ?
        )
        BEGIN
            UPDATE dbo.{TABLE_NAME}
            SET [外資買賣超] = ?,
                [投信買賣超] = ?,
                [自營商買賣超] = ?,
                [三大法人合計] = ?,
                [資料來源] = ?,
                [更新時間] = GETDATE()
            WHERE [日期] = ? AND [Ticker SYMBOL] = ?
        END
        ELSE
        BEGIN
            INSERT INTO dbo.{TABLE_NAME}
            ([日期], [Ticker SYMBOL], [外資買賣超], [投信買賣超], [自營商買賣超], [三大法人合計], [資料來源], [更新時間])
            VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
        END
    """

    count = 0
    for row in rows:
        date_str = normalize_date_str(row["日期"])
        ticker_symbol = str(row["Ticker SYMBOL"]).strip()
        f_net = row["外資買賣超"]
        t_net = row["投信買賣超"]
        d_net = row["自營商買賣超"]
        total_net = row["三大法人合計"]
        source = row["資料來源"]

        cursor.execute(sql, (
            date_str, ticker_symbol,
            f_net, t_net, d_net, total_net, source, date_str, ticker_symbol,
            date_str, ticker_symbol, f_net, t_net, d_net, total_net, source
        ))
        count += 1

    return count

# =========================================================
# 🧩 單一股票智慧補抓
# =========================================================
def build_target_dates(start_dt):
    start_date_obj = datetime.strptime(start_dt, "%Y-%m-%d").date()
    end_date_obj = datetime.now().date()

    target_dates = []
    cur = start_date_obj
    while cur <= end_date_obj:
        cur_str = cur.strftime("%Y-%m-%d")
        if is_tw_trading_day(cur_str):
            target_dates.append(cur_str)
        cur += timedelta(days=1)

    return target_dates

def fetch_single_stock_smart(dl, stock_id, existing_union_keys, target_dates, backup_map):
    need_dates = set()

    for date_str in target_dates:
        has_tw = (date_str, f"{stock_id}.TW") in existing_union_keys
        has_two = (date_str, f"{stock_id}.TWO") in existing_union_keys
        if not has_tw and not has_two:
            need_dates.add(date_str)

    if not need_dates:
        return {
            "stock_id": stock_id,
            "success": True,
            "rows": [],
            "message": f"{stock_id} 已完整覆蓋，略過下載"
        }

    finmind_status = "未執行"
    backup_status = "未執行"

    try:
        finmind_start = min(need_dates)
        raw_finmind_rows = fetch_chip_from_finmind(dl, stock_id, finmind_start)

        if raw_finmind_rows is None:
            finmind_status = "FinMind回空資料"
        else:
            finmind_rows = [r for r in raw_finmind_rows if r["日期"] in need_dates]
            if finmind_rows:
                return {
                    "stock_id": stock_id,
                    "success": True,
                    "rows": finmind_rows,
                    "message": f"{stock_id} FinMind 補抓成功 {len(finmind_rows)} 筆"
                }
            else:
                raw_dates = sorted(list(set(r["日期"] for r in raw_finmind_rows)))
                finmind_status = f"FinMind有資料，但未覆蓋缺漏日期 | FinMind日期={raw_dates}"
    except Exception as e:
        finmind_status = f"FinMind例外: {e}"

    if ENABLE_BACKUP_SOURCE:
        try:
            backup_rows = fetch_chip_from_backup(stock_id, need_dates, backup_map)
            if backup_rows:
                return {
                    "stock_id": stock_id,
                    "success": True,
                    "rows": backup_rows,
                    "message": f"{stock_id} 官方備援補抓成功 {len(backup_rows)} 筆"
                }
            else:
                exist_backup_dates = sorted([
                    d for (sid, d) in backup_map.keys() if sid == stock_id
                ])
                if exist_backup_dates:
                    backup_status = f"官方備援有資料，但未覆蓋缺漏日期 | 備援日期={exist_backup_dates}"
                else:
                    backup_status = "官方備援無資料"
        except Exception as e:
            backup_status = f"官方備援例外: {e}"
    else:
        backup_status = "官方備援未啟用"

    need_dates_sorted = sorted(list(need_dates))

    return {
        "stock_id": stock_id,
        "success": False,
        "rows": [],
        "message": (
            f"{stock_id} 缺漏資料補抓失敗 | "
            f"缺漏日期={need_dates_sorted} | "
            f"{finmind_status} | "
            f"{backup_status}"
        )
    }

# =========================================================
# 🚀 智慧同步主流程
# =========================================================
def smart_sync_daily_chip():
    now = datetime.now()

    if SKIP_WEEKEND and now.weekday() >= 5:
        print(f"[{now.strftime('%H:%M:%S')}] ☕ 今天是週末（非交易日），自動收集車休息中...")
        return

    print("========================================================")
    print("🧠 啟動 daily_chip_data 智慧版：先查 SQL / CSV，再補抓缺漏")
    print("========================================================")

    if not API_TOKEN:
        print("⚠️ 尚未設定 FINMIND_API_TOKEN，請先設定環境變數後再執行")
        return

    start_dt = (datetime.now() - timedelta(days=DAYS_TO_FETCH)).strftime("%Y-%m-%d")
    target_dates = build_target_dates(start_dt)

    if not target_dates:
        print("⚠️ 目前目標區間內沒有交易日，停止執行。")
        return

    print(f"📅 目標交易日期區間：{target_dates[0]} ~ {target_dates[-1]}")
    print(f"📆 本次有效交易日數：{len(target_dates)}")

    local_csv_df = load_existing_csv()
    sql_keys = load_sql_existing_keys()

    if not local_csv_df.empty:
        csv_keys = set(
            (str(r["日期"]).strip(), str(r["Ticker SYMBOL"]).strip())
            for _, r in local_csv_df.iterrows()
        )
        csv_only_keys = csv_keys - sql_keys

        if csv_only_keys:
            csv_missing_in_sql_df = local_csv_df[
                local_csv_df.apply(
                    lambda r: (str(r["日期"]).strip(), str(r["Ticker SYMBOL"]).strip()) in csv_only_keys,
                    axis=1
                )
            ].copy()

            print(f"📥 發現 CSV 有但 SQL 沒有的資料：{len(csv_missing_in_sql_df)} 筆，先補進 SQL...")

            try:
                with pyodbc.connect(DB_CONN_STR) as conn:
                    cursor = conn.cursor()
                    ensure_chip_table(cursor)
                    count = upsert_chip_rows(cursor, csv_missing_in_sql_df.to_dict("records"))
                    conn.commit()
                    print(f"🎉 [CSV補SQL] 成功寫入 / 更新 {count} 筆")
            except Exception as e:
                print(f"❌ [CSV補SQL] 寫入失敗：{e}")

            sql_keys = load_sql_existing_keys()
        else:
            print("✅ CSV 與 SQL 之間沒有待補的舊資料。")
    else:
        print("📂 地端 CSV 為空，略過 CSV 補 SQL。")

    union_keys = build_union_key_set(local_csv_df, sql_keys)
    target_stocks = get_official_stock_list()

    need_fetch_stocks = []
    skip_count = 0

    for stock_id in target_stocks:
        missing_dates = []
        for d in target_dates:
            has_tw = (d, f"{stock_id}.TW") in union_keys
            has_two = (d, f"{stock_id}.TWO") in union_keys
            if not has_tw and not has_two:
                missing_dates.append(d)

        if missing_dates:
            need_fetch_stocks.append(stock_id)
        else:
            skip_count += 1

    print(f"✅ 已完整覆蓋股票數：{skip_count}")
    print(f"🌐 需要上網補抓股票數：{len(need_fetch_stocks)}")

    backup_map = {}
    if ENABLE_BACKUP_SOURCE and need_fetch_stocks:
        print("🛟 正在預先下載官方備援資料...")
        try:
            backup_map = fetch_backup_chip_all_markets(start_dt)
            print(f"✅ 官方備援資料筆數：{len(backup_map)}")
        except Exception as e:
            print(f"⚠️ 官方備援預抓失敗：{e}")
            backup_map = {}

    dl = init_finmind_loader(API_TOKEN)

    new_rows = []
    success_count = 0
    fail_count = 0
    since_last_save = 0

    if need_fetch_stocks:
        total = len(need_fetch_stocks)

        for idx, stock_id in enumerate(need_fetch_stocks, 1):
            since_last_save += 1
            print(f"📡 [{idx}/{total}] 正在處理 {stock_id} ...")

            result = fetch_single_stock_smart(
                dl=dl,
                stock_id=stock_id,
                existing_union_keys=union_keys,
                target_dates=target_dates,
                backup_map=backup_map
            )

            if result["success"]:
                if result["rows"]:
                    new_rows.extend(result["rows"])
                    for r in result["rows"]:
                        union_keys.add((str(r["日期"]).strip(), str(r["Ticker SYMBOL"]).strip()))
                    print(f"✅ {result['message']}")
                else:
                    print(f"⏭️ {result['message']}")
                success_count += 1
            else:
                fail_count += 1
                print(f"⚠️ {result['message']}")

            if since_last_save >= SAVE_EVERY_N_STOCKS:
                if new_rows:
                    local_csv_df = flush_new_rows_to_csv(
                        local_csv_df,
                        new_rows,
                        note=f"每 {SAVE_EVERY_N_STOCKS} 檔自動存檔"
                    )
                    new_rows = []
                since_last_save = 0

            time.sleep(SLEEP_BETWEEN_STOCKS)
    else:
        print("🎯 所有股票都已達到本地覆蓋條件，這次不需要上網下載。")

    if new_rows:
        local_csv_df = flush_new_rows_to_csv(local_csv_df, new_rows, note="最後收尾存檔")

    final_df = load_existing_csv()
    if not final_df.empty:
        try:
            with pyodbc.connect(DB_CONN_STR) as conn:
                cursor = conn.cursor()
                ensure_chip_table(cursor)
                count = upsert_chip_rows(cursor, final_df.to_dict("records"))
                conn.commit()
                print(f"🎉 [最終補SQL] 成功寫入 / 更新 {count} 筆資料")
        except Exception as e:
            print(f"❌ [最終補SQL] 寫入失敗：{e}")
    else:
        print("📭 沒有資料需要補進 SQL。")

    print("\n========================================================")
    print("🎉 daily_chip_data 智慧同步完成")
    print("========================================================")
    print(f"📂 地端 CSV 總筆數：{len(final_df)}")
    print(f"✅ 網路任務成功數：{success_count}")
    print(f"⚠️ 網路任務失敗數：{fail_count}")

# =========================================================
# 🚀 總司令部：全自動智慧排程系統
# =========================================================
def main_scheduler():
    print("==========================================================")
    print(f"🚢 [旗艦巨獸] 啟動時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("==========================================================")

    now = datetime.now()

    run_daily_chip = now.weekday() < 5
    run_monthly_revenue = 1 <= now.day <= 12
    run_fundamentals = now.month in [3, 5, 8, 11] and now.day >= 15

    if run_daily_chip:
        print("\n🟢 [日更雷達] 今天是交易日，準備啟動【法人籌碼】收集車...")
        try:
            smart_sync_daily_chip()
        except Exception as e:
            print(f"❌ 每日法人籌碼模組失敗：{e}")
    else:
        print("\n⚪ [日更雷達] 週末休市，【法人籌碼】馬達休眠中。")

    if run_monthly_revenue:
        print("\n🟢 [月更雷達] 目前為營收公佈期 (1~12號)，準備啟動【月營收】雙引擎...")
        try:
            from monthly_revenue_simple import step3_download_revenue_to_csv, step4_import_revenue_to_sql
            step3_download_revenue_to_csv()
            step4_import_revenue_to_sql()
        except ImportError:
            print("⚠️ 找不到營收模組 (monthly_revenue_simple.py)，請確認檔案是否放在一起。")
        except Exception as e:
            print(f"❌ 月營收模組發生異常：{e}")
    else:
        print(f"\n⚪ [月更雷達] 今天是 {now.day} 號 (非 1~12 號)，【月營收】馬達休眠中。")

    if run_fundamentals:
        print("\n🟢 [季更雷達] 目前為財報公佈旺季，準備啟動【季財報】收集車...")
        try:
            pass
        except Exception as e:
            print(f"❌ 季財報模組發生異常：{e}")
    else:
        print("\n⚪ [季更雷達] 非財報集中公佈期，【季財報】馬達休眠中。")

    print("\n==========================================================")
    print("🏁 全產線自動化排程檢測完畢！")
    print("==========================================================")

if __name__ == "__main__":
    main_scheduler()