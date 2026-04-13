# -*- coding: utf-8 -*-
"""v83 主線收編服務：daily_chip_etl.py 的核心 ETL/排程能力。

這支檔保留 legacy 引擎邏輯，但主線之後應優先直接 import 本 service。
原本的 daily_chip_etl.py 現在只保留相容入口(wrapper)。
"""

import os
from pathlib import Path
import json
import time
import requests
import urllib3
import pandas as pd
import pyodbc

from io import StringIO
from datetime import datetime, timedelta, time as dt_time
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
SCHEDULER_STATE_FILE = "daily_chip_scheduler_state.json"

DAYS_TO_FETCH = 10
SLEEP_BETWEEN_STOCKS = 0.8
ENABLE_BACKUP_SOURCE = True
SAVE_EVERY_N_STOCKS = 50

# 月營收 / 財報 本地檔
MONTHLY_REVENUE_CSV = "monthly_revenue_simple.csv"
FUNDAMENTALS_CSV = "market_financials_backup_fullspeed.csv"

# =========================================================
# ⏰ 可抓取時間設定
# =========================================================
DAILY_CHIP_READY_TIME = dt_time(17, 30)          # 法人籌碼：平日 17:30 後才上網抓
MONTHLY_REVENUE_READY_TIME = dt_time(18, 30)     # 月營收：每月 1~12 號 18:30 後
FUNDAMENTALS_READY_TIME = dt_time(19, 30)        # 季財報：3/5/8/11 月 15 號後 19:30 後

# =========================================================
# 🔐 FinMind Token
# =========================================================
try:
    from config import FINMIND_API_TOKEN as CONFIG_API_TOKEN
except ImportError:
    print("⚠️ 警告：找不到 config.py 或 FINMIND_API_TOKEN！請確認中央設定檔是否正確。")
    CONFIG_API_TOKEN = ""

API_TOKEN = (os.getenv("FINMIND_API_TOKEN", "") or CONFIG_API_TOKEN or "").strip()

# =========================================================
# 🗓️ 台股休市日
# =========================================================
TW_MARKET_HOLIDAYS = {
    "2026-01-01", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20",
    "2026-02-27", "2026-04-03", "2026-04-06", "2026-05-01", "2026-06-19", "2026-09-25", "2026-10-09",
}

CSV_COLUMNS = [
    "日期", "Ticker SYMBOL", "外資買賣超", "投信買賣超", "自營商買賣超", "三大法人合計", "資料來源"
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


def _candidate_local_paths(filename: str):
    base = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(base, filename),
        os.path.join(base, 'data', filename),
        os.path.join(base, 'seed_data', filename),
    ]
    # dedupe while preserving order
    seen = set()
    ordered = []
    for path in candidates:
        norm = os.path.normcase(os.path.normpath(path))
        if norm in seen:
            continue
        seen.add(norm)
        ordered.append(path)
    return ordered


def resolve_local_csv_path(filename: str):
    for path in _candidate_local_paths(filename):
        if os.path.exists(path):
            return path
    return None


# =========================================================
# 🛠️ 共用工具
# =========================================================
def today_str():
    return datetime.now().strftime("%Y-%m-%d")


def to_nullable_float(x):
    try:
        v = pd.to_numeric(x, errors="coerce")
        if pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def nullable_sum(*values):
    clean = [v for v in values if v is not None]
    return sum(clean) if clean else None


def load_scheduler_state():
    if not os.path.exists(SCHEDULER_STATE_FILE):
        return {}
    try:
        with open(SCHEDULER_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_scheduler_state(state: dict):
    try:
        with open(SCHEDULER_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ 排程狀態檔寫入失敗：{e}")


def mark_task_success(task_key: str):
    state = load_scheduler_state()
    state[task_key] = today_str()
    save_scheduler_state(state)


def should_run_task_today(task_key: str, ready_time: dt_time, date_ok: bool):
    now = datetime.now()
    state = load_scheduler_state()
    last_success_date = state.get(task_key)

    if not date_ok:
        return False, "日期條件未達成。"

    if now.time() < ready_time:
        return False, f"尚未到可抓取時間 {ready_time.strftime('%H:%M')}，目前 {now.strftime('%H:%M')}。"

    if last_success_date == today_str():
        return False, "今天已成功執行過。"

    return True, "符合條件，可立即補跑。"


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
# 📡 股票清單：對接中央名單樞紐
# =========================================================
def get_official_stock_list():
    try:
        from config import get_dynamic_watch_list
        target_list = get_dynamic_watch_list()
        print(f"🎯 從 config.py 讀取混合監控清單：共 {len(target_list)} 檔")
        
        # 籌碼 API 通常不需要 .TW/.TWO，做個字串清理後回傳
        return [str(t).replace(".TW", "").replace(".TWO", "") for t in target_list]
    except Exception as e:
        print(f"⚠️ 無法讀取動態名單，使用極限備援名單: {e}")
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

    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

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
    csv_path = resolve_local_csv_path(CSV_FILENAME)
    if not csv_path:
        print(f"📂 地端 CSV 不存在：{CSV_FILENAME}")
        return pd.DataFrame(columns=CSV_COLUMNS)

    try:
        df = pd.read_csv(
            csv_path,
            encoding="utf-8-sig",
            dtype={"日期": str, "Ticker SYMBOL": str}
        )
        df = normalize_chip_dataframe(df)
        df = df.drop_duplicates(subset=["日期", "Ticker SYMBOL"]).reset_index(drop=True)
        print(f"📂 已讀取地端 CSV：{csv_path} | {len(df)} 筆")
        return df
    except Exception as e:
        print(f"⚠️ 讀取地端 CSV 失敗，改視為空白檔案：{e}")
        return pd.DataFrame(columns=CSV_COLUMNS)


def save_csv(df):
    df = normalize_chip_dataframe(df)
    df = df.drop_duplicates(subset=["日期", "Ticker SYMBOL"]).reset_index(drop=True)
    target_path = resolve_local_csv_path(CSV_FILENAME) or os.path.join(os.path.dirname(os.path.abspath(__file__)), CSV_FILENAME)
    df.to_csv(target_path, index=False, encoding="utf-8-sig")
    print(f"💾 CSV 已更新：{target_path} | 共 {len(df)} 筆")


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
# 🚀 開機先補本地檔 → SQL
# =========================================================
def reconcile_local_chip_csv_to_sql():
    print("========================================================")
    print("🧩 開機先執行：法人籌碼 CSV → SQL 本地補資料")
    print("========================================================")

    local_csv_df = load_existing_csv()
    sql_keys = load_sql_existing_keys()

    if local_csv_df.empty:
        print("📂 法人籌碼本地 CSV 為空，沒有資料可補進 SQL。")
        return True

    csv_keys = set(
        (str(r["日期"]).strip(), str(r["Ticker SYMBOL"]).strip())
        for _, r in local_csv_df.iterrows()
    )
    csv_only_keys = csv_keys - sql_keys

    if not csv_only_keys:
        print("✅ 法人籌碼 CSV 與 SQL 之間沒有待補的舊資料。")
        return True

    csv_missing_in_sql_df = local_csv_df[
        local_csv_df.apply(
            lambda r: (str(r["日期"]).strip(), str(r["Ticker SYMBOL"]).strip()) in csv_only_keys,
            axis=1
        )
    ].copy()

    print(f"📥 發現法人籌碼 CSV 有但 SQL 沒有的資料：{len(csv_missing_in_sql_df)} 筆，先補進 SQL...")

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            ensure_chip_table(cursor)
            count = upsert_chip_rows(cursor, csv_missing_in_sql_df.to_dict("records"))
            conn.commit()
            print(f"🎉 [CSV補SQL] 成功寫入 / 更新 {count} 筆")
        return True
    except Exception as e:
        print(f"❌ [CSV補SQL] 寫入失敗：{e}")
        return False


def reconcile_local_monthly_revenue_csv_to_sql():
    print("========================================================")
    print("🧩 開機先執行：月營收 CSV → SQL 本地補資料")
    print("========================================================")

    monthly_path = resolve_local_csv_path(MONTHLY_REVENUE_CSV)
    if not monthly_path:
        print(f"📂 月營收本地 CSV 不存在：{MONTHLY_REVENUE_CSV}")
        return True

    try:
        from fts_etl_monthly_revenue_service import write_to_sql
        df = pd.read_csv(monthly_path, encoding="utf-8-sig")
        df = df.where(pd.notnull(df), None)
        if df.empty:
            print("📂 月營收本地 CSV 為空，無需補 SQL。")
            return True

        write_to_sql(df)
        print("🎉 月營收本地 CSV 已補進 SQL。")
        return True
    except Exception as e:
        print(f"❌ 月營收本地 CSV 補 SQL 失敗：{e}")
        return False


def reconcile_local_fundamentals_csv_to_sql():
    print("========================================================")
    print("🧩 開機先執行：季財報 CSV → SQL 本地補資料")
    print("========================================================")

    fundamentals_path = resolve_local_csv_path(FUNDAMENTALS_CSV)
    if not fundamentals_path:
        print(f"📂 季財報本地 CSV 不存在：{FUNDAMENTALS_CSV}")
        return True
    print(f"📂 季財報本地 CSV 已找到：{fundamentals_path}")

    try:
        from fts_fundamentals_etl_mainline import FundamentalsETLMainline

        mainline = FundamentalsETLMainline()
        df = mainline.load_existing_csv(Path(fundamentals_path))
        if df is None or df.empty:
            print("📂 季財報本地 CSV 為空，無需補 SQL。")
            return True

        sql_result = mainline.import_df_to_sql(df)
        if sql_result.get("sql_error"):
            raise RuntimeError(sql_result.get("sql_error"))

        print(f"🎉 季財報本地 CSV 已補進 SQL。共 {sql_result.get('sql_imported_rows', 0)} 筆")
        return True
    except Exception as e:
        print(f"❌ 季財報本地 CSV 補 SQL 失敗：{e}")
        return False


# =========================================================
# 🧠 FinMind 初始化
# =========================================================
def init_finmind_loader(api_token: str):
    if not api_token:
        raise ValueError("尚未設定 FINMIND_API_TOKEN")

    dl = DataLoader()

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
# 🧠 FinMind 來源（缺值保留 NULL）
# =========================================================
def fetch_chip_from_finmind(dl, stock_id, start_dt):
    try:
        chip_df = dl.taiwan_stock_institutional_investors(
            stock_id=stock_id,
            start_date=start_dt
        )
    except TypeError:
        chip_df = dl.taiwan_stock_institutional_investors(
            stock_id=stock_id,
            start_date=start_dt,
            end_date=datetime.now().strftime("%Y-%m-%d")
        )

    if chip_df is None or chip_df.empty:
        return None

    chip_df = chip_df.copy()
    chip_df["date"] = chip_df["date"].astype(str).str[:10]
    chip_df["buy"] = pd.to_numeric(chip_df["buy"], errors="coerce")
    chip_df["sell"] = pd.to_numeric(chip_df["sell"], errors="coerce")
    chip_df["Net"] = chip_df["buy"] - chip_df["sell"]

    foreign = chip_df[chip_df["name"] == "Foreign_Investor"].groupby("date")["Net"].sum(min_count=1)
    trust = chip_df[chip_df["name"] == "Investment_Trust"].groupby("date")["Net"].sum(min_count=1)
    dealers = chip_df[chip_df["name"].isin(["Dealer_self", "Dealer_Hedging"])].groupby("date")["Net"].sum(min_count=1)

    rows = []
    dates = sorted(chip_df["date"].dropna().unique())

    for date_str in dates:
        if not is_tw_trading_day(date_str):
            continue

        f_net = to_nullable_float(foreign.get(date_str, None))
        t_net = to_nullable_float(trust.get(date_str, None))
        d_net = to_nullable_float(dealers.get(date_str, None))
        total_net = nullable_sum(f_net, t_net, d_net)

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
# 🛟 官方備援（缺值保留 NULL）
# =========================================================
def roc_year(ad_year):
    return ad_year - 1911


def try_parse_csv_text_to_df(text):
    if not text or not text.strip():
        return pd.DataFrame()

    lines = [line.strip("\ufeff").strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return pd.DataFrame()

    header_idx = None
    for i, line in enumerate(lines):
        line_no_space = line.replace(" ", "")
        if ("證券代號" in line_no_space or "代號" in line_no_space) and line.count(",") >= 3:
            header_idx = i
            break

    if header_idx is not None:
        csv_text = "\n".join(lines[header_idx:])
    else:
        candidates = [line for line in lines if line.count(",") >= 3]
        csv_text = "\n".join(candidates)

    if not csv_text.strip():
        return pd.DataFrame()

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
            result[col] = pd.to_numeric(result[col], errors="coerce")

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
                result[col] = pd.to_numeric(result[col], errors="coerce")

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

                    f_net = to_nullable_float(row["foreign"])
                    t_net = to_nullable_float(row["trust"])
                    d_net = to_nullable_float(row["dealer"])
                    total_net = nullable_sum(f_net, t_net, d_net)

                    backup_map[(stock_id, date_str)] = {
                        "日期": date_str,
                        "Ticker SYMBOL": f"{stock_id}{suffix}",
                        "外資買賣超": f_net,
                        "投信買賣超": t_net,
                        "自營商買賣超": d_net,
                        "三大法人合計": total_net,
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

        cursor.execute(sql, (
            date_str, ticker_symbol,
            row["外資買賣超"], row["投信買賣超"], row["自營商買賣超"], row["三大法人合計"], row["資料來源"], date_str, ticker_symbol,
            date_str, ticker_symbol, row["外資買賣超"], row["投信買賣超"], row["自營商買賣超"], row["三大法人合計"], row["資料來源"]
        ))
        count += 1

    return count


# =========================================================
# 🚀 法人籌碼：上網抓新資料
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


def download_and_sync_new_chip_data():
    print("========================================================")
    print("🌐 啟動法人籌碼上網補抓：只處理缺漏新資料")
    print("========================================================")

    if not API_TOKEN:
        print("⚠️ 尚未設定 FINMIND_API_TOKEN，請先設定後再執行")
        return False

    start_dt = (datetime.now() - timedelta(days=DAYS_TO_FETCH)).strftime("%Y-%m-%d")
    target_dates = build_target_dates(start_dt)

    if not target_dates:
        print("⚠️ 目前目標區間內沒有交易日，停止執行。")
        return False

    print(f"📅 目標交易日期區間：{target_dates[0]} ~ {target_dates[-1]}")
    print(f"📆 本次有效交易日數：{len(target_dates)}")

    local_csv_df = load_existing_csv()
    sql_keys = load_sql_existing_keys()
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
            return False
    else:
        print("📭 沒有資料需要補進 SQL。")

    print("\n========================================================")
    print("🎉 法人籌碼上網補抓完成")
    print("========================================================")
    print(f"📂 地端 CSV 總筆數：{len(final_df)}")
    print(f"✅ 網路任務成功數：{success_count}")
    print(f"⚠️ 網路任務失敗數：{fail_count}")
    return True


# =========================================================
# 🚀 其他模組：上網抓新資料
# =========================================================
def run_monthly_revenue_module():
    try:
        from fts_etl_monthly_revenue_service import main as monthly_revenue_main
        monthly_revenue_main()
        print("✅ 月營收模組執行完成。")
        return True
    except ImportError as e:
        print(f"⚠️ 找不到營收服務模組 (fts_etl_monthly_revenue_service.py) 或匯入失敗：{e}")
        return False
    except Exception as e:
        print(f"❌ 月營收模組發生異常：{e}")
        return False


def run_fundamentals_module():
    try:
        from fts_service_api import fundamentals_smart_sync as yahoo_fundamentals_sync
        yahoo_fundamentals_sync()
        print("✅ 季財報模組執行完成。")
        return True
    except ImportError as e:
        print(f"⚠️ 找不到財報服務模組 (fts_fundamentals_etl_mainline.py) 或匯入失敗：{e}")
        return False
    except Exception as e:
        print(f"❌ 季財報模組發生異常：{e}")
        return False


# =========================================================
# 🚀 總司令部：全自動智慧排程系統
# =========================================================
def main_scheduler():
    print("==========================================================")
    print(f"🚢 [旗艦巨獸] 啟動時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("==========================================================")

    # 0. 開機先補三個本地檔 → SQL
    reconcile_local_chip_csv_to_sql()
    reconcile_local_monthly_revenue_csv_to_sql()
    reconcile_local_fundamentals_csv_to_sql()

    now = datetime.now()

    # 1. 法人籌碼：平日 + 17:30 後，才上網抓新資料
    chip_date_ok = now.weekday() < 5
    should_run_chip, chip_reason = should_run_task_today(
        task_key="daily_chip",
        ready_time=DAILY_CHIP_READY_TIME,
        date_ok=chip_date_ok
    )

    if should_run_chip:
        print(f"\n🟢 [日更雷達] 法人籌碼上網補抓：{chip_reason}")
        ok = download_and_sync_new_chip_data()
        if ok:
            mark_task_success("daily_chip")
    else:
        print(f"\n⚪ [日更雷達] 法人籌碼上網補抓：{chip_reason}")

    # 2. 月營收：每月 1~12 號 + 18:30 後
    revenue_date_ok = 1 <= now.day <= 12
    should_run_revenue, revenue_reason = should_run_task_today(
        task_key="monthly_revenue",
        ready_time=MONTHLY_REVENUE_READY_TIME,
        date_ok=revenue_date_ok,
    )

    if should_run_revenue:
        print(f"\n🟢 [月更雷達] 月營收上網補抓：{revenue_reason}")
        ok = run_monthly_revenue_module()
        if ok:
            mark_task_success("monthly_revenue")
    else:
        print(f"\n⚪ [月更雷達] 月營收上網補抓：{revenue_reason}")

    # 3. 季財報：3/5/8/11 月且 15 號後 + 19:30 後
    fundamentals_date_ok = now.month in [3, 5, 8, 11] and now.day >= 15
    should_run_fundamentals, fundamentals_reason = should_run_task_today(
        task_key="fundamentals",
        ready_time=FUNDAMENTALS_READY_TIME,
        date_ok=fundamentals_date_ok,
    )

    if should_run_fundamentals:
        print(f"\n🟢 [季更雷達] 季財報上網補抓：{fundamentals_reason}")
        ok = run_fundamentals_module()
        if ok:
            mark_task_success("fundamentals")
    else:
        print(f"\n⚪ [季更雷達] 季財報上網補抓：{fundamentals_reason}")

    print("\n==========================================================")
    print("🏁 全產線自動化排程檢測完畢！")
    print("==========================================================")


if __name__ == "__main__":
    main_scheduler()


# v83 mainline alias
run_daily_chip_job = main_scheduler
