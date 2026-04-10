import yfinance as yf
import pandas as pd
import pyodbc
import time
import os
import requests
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================
# ⚙️ 基本設定
# =========================================================
DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;DATABASE=股票online;Trusted_Connection=yes;'
)

CSV_FILENAME = 'market_financials_backup_fullspeed.csv'
TABLE_NAME = 'fundamentals_clean'

# 🌟 模式 B：完整版
ENABLE_DIVIDEND_YIELD = True

# 🌟 智慧版下載設定
MAX_WORKERS = 2
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_TASKS = 1.5
BATCH_COMMIT_SIZE = 200
SAVE_EVERY_N_STOCKS = 50

# 每檔股票希望保留最近幾期季報
TARGET_REPORTS_PER_STOCK = 2

# CSV/SQL 欄位
DATA_COLUMNS = [
    'Ticker SYMBOL',
    '資料年月日',
    '毛利率(%)',
    '營業利益率(%)',
    '單季EPS',
    'ROE(%)',
    '稅後淨利率(%)',
    '營業現金流',
    '預估殖利率(%)',
    '負債比率(%)',
    '本業獲利比(%)'
]


# =========================================================
# 🌟 建立 requests Session（含重試）
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


def safe_get_json(url, timeout=REQUEST_TIMEOUT):
    session = build_session()

    try:
        res = session.get(url, timeout=timeout, verify=True)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.SSLError as e:
        print(f"  ⚠️ SSL 驗證失敗，改用容錯模式重試: {url}")
        try:
            res = session.get(url, timeout=timeout, verify=False)
            res.raise_for_status()
            return res.json()
        except Exception as e2:
            raise Exception(f"SSL 容錯重試仍失敗: {e2}") from e
    except ValueError as e:
        raise Exception(f"回傳內容不是有效 JSON: {url} | {e}")
    except Exception as e:
        raise Exception(f"請求失敗: {url} | {e}")


# =========================================================
# 📡 官方股票清單
# =========================================================
def get_official_stock_list():
    print("📡 正在向【證交所/櫃買中心】官方伺服器請求全市場名單...")

    twse_url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    tpex_url = "https://www.tpex.org.tw/openapi/v1/t187ap03_O"

    all_stocks = []

    # 上市
    try:
        twse_data = safe_get_json(twse_url)
        if isinstance(twse_data, list):
            twse_stocks = [str(item.get('公司代號', '')).strip() for item in twse_data]
            twse_stocks = [s for s in twse_stocks if len(s) == 4 and s.isdigit()]
            all_stocks.extend(twse_stocks)
            print(f"  ✅ 成功取得 {len(twse_stocks)} 檔上市股票。")
        else:
            print("  ⚠️ 上市 API 回傳格式異常，略過。")
    except Exception as e:
        print(f"  ❌ 上市名單抓取失敗: {e}")

    # 上櫃
    try:
        tpex_data = safe_get_json(tpex_url)
        if isinstance(tpex_data, list):
            tpex_stocks = [str(item.get('公司代號', '')).strip() for item in tpex_data]
            tpex_stocks = [s for s in tpex_stocks if len(s) == 4 and s.isdigit()]
            all_stocks.extend(tpex_stocks)
            print(f"  ✅ 成功取得 {len(tpex_stocks)} 檔上櫃股票。")
        else:
            print("  ⚠️ 上櫃 API 回傳格式異常，略過。")
    except Exception as e:
        print(f"  ❌ 上櫃名單抓取失敗: {e}")

    pure_stocks = sorted(list(set(all_stocks)))
    if pure_stocks:
        print(f"🎯 成功鎖定全市場共 {len(pure_stocks)} 檔標的！\n")
        return pure_stocks

    print("⚠️ 官方名單全部失敗，改用備用名單：2330 / 2317 / 2454")
    return ['2330', '2317', '2454']


# =========================================================
# 🛠️ 工具
# =========================================================
def safe_float(x):
    try:
        if x is None or pd.isna(x):
            return None
        return round(float(x), 2)
    except:
        return None


def safe_int(x):
    try:
        if x is None or pd.isna(x):
            return None
        return int(float(x))
    except:
        return None


def sql_float_or_none(x):
    try:
        if x is None or pd.isna(x):
            return None
        return round(float(x), 2)
    except:
        return None


def sql_int_or_none(x):
    try:
        if x is None or pd.isna(x):
            return None
        return int(float(x))
    except:
        return None


def get_safe_value(df, key, date_col):
    if df is not None and not df.empty and key in df.index and date_col in df.columns:
        val = df.loc[key, date_col]
        return val if pd.notna(val) else None
    return None


def to_ymd(date_col):
    try:
        return pd.to_datetime(date_col).strftime("%Y-%m-%d")
    except:
        return None


def normalize_dataframe(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=DATA_COLUMNS)

    for col in DATA_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[DATA_COLUMNS].copy()
    df = df.where(pd.notnull(df), None)
    return df


def get_stock_id_from_ticker_symbol(ticker_symbol):
    return str(ticker_symbol).split('.')[0].strip()


# =========================================================
# 🛠️ SQL Table / 欄位檢查
# =========================================================
def ensure_sql_table(cursor):
    cursor.execute(f"""
    IF OBJECT_ID(N'{TABLE_NAME}', N'U') IS NULL
    BEGIN
        CREATE TABLE {TABLE_NAME} (
            [Ticker SYMBOL] NVARCHAR(20),
            [資料年月日] DATE,
            [毛利率(%)] DECIMAL(18,2),
            [營業利益率(%)] DECIMAL(18,2),
            [單季EPS] DECIMAL(18,2),
            [ROE(%)] DECIMAL(18,2),
            [稅後淨利率(%)] DECIMAL(18,2),
            [營業現金流] BIGINT,
            [預估殖利率(%)] DECIMAL(18,2),
            [負債比率(%)] DECIMAL(18,2),
            [本業獲利比(%)] DECIMAL(18,2),
            [更新時間] DATETIME
        )
    END
    """)


def ensure_sql_columns(cursor):
    sqls = [
        f"IF COL_LENGTH('{TABLE_NAME}', N'Ticker SYMBOL') IS NULL ALTER TABLE {TABLE_NAME} ADD [Ticker SYMBOL] NVARCHAR(20)",
        f"IF COL_LENGTH('{TABLE_NAME}', N'資料年月日') IS NULL ALTER TABLE {TABLE_NAME} ADD [資料年月日] DATE",
        f"IF COL_LENGTH('{TABLE_NAME}', N'毛利率(%)') IS NULL ALTER TABLE {TABLE_NAME} ADD [毛利率(%)] DECIMAL(18,2)",
        f"IF COL_LENGTH('{TABLE_NAME}', N'營業利益率(%)') IS NULL ALTER TABLE {TABLE_NAME} ADD [營業利益率(%)] DECIMAL(18,2)",
        f"IF COL_LENGTH('{TABLE_NAME}', N'單季EPS') IS NULL ALTER TABLE {TABLE_NAME} ADD [單季EPS] DECIMAL(18,2)",
        f"IF COL_LENGTH('{TABLE_NAME}', N'ROE(%)') IS NULL ALTER TABLE {TABLE_NAME} ADD [ROE(%)] DECIMAL(18,2)",
        f"IF COL_LENGTH('{TABLE_NAME}', N'稅後淨利率(%)') IS NULL ALTER TABLE {TABLE_NAME} ADD [稅後淨利率(%)] DECIMAL(18,2)",
        f"IF COL_LENGTH('{TABLE_NAME}', N'營業現金流') IS NULL ALTER TABLE {TABLE_NAME} ADD [營業現金流] BIGINT",
        f"IF COL_LENGTH('{TABLE_NAME}', N'預估殖利率(%)') IS NULL ALTER TABLE {TABLE_NAME} ADD [預估殖利率(%)] DECIMAL(18,2)",
        f"IF COL_LENGTH('{TABLE_NAME}', N'負債比率(%)') IS NULL ALTER TABLE {TABLE_NAME} ADD [負債比率(%)] DECIMAL(18,2)",
        f"IF COL_LENGTH('{TABLE_NAME}', N'本業獲利比(%)') IS NULL ALTER TABLE {TABLE_NAME} ADD [本業獲利比(%)] DECIMAL(18,2)",
        f"IF COL_LENGTH('{TABLE_NAME}', N'更新時間') IS NULL ALTER TABLE {TABLE_NAME} ADD [更新時間] DATETIME"
    ]

    for sql in sqls:
        cursor.execute(sql)


# =========================================================
# 📂 地端 CSV
# =========================================================
def load_existing_csv():
    if not os.path.exists(CSV_FILENAME):
        print(f"📂 地端 CSV 不存在：{CSV_FILENAME}")
        return pd.DataFrame(columns=DATA_COLUMNS)

    try:
        df = pd.read_csv(CSV_FILENAME, encoding='utf-8-sig')
        df = normalize_dataframe(df)
        df = df.drop_duplicates(subset=['Ticker SYMBOL', '資料年月日']).reset_index(drop=True)
        print(f"📂 已讀取地端 CSV：{CSV_FILENAME} | {len(df)} 筆")
        return df
    except Exception as e:
        print(f"⚠️ 讀取地端 CSV 失敗，改視為空白檔案：{e}")
        return pd.DataFrame(columns=DATA_COLUMNS)


def save_csv(df):
    df = normalize_dataframe(df)
    df = df.drop_duplicates(subset=['Ticker SYMBOL', '資料年月日']).reset_index(drop=True)
    df.to_csv(CSV_FILENAME, index=False, encoding='utf-8-sig')
    print(f"💾 CSV 已更新：{CSV_FILENAME} | 共 {len(df)} 筆")


def flush_new_rows_to_csv(local_csv_df, new_rows, note="批次存檔"):
    """
    把暫存 new_rows 併回 local_csv_df 並寫入 CSV
    回傳更新後的 local_csv_df
    """
    if not new_rows:
        return local_csv_df

    temp_df = pd.DataFrame(new_rows, columns=DATA_COLUMNS)
    temp_df = normalize_dataframe(temp_df)

    merged_df = pd.concat([local_csv_df, temp_df], ignore_index=True)
    merged_df = normalize_dataframe(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['Ticker SYMBOL', '資料年月日']).reset_index(drop=True)

    save_csv(merged_df)
    print(f"💾 [{note}] 已批次存檔 {len(temp_df)} 筆新資料")

    return merged_df


# =========================================================
# 🗄️ 讀取 SQL 既有 Key
# =========================================================
def load_sql_existing_keys():
    existing_keys = set()

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()

            ensure_sql_table(cursor)
            ensure_sql_columns(cursor)
            conn.commit()

            cursor.execute(f"""
                SELECT [Ticker SYMBOL], CONVERT(VARCHAR(10), [資料年月日], 23)
                FROM {TABLE_NAME}
                WHERE [Ticker SYMBOL] IS NOT NULL
                  AND [資料年月日] IS NOT NULL
            """)

            rows = cursor.fetchall()
            for row in rows:
                existing_keys.add((str(row[0]).strip(), str(row[1]).strip()))

        print(f"🗄️ 已讀取 SQL 既有資料鍵值：{len(existing_keys)} 筆")
        return existing_keys

    except Exception as e:
        print(f"⚠️ 讀取 SQL 既有資料失敗，視為空表：{e}")
        return set()


# =========================================================
# 📊 覆蓋狀況分析
# =========================================================
def build_union_key_set(csv_df, sql_keys):
    csv_keys = set()
    if csv_df is not None and not csv_df.empty:
        csv_keys = set((str(r['Ticker SYMBOL']).strip(), str(r['資料年月日']).strip()) for _, r in csv_df.iterrows())

    return csv_keys.union(sql_keys)


def build_stock_date_map_from_keys(key_set):
    stock_date_map = {}

    for ticker_symbol, ymd in key_set:
        stock_id = get_stock_id_from_ticker_symbol(ticker_symbol)
        stock_date_map.setdefault(stock_id, set()).add(ymd)

    return stock_date_map


# =========================================================
# 🧠 單一股票補抓器
# =========================================================
def fetch_single_stock(stock_id, existing_union_keys, stock_existing_date_count):
    """
    智慧邏輯：
    - 如果這檔股票現有資料已達 TARGET_REPORTS_PER_STOCK，直接略過
    - 不夠才上網抓
    - 抓回來後只保留「地端 CSV + SQL 都沒有」的新資料
    """
    if stock_existing_date_count >= TARGET_REPORTS_PER_STOCK:
        return {
            'stock_id': stock_id,
            'success': True,
            'rows': [],
            'message': f"{stock_id} 已有 {stock_existing_date_count} 期，略過網路下載"
        }

    possible_suffixes = [".TW", ".TWO"]
    last_error = None

    for suffix in possible_suffixes:
        ticker_symbol = f"{stock_id}{suffix}"

        try:
            ticker = yf.Ticker(ticker_symbol)

            is_df = ticker.quarterly_financials
            bs_df = ticker.quarterly_balance_sheet
            cf_df = ticker.quarterly_cashflow

            if is_df is None or is_df.empty:
                last_error = f"{ticker_symbol} quarterly_financials 為空"
                continue

            div_yield = None
            if ENABLE_DIVIDEND_YIELD:
                try:
                    info = ticker.info
                    div_yield_raw = info.get('dividendYield', None)
                    div_yield = round(div_yield_raw * 100, 2) if div_yield_raw is not None else None
                except Exception as e:
                    last_error = f"{ticker_symbol} info 讀取失敗: {e}"
                    div_yield = None

            rows = []

            for date_col in is_df.columns[:TARGET_REPORTS_PER_STOCK]:
                report_date = to_ymd(date_col)
                if not report_date:
                    continue

                key = (ticker_symbol, report_date)
                if key in existing_union_keys:
                    continue

                revenue = get_safe_value(is_df, 'Total Revenue', date_col)
                gross_profit = get_safe_value(is_df, 'Gross Profit', date_col)
                op_income = get_safe_value(is_df, 'Operating Income', date_col)
                pre_tax = get_safe_value(is_df, 'Pretax Income', date_col)
                net_income = get_safe_value(is_df, 'Net Income', date_col)
                eps = get_safe_value(is_df, 'Diluted EPS', date_col)
                assets = get_safe_value(bs_df, 'Total Assets', date_col)
                liabilities = get_safe_value(bs_df, 'Total Liabilities Net Minority Interest', date_col)
                equity = get_safe_value(bs_df, 'Total Equity Gross Minority Interest', date_col)
                cash_flow = get_safe_value(cf_df, 'Operating Cash Flow', date_col)

                gross_margin = round((gross_profit / revenue * 100), 2) \
                    if revenue not in [None, 0] and gross_profit is not None else None

                op_margin = round((op_income / revenue * 100), 2) \
                    if revenue not in [None, 0] and op_income is not None else None

                net_margin = round((net_income / revenue * 100), 2) \
                    if revenue not in [None, 0] and net_income is not None else None

                roe = round((net_income / equity * 100), 2) \
                    if equity not in [None, 0] and net_income is not None else None

                debt_ratio = round((liabilities / assets * 100), 2) \
                    if assets not in [None, 0] and liabilities is not None else None

                core_profit_ratio = round((op_income / pre_tax * 100), 2) \
                    if pre_tax not in [None, 0] and op_income is not None else None

                rows.append({
                    'Ticker SYMBOL': ticker_symbol,
                    '資料年月日': report_date,
                    '毛利率(%)': safe_float(gross_margin),
                    '營業利益率(%)': safe_float(op_margin),
                    '單季EPS': safe_float(eps),
                    'ROE(%)': safe_float(roe),
                    '稅後淨利率(%)': safe_float(net_margin),
                    '營業現金流': safe_int(cash_flow),
                    '預估殖利率(%)': safe_float(div_yield),
                    '負債比率(%)': safe_float(debt_ratio),
                    '本業獲利比(%)': safe_float(core_profit_ratio)
                })

            time.sleep(SLEEP_BETWEEN_TASKS)

            return {
                'stock_id': stock_id,
                'success': True,
                'rows': rows,
                'message': f"{ticker_symbol} 補抓完成，新資料 {len(rows)} 筆"
            }

        except Exception as e:
            last_error = f"{ticker_symbol} 發生錯誤: {e}"
            time.sleep(SLEEP_BETWEEN_TASKS)
            continue

    return {
        'stock_id': stock_id,
        'success': False,
        'rows': [],
        'message': last_error or f"{stock_id} 無法取得財報資料"
    }


# =========================================================
# 📤 將 DataFrame 寫入 SQL（UPSERT）
# =========================================================
def import_df_to_sql(df, stage_name="SQL匯入"):
    df = normalize_dataframe(df)

    if df.empty:
        print(f"📭 [{stage_name}] 沒有需要寫入 SQL 的資料。")
        return 0

    float_cols = [
        '毛利率(%)',
        '營業利益率(%)',
        '單季EPS',
        'ROE(%)',
        '稅後淨利率(%)',
        '預估殖利率(%)',
        '負債比率(%)',
        '本業獲利比(%)'
    ]

    for col in float_cols:
        if col in df.columns:
            df[col] = df[col].apply(sql_float_or_none)

    if '營業現金流' in df.columns:
        df['營業現金流'] = df['營業現金流'].apply(sql_int_or_none)

    processed_count = 0

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()

            ensure_sql_table(cursor)
            ensure_sql_columns(cursor)
            conn.commit()

            for _, row in df.iterrows():
                ticker = row['Ticker SYMBOL']
                ymd = row['資料年月日']

                gm = sql_float_or_none(row['毛利率(%)'])
                op = sql_float_or_none(row['營業利益率(%)'])
                eps = sql_float_or_none(row['單季EPS'])
                roe = sql_float_or_none(row['ROE(%)'])
                nm = sql_float_or_none(row['稅後淨利率(%)'])
                cf = sql_int_or_none(row['營業現金流'])
                yield_val = sql_float_or_none(row['預估殖利率(%)'])
                debt = sql_float_or_none(row['負債比率(%)'])
                core = sql_float_or_none(row['本業獲利比(%)'])

                try:
                    cursor.execute(f"""
                        IF EXISTS (
                            SELECT 1
                            FROM {TABLE_NAME}
                            WHERE [Ticker SYMBOL] = ? AND [資料年月日] = ?
                        )
                        BEGIN
                            UPDATE {TABLE_NAME}
                            SET [毛利率(%)] = ?,
                                [營業利益率(%)] = ?,
                                [單季EPS] = ?,
                                [ROE(%)] = ?,
                                [稅後淨利率(%)] = ?,
                                [營業現金流] = ?,
                                [預估殖利率(%)] = ?,
                                [負債比率(%)] = ?,
                                [本業獲利比(%)] = ?,
                                [更新時間] = GETDATE()
                            WHERE [Ticker SYMBOL] = ? AND [資料年月日] = ?
                        END
                        ELSE
                        BEGIN
                            INSERT INTO {TABLE_NAME}
                            (
                                [Ticker SYMBOL],
                                [資料年月日],
                                [毛利率(%)],
                                [營業利益率(%)],
                                [單季EPS],
                                [ROE(%)],
                                [稅後淨利率(%)],
                                [營業現金流],
                                [預估殖利率(%)],
                                [負債比率(%)],
                                [本業獲利比(%)],
                                [更新時間]
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
                        END
                    """, (
                        ticker, ymd,
                        gm, op, eps, roe, nm, cf, yield_val, debt, core, ticker, ymd,
                        ticker, ymd, gm, op, eps, roe, nm, cf, yield_val, debt, core
                    ))

                    processed_count += 1

                    if processed_count % BATCH_COMMIT_SIZE == 0:
                        conn.commit()
                        print(f"⏳ [{stage_name}] 已處理 {processed_count} 筆")

                except Exception as row_err:
                    print(f"❌ [{stage_name}] 單筆寫入失敗: {ticker} | {ymd} | {row_err}")

            conn.commit()
            print(f"🎉 [{stage_name}] 成功寫入 / 更新 {processed_count} 筆資料到 SQL。")
            return processed_count

    except pyodbc.Error as e:
        print(f"❌ [{stage_name}] SQL Server 連線或寫入失敗: {e}")
        return 0


# =========================================================
# 🧩 智慧同步主流程
# =========================================================
def smart_sync():
    print("========================================================")
    print("🧠 啟動智慧版：先讀地端 CSV / SQL，再補抓缺漏資料")
    print("========================================================")

    # 1) 讀地端 CSV
    local_csv_df = load_existing_csv()

    # 2) 讀 SQL 既有資料 key
    sql_keys = load_sql_existing_keys()

    # 3) 先把「CSV 有、SQL 沒有」的資料補進 SQL
    if not local_csv_df.empty:
        csv_keys = set((str(r['Ticker SYMBOL']).strip(), str(r['資料年月日']).strip()) for _, r in local_csv_df.iterrows())
        csv_only_keys = csv_keys - sql_keys

        if csv_only_keys:
            csv_missing_in_sql_df = local_csv_df[
                local_csv_df.apply(lambda r: (str(r['Ticker SYMBOL']).strip(), str(r['資料年月日']).strip()) in csv_only_keys, axis=1)
            ].copy()

            print(f"📥 發現 CSV 有但 SQL 沒有的資料：{len(csv_missing_in_sql_df)} 筆，先補進 SQL...")
            import_df_to_sql(csv_missing_in_sql_df, stage_name="CSV補SQL")

            # 重新讀取 SQL keys，讓後面判斷更準
            sql_keys = load_sql_existing_keys()
        else:
            print("✅ CSV 與 SQL 之間沒有待補的舊資料。")
    else:
        print("📂 地端 CSV 為空，略過 CSV 補 SQL。")

    # 4) 建立目前聯集 key（CSV + SQL）
    union_keys = build_union_key_set(local_csv_df, sql_keys)
    stock_date_map = build_stock_date_map_from_keys(union_keys)

    # 5) 股票清單
    target_stocks = get_official_stock_list()

    # 6) 篩出缺漏股票
    need_fetch_stocks = []
    skip_count = 0

    for stock_id in target_stocks:
        existing_count = len(stock_date_map.get(stock_id, set()))
        if existing_count < TARGET_REPORTS_PER_STOCK:
            need_fetch_stocks.append(stock_id)
        else:
            skip_count += 1

    print(f"✅ 已完整覆蓋股票數：{skip_count}")
    print(f"🌐 需要上網補抓股票數：{len(need_fetch_stocks)}")

    # 7) 多執行緒補抓缺漏
    new_rows = []
    fetch_success = 0
    fetch_fail = 0
    since_last_save = 0

    if need_fetch_stocks:
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {}
            for stock_id in need_fetch_stocks:
                existing_count = len(stock_date_map.get(stock_id, set()))
                future = executor.submit(fetch_single_stock, stock_id, union_keys, existing_count)
                future_map[future] = stock_id

            completed = 0
            total = len(future_map)

            for future in as_completed(future_map):
                stock_id = future_map[future]
                completed += 1
                since_last_save += 1

                try:
                    result = future.result()

                    if result['success']:
                        if result['rows']:
                            new_rows.extend(result['rows'])
                            print(f"✅ [{completed}/{total}] {stock_id} 補抓成功，新資料 {len(result['rows'])} 筆")
                        else:
                            print(f"⏭️ [{completed}/{total}] {stock_id} 已完整，無需新增")
                        fetch_success += 1
                    else:
                        fetch_fail += 1
                        print(f"⏭️ [{completed}/{total}] {stock_id} 失敗：{result['message']}")

                except Exception as e:
                    fetch_fail += 1
                    print(f"⚠️ [{completed}/{total}] {stock_id} 執行失敗：{e}")

                # ===== 每 50 檔就先存一次 CSV =====
                if since_last_save >= SAVE_EVERY_N_STOCKS:
                    if new_rows:
                        local_csv_df = flush_new_rows_to_csv(
                            local_csv_df,
                            new_rows,
                            note=f"每 {SAVE_EVERY_N_STOCKS} 檔自動存檔"
                        )

                        current_keys = set(
                            (str(r['Ticker SYMBOL']).strip(), str(r['資料年月日']).strip())
                            for _, r in local_csv_df.iterrows()
                        )
                        union_keys = current_keys.union(sql_keys)
                        stock_date_map = build_stock_date_map_from_keys(union_keys)

                        new_rows = []

                    since_last_save = 0

        elapsed = time.time() - start_time
        print(f"⏱️ 網路補抓耗時：{elapsed:.2f} 秒")
    else:
        print("🎯 所有股票都已達到本地覆蓋條件，這次不需要上網下載。")

    # 8) 收尾：把最後不足 50 檔的暫存資料也存檔
    if new_rows:
        local_csv_df = flush_new_rows_to_csv(local_csv_df, new_rows, note="最後收尾存檔")

    # 9) 重新讀取這次最終 CSV 內容（保險）
    final_df = load_existing_csv()

    # 10) 將整份 CSV 再補進 SQL
    if not final_df.empty:
        import_df_to_sql(final_df, stage_name="最終補SQL")
    else:
        print("📭 沒有資料需要補進 SQL。")

    print("\n========================================================")
    print("🎉 智慧同步完成")
    print("========================================================")
    print(f"📂 地端 CSV 總筆數：{len(final_df)}")
    print(f"✅ 網路任務成功數：{fetch_success}")
    print(f"⚠️ 網路任務失敗數：{fetch_fail}")


# =========================================================
# 🚀 主程式
# =========================================================
if __name__ == "__main__":
    smart_sync()