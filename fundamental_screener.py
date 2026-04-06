import pyodbc
import pandas as pd
from config import WATCH_LIST

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)

DEFAULT_DEFENSIVE_LIST = ["2330.TW", "2317.TW", "2454.TW", "2881.TW", "2603.TW"]


def get_vip_stock_pool():
    print("========================================")
    print("🕵️‍♂️ [機構級海關] 啟動基本面雙引擎體檢程序...")
    print("========================================")

    vip_pool = []

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = """
            WITH LatestRevenue AS (
                SELECT
                    [Ticker SYMBOL],
                    [單月營收年增率(%)],
                    [資料年月日],
                    ROW_NUMBER() OVER(
                        PARTITION BY [Ticker SYMBOL]
                        ORDER BY [資料年月日] DESC
                    ) AS rn
                FROM monthly_revenue_simple
            ),
            FinancialsBase AS (
                SELECT
                    [Ticker SYMBOL],
                    [資料年月日],
                    [毛利率(%)],
                    [營業利益率(%)],
                    [單季EPS],
                    [ROE(%)],
                    [稅後淨利率(%)],
                    [營業現金流],
                    [負債比率(%)],
                    [本業獲利比(%)]
                FROM fundamentals_clean
            ),
            FinancialsWithLag AS (
                SELECT
                    [Ticker SYMBOL],
                    [資料年月日],
                    [毛利率(%)],
                    [營業利益率(%)],
                    [單季EPS],
                    [ROE(%)],
                    [稅後淨利率(%)],
                    [營業現金流],
                    [負債比率(%)],
                    [本業獲利比(%)],
                    LAG([單季EPS], 1) OVER (
                        PARTITION BY [Ticker SYMBOL]
                        ORDER BY [資料年月日]
                    ) AS Prev_EPS,
                    LAG([單季EPS], 4) OVER (
                        PARTITION BY [Ticker SYMBOL]
                        ORDER BY [資料年月日]
                    ) AS Prev_Year_EPS,
                    LAG([毛利率(%)], 1) OVER (
                        PARTITION BY [Ticker SYMBOL]
                        ORDER BY [資料年月日]
                    ) AS Prev_Margin
                FROM FinancialsBase
            ),
            LatestFinancials AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER(
                        PARTITION BY [Ticker SYMBOL]
                        ORDER BY [資料年月日] DESC
                    ) AS rn
                FROM FinancialsWithLag
            )
            SELECT
                r.[Ticker SYMBOL],
                r.[單月營收年增率(%)],
                r.[資料年月日] AS Revenue_Date,
                f.[資料年月日] AS Financial_Date,
                f.[毛利率(%)],
                f.[營業利益率(%)],
                f.[單季EPS],
                f.[ROE(%)],
                f.[稅後淨利率(%)],
                f.[營業現金流],
                f.[負債比率(%)],
                f.[本業獲利比(%)],
                f.Prev_EPS,
                f.Prev_Year_EPS,
                f.Prev_Margin
            FROM (
                SELECT * FROM LatestRevenue WHERE rn = 1
            ) r
            INNER JOIN (
                SELECT * FROM LatestFinancials WHERE rn = 1
            ) f
                ON r.[Ticker SYMBOL] = f.[Ticker SYMBOL]
            """
            df = pd.read_sql(query, conn)

        if df.empty:
            print("⚠️ 基本面庫尚未建立足夠歷史，自動退回使用原名單。")
            return WATCH_LIST

        for _, row in df.iterrows():
            ticker = row["Ticker SYMBOL"]
            eps = row.get("單季EPS", 0)
            op_margin = row.get("營業利益率(%)", 0)
            yoy = row.get("單月營收年增率(%)", 0)
            margin = row.get("毛利率(%)", 0)
            roe = row.get("ROE(%)", 0)
            cash_flow = row.get("營業現金流", 1)

            prev_eps = row.get("Prev_EPS", 0)
            prev_year_eps = row.get("Prev_Year_EPS", 0)
            prev_margin = row.get("Prev_Margin", 0)

            debt_ratio = row.get("負債比率(%)", 50.0) if pd.notna(row.get("負債比率(%)")) else 50.0
            core_profit = row.get("本業獲利比(%)", 100.0) if pd.notna(row.get("本業獲利比(%)")) else 100.0

            if pd.isna(eps) or pd.isna(margin):
                continue

            # 第一層：排地雷
            if eps < 0:
                continue
            if op_margin < 0:
                continue
            if yoy < -10.0:
                continue
            if debt_ratio > 65.0:
                continue
            if core_profit < 50.0:
                continue

            # 第二層：挑強股
            cond_rev_growth = yoy > 20.0
            cond_eps_high = (eps >= (prev_year_eps if pd.notna(prev_year_eps) else 0)) and (eps >= (prev_eps if pd.notna(prev_eps) else 0))
            cond_roe = roe > 15.0 if pd.notna(roe) else False
            cond_margin_up = margin >= (prev_margin if pd.notna(prev_margin) else 0)
            cond_cash_flow = cash_flow > 0 if pd.notna(cash_flow) else False

            if cond_roe and cond_cash_flow and (cond_rev_growth or cond_margin_up or cond_eps_high):
                vip_pool.append(ticker)

        vip_pool = list(dict.fromkeys(vip_pool))
        print(f"✅ 體檢完畢！為您淘金出 {len(vip_pool)} 檔「體質強健、絕不虧損」的真飆股！")

        if len(vip_pool) == 0:
            print("⚠️ 今日無股票通過嚴格海關，啟動防禦機制，僅監控權值股。")
            return DEFAULT_DEFENSIVE_LIST

        return vip_pool

    except Exception as e:
        print(f"❌ 基本面篩選失敗: {e}")
        return WATCH_LIST


if __name__ == "__main__":
    vip_stocks = get_vip_stock_pool()
    print(f"\n🔥 今日 AI 最終核准之狙擊目標池：\n{vip_stocks}")
