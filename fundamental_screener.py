import os
import warnings
import pyodbc
import pandas as pd
from config import WATCH_LIST
from fts_sql_table_name_map import sql_table

warnings.filterwarnings("ignore", category=UserWarning)

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)

DEFAULT_DEFENSIVE_LIST = ["2330.TW", "2317.TW", "2454.TW", "2881.TW", "2603.TW"]
CSV_OUTPUT_PATH = "data/stock_list_cache_listed.csv"

TABLE_MONTHLY_REVENUE = sql_table('monthly_revenue_simple')
TABLE_FUNDAMENTALS = sql_table('fundamentals_clean')


def _safe_pct_str(x):
    try:
        if pd.isna(x):
            return "NaN"
        return f"{float(x):.3f}"
    except Exception:
        return "NaN"


def get_vip_stock_pool(save_csv=True, return_details=False):
    print("==========================================================")
    print("🕵️‍♂️ [機構級海關] 啟動基本面雙引擎體檢程序 (整合強化版)...")
    print("==========================================================")

    vip_pool = []
    vip_details = []

    stats = {
        "1_資料缺失": 0,
        "2_EPS或本業虧損": 0,
        "3_營收重挫(<-10%)": 0,
        "4_非金融股負債過高": 0,
        "5_單季ROE未達標(<2.5%)": 0,
        "6_非金融股現金流負數": 0,
        "7_本業獲利比過低": 0,
        "8_缺乏爆發成長動能": 0,
        "🎉_成功入選": 0,
    }

    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = f"""
            WITH LatestRevenue AS (
                SELECT
                    [Ticker SYMBOL],
                    [單月營收年增率(%)],
                    [產業類別名稱],
                    [資料年月日],
                    ROW_NUMBER() OVER(
                        PARTITION BY [Ticker SYMBOL]
                        ORDER BY [資料年月日] DESC
                    ) AS rn
                FROM {TABLE_MONTHLY_REVENUE}
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
                FROM {TABLE_FUNDAMENTALS}
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
                    ) AS Prev_Margin,
                    ROW_NUMBER() OVER(
                        PARTITION BY [Ticker SYMBOL]
                        ORDER BY [資料年月日] DESC
                    ) AS rn
                FROM FinancialsBase
            )
            SELECT
                r.[Ticker SYMBOL],
                r.[單月營收年增率(%)] AS [單月營收年增率(%)],
                r.[產業類別名稱],
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
            FROM LatestRevenue r
            INNER JOIN FinancialsWithLag f
                ON r.[Ticker SYMBOL] = f.[Ticker SYMBOL]
            WHERE r.rn = 1
              AND f.rn = 1
            """
            df = pd.read_sql(query, conn)

        if df.empty:
            print("⚠️ 基本面庫尚未建立足夠歷史，自動退回使用原名單。")
            return WATCH_LIST if not return_details else (WATCH_LIST, pd.DataFrame())

        print(f"📦 成功提取並融合最新月營收 + 最新財報：共 {len(df)} 檔。")

        for _, row in df.iterrows():
            ticker = str(row["Ticker SYMBOL"]).strip()
            ind_name = str(row.get("產業類別名稱", "")).strip()

            eps = row.get("單季EPS")
            op_margin = row.get("營業利益率(%)")
            yoy = row.get("單月營收年增率(%)")
            margin = row.get("毛利率(%)")
            roe = row.get("ROE(%)")
            cash_flow = row.get("營業現金流")

            prev_eps = row.get("Prev_EPS")
            prev_year_eps = row.get("Prev_Year_EPS")
            prev_margin = row.get("Prev_Margin")

            debt_ratio = row.get("負債比率(%)")
            core_profit = row.get("本業獲利比(%)")

            debt_ratio = float(debt_ratio) if pd.notna(debt_ratio) else 50.0
            core_profit = float(core_profit) if pd.notna(core_profit) else 100.0

            # 1) 關鍵欄位缺值先排除
            critical_fields = [eps, op_margin, yoy, margin, roe]
            if any(pd.isna(x) for x in critical_fields):
                stats["1_資料缺失"] += 1
                continue

            # 2) 排雷裝甲
            if eps <= 0 or op_margin <= 0:
                stats["2_EPS或本業虧損"] += 1
                continue

            if yoy < -10.0:
                stats["3_營收重挫(<-10%)"] += 1
                continue

            is_finance = ind_name == "金融保險業"

            if (not is_finance) and debt_ratio > 65.0:
                stats["4_非金融股負債過高"] += 1
                continue

            if roe < 2.5:
                stats["5_單季ROE未達標(<2.5%)"] += 1
                continue

            if (not is_finance) and pd.notna(cash_flow) and float(cash_flow) < 0:
                stats["6_非金融股現金流負數"] += 1
                continue

            if core_profit < 50.0:
                stats["7_本業獲利比過低"] += 1
                continue

            # 3) 成長動能三擇一
            cond_rev_growth = yoy > 15.0
            cond_eps_high = pd.notna(prev_year_eps) and float(eps) > float(prev_year_eps)
            cond_margin_up = pd.notna(prev_margin) and float(margin) > float(prev_margin)

            if not (cond_rev_growth or cond_eps_high or cond_margin_up):
                stats["8_缺乏爆發成長動能"] += 1
                continue

            vip_pool.append(ticker)
            vip_details.append({
                "Ticker SYMBOL": ticker,
                "產業類別名稱": ind_name,
                "Revenue_Date": row.get("Revenue_Date"),
                "Financial_Date": row.get("Financial_Date"),
                "ROE(%)": roe,
                "單月營收年增率(%)": yoy,
                "毛利率(%)": margin,
                "營業利益率(%)": op_margin,
                "單季EPS": eps,
                "Prev_EPS": prev_eps,
                "Prev_Year_EPS": prev_year_eps,
                "Prev_Margin": prev_margin,
                "營業現金流": cash_flow,
                "負債比率(%)": debt_ratio,
                "本業獲利比(%)": core_profit,
                "is_finance": is_finance,
                "rev_growth_pass": cond_rev_growth,
                "eps_growth_pass": cond_eps_high,
                "margin_up_pass": cond_margin_up,
            })
            stats["🎉_成功入選"] += 1

        vip_pool = list(dict.fromkeys(vip_pool))

        print("\n📊 【海關淘汰漏斗分析報告】")
        for key, value in stats.items():
            print(f"   ➤ {key.ljust(22)}: {str(value).rjust(4)} 檔")

        print("\n==========================================================")
        print(f"🎉 體檢完成！本次共海選出 {len(vip_pool)} 檔「台股特種部隊」！")
        print("==========================================================")

        if len(vip_pool) == 0:
            print("⚠️ 今日無股票通過嚴格海關，啟動防禦機制，僅監控權值股。")
            result = DEFAULT_DEFENSIVE_LIST
            return result if not return_details else (result, pd.DataFrame(vip_details))

        df_out = pd.DataFrame(vip_details)

        if not df_out.empty:
            print("\n📋 獲選名單財務指標預覽:")
            preview = df_out[[
                "Ticker SYMBOL",
                "產業類別名稱",
                "ROE(%)",
                "單月營收年增率(%)",
                "毛利率(%)",
                "營業利益率(%)",
                "單季EPS",
            ]].copy()

            for col in ["ROE(%)", "單月營收年增率(%)", "毛利率(%)", "營業利益率(%)", "單季EPS"]:
                preview[col] = preview[col].apply(_safe_pct_str)

            print(preview.head(15).to_string(index=False))

        if save_csv:
            os.makedirs("data", exist_ok=True)
            pd.DataFrame({"Ticker SYMBOL": vip_pool}).to_csv(
                CSV_OUTPUT_PATH,
                index=False,
                encoding="utf-8-sig",
            )
            print(f"\n💾 名單已成功寫入 {CSV_OUTPUT_PATH}，中央樞紐隨時可提取！")

        return vip_pool if not return_details else (vip_pool, df_out)

    except Exception as e:
        print(f"❌ 基本面篩選失敗: {e}")
        result = WATCH_LIST
        return result if not return_details else (result, pd.DataFrame())


if __name__ == "__main__":
    vip_stocks = get_vip_stock_pool()
    print(f"\n🔥 今日 AI 最終核准之狙擊目標池：\n{vip_stocks}")
