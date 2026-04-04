import pyodbc
import pandas as pd
from config import WATCH_LIST

DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

def get_vip_stock_pool():
    print("========================================")
    print("🕵️‍♂️ [機構級海關] 啟動基本面雙引擎體檢程序...")
    print("========================================")
    
    vip_pool = []
    
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            # 🌟 SQL 擴充：把新欄位讀出來
            query = """
            WITH RankedData AS (
                SELECT 
                    [Ticker SYMBOL], [資料年月], [單月營收年增率(%)], [毛利率(%)], [營業利益率(%)],
                    [單季EPS], [ROE(%)], [稅後淨利率(%)], [營業現金流], [負債比率(%)], [本業獲利比(%)],
                    LAG([單季EPS], 1) OVER(PARTITION BY [Ticker SYMBOL] ORDER BY [資料年月]) as Prev_EPS,
                    LAG([單季EPS], 4) OVER(PARTITION BY [Ticker SYMBOL] ORDER BY [資料年月]) as Prev_Year_EPS,
                    LAG([毛利率(%)], 1) OVER(PARTITION BY [Ticker SYMBOL] ORDER BY [資料年月]) as Prev_Margin,
                    ROW_NUMBER() OVER(PARTITION BY [Ticker SYMBOL] ORDER BY [資料年月] DESC) as rn
                FROM fundamental_data
            )
            SELECT * FROM RankedData WHERE rn = 1
            """
            
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("⚠️ 基本面庫尚未建立足夠歷史，自動退回使用原名單。")
                return WATCH_LIST

            for _, row in df.iterrows():
                ticker = row['Ticker SYMBOL']
                eps = row.get('單季EPS', 0)
                op_margin = row.get('營業利益率(%)', 0)
                yoy = row.get('單月營收年增率(%)', 0)
                margin = row.get('毛利率(%)', 0)
                roe = row.get('ROE(%)', 0)
                cash_flow = row.get('營業現金流', 1)
                
                # 讀取新擴充的安全指標 (若空值則給予安全預設值，避免誤殺)
                debt_ratio = row.get('負債比率(%)', 50.0) if pd.notna(row.get('負債比率(%)')) else 50.0
                core_profit = row.get('本業獲利比(%)', 100.0) if pd.notna(row.get('本業獲利比(%)')) else 100.0
                
                if pd.isna(eps) or pd.isna(margin): continue
                
                # ==========================================
                # 🚨 第一道防線：地雷股絕對汰除 (新增負債與本業濾網)
                # ==========================================
                if eps < 0:
                    print(f"🗑️ 淘汰 {ticker}: 單季 EPS 為負 ({eps})，本業虧損。")
                    continue
                if op_margin < 0:
                    print(f"🗑️ 淘汰 {ticker}: 營業利益率為負 ({op_margin}%)，做白工。")
                    continue
                if yoy < -10.0:
                    print(f"🗑️ 淘汰 {ticker}: 營收嚴重衰退 ({yoy}%)。")
                    continue
                if debt_ratio > 65.0:
                    print(f"🗑️ 淘汰 {ticker}: 負債比過高 ({debt_ratio:.1f}%)，具備高槓桿風險。")
                    continue
                if core_profit < 50.0:
                    print(f"🗑️ 淘汰 {ticker}: 本業獲利比過低 ({core_profit:.1f}%)，靠業外收益撐場面。")
                    continue
                
                # ==========================================
                # 🏆 第二道防線：精銳部隊篩選 (長官的黃金條件)
                # ==========================================
                cond_rev_growth = yoy > 20.0
                cond_eps_high = (eps >= row.get('Prev_Year_EPS', 0)) and (eps >= row.get('Prev_EPS', 0))
                cond_roe = roe > 15.0
                cond_margin_up = margin >= row.get('Prev_Margin', 0)
                cond_cash_flow = cash_flow > 0
                
                # 綜合判定：通過死刑區後，具備高效率(ROE)+現金流安全，且帶有爆發力亮點
                if cond_roe and cond_cash_flow and (cond_rev_growth or cond_margin_up or cond_eps_high):
                    vip_pool.append(ticker)
                    
            print(f"\n✅ 體檢完畢！為您淘金出 {len(vip_pool)} 檔「體質強健、絕不虧損」的真飆股！")
            return vip_pool if len(vip_pool) > 0 else WATCH_LIST

    except Exception as e:
        print(f"❌ 基本面篩選失敗: {e}")
        return WATCH_LIST

if __name__ == "__main__":
    vip_stocks = get_vip_stock_pool()
    print(f"\n🔥 今日 AI 最終核准之狙擊目標池：\n{vip_stocks}")