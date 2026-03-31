import numpy as np
import pandas as pd
import pyodbc

# 🌟 統一資料庫連線字串
DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

# ==========================================
# 📊 實戰績效引擎 1：取得策略期望值 (EV)
# ==========================================
def get_strategy_ev(setup_tag, current_regime):
    """
    從 SQL 歷史交易總帳 (trade_history) 中，計算該陣型的真實平均期望值 (EV)
    """
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            # 撈取該陣型過去所有的報酬率來計算真實 EV
            query = '''
                SELECT [報酬率(%)] 
                FROM trade_history 
                WHERE [進場陣型] = ?
            '''
            df = pd.read_sql(query, conn, params=(setup_tag,))
            
            # 如果是全新陣型，還沒有交易紀錄，預設給予 0.5% 的基礎試單期望值
            if df.empty or len(df) == 0:
                return 0.5 
            
            # 期望值 (EV) = 歷史平均報酬率
            ev = df['報酬率(%)'].mean()
            return float(ev)
            
    except Exception as e:
        print(f"⚠️ 讀取策略期望值 (EV) 失敗: {e}")
        return 0.0

# ==========================================
# 🛑 實戰績效引擎 2：動態策略淘汰機制 (Live Monitor)
# ==========================================
def check_strategy_health(setup_tag, min_trades=10):
    """
    讀取 SQL 中該陣型最近 20 筆的真實交易紀錄。
    如果勝率低於 35% 或 夏普值 (Sharpe) 為負，立刻亮紅燈阻斷新資金投入。
    """
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            # 撈取該陣型最近 20 筆「已結案」的交易紀錄
            query = '''
                SELECT TOP 20 [報酬率(%)], [淨損益金額]
                FROM trade_history 
                WHERE [進場陣型] = ?
                ORDER BY [出場時間] DESC
            '''
            df = pd.read_sql(query, conn, params=(setup_tag,))
            
            # 🛡️ 防呆機制：如果交易次數還不夠，處於「熱身期」，預設放行
            if len(df) < min_trades:
                return "KEEP", f"樣本數不足 ({len(df)}/{min_trades})，熱身中"
            
            pnl_array = df['淨損益金額'].values
            win_rate = np.mean(pnl_array > 0)
            
            # 計算夏普值 (Sharpe Ratio = 平均報酬 / 標準差)
            std_pnl = np.std(pnl_array)
            sharpe = 0 if std_pnl == 0 else np.mean(pnl_array) / std_pnl
            
            # 🚨 淘汰判定：勝率跌破 35%，或近期根本在穩定賠錢 (Sharpe < 0)
            if win_rate < 0.350 or sharpe < 0:
                return "KILL", f"勝率崩潰 {win_rate*100:.3f}% | Sharpe {sharpe:.3f}"
                
            return "KEEP", f"健康良好 (勝率 {win_rate*100:.3f}% | Sharpe {sharpe:.3f})"
            
    except Exception as e:
        print(f"⚠️ 策略健康度檢查失敗 ({setup_tag}): {e}")
        # 若資料庫異常，為避免機台當機，預設先放行
        return "KEEP", "系統連線異常，預設放行"