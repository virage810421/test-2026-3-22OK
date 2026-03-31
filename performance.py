import pyodbc
import pandas as pd
import numpy as np
import warnings
import time
warnings.filterwarnings('ignore', category=UserWarning)


# ⚙️ 資料庫連線設定
# ==========================================
DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

def analyze_performance(table_name="backtest_history"):
    """
    從 SQL 提取交易紀錄，並針對「進場陣型(Strategy)」進行機構級的績效運算
    """
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            # 讀取非空的陣型紀錄
            query = f"SELECT * FROM {table_name} WHERE [進場陣型] IS NOT NULL AND [進場陣型] != '無' AND [進場陣型] != '傳統訊號'"
            df = pd.read_sql(query, conn)
            
            if df.empty:
                return None

            df['報酬率(%)'] = pd.to_numeric(df['報酬率(%)'], errors='coerce')
            
            results = {}
            # 依照「進場陣型」與「市場狀態」進行分組
            grouped = df.groupby(['進場陣型', '市場狀態'])
            
            for name, group in grouped:
                strategy_name = f"{name[0]} ({name[1]})"
                total_trades = len(group)
                
                # 過濾掉極端少數的偶然交易 (低於 3 筆不具統計意義)
                if total_trades < 3:
                    continue
                
                wins = group[group['報酬率(%)'] > 0]
                losses = group[group['報酬率(%)'] <= 0]
                
                win_rate = len(wins) / total_trades
                avg_win = wins['報酬率(%)'].mean() if len(wins) > 0 else 0
                avg_loss = losses['報酬率(%)'].mean() if len(losses) > 0 else 0
                
                # ✨ 核心計算：真實期望值 (EV) 與 風報比 (RR)
                ev = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)
                rr_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
                
                results[strategy_name] = {
                    "市場狀態": name[1],
                    "陣型": name[0],
                    "交易次數": total_trades,
                    "勝率(%)": round(win_rate * 100, 1),
                    "平均獲利(%)": round(avg_win, 2),
                    "平均虧損(%)": round(avg_loss, 2),
                    "實現風報比": round(rr_ratio, 2),
                    "真實期望值(EV%)": round(ev, 3),
                    "累計總報酬(%)": round(group['報酬率(%)'].sum(), 2)
                }
            return results
    except Exception as e:
        print(f"❌ 讀取資料庫失敗: {e}")
        return None

def print_report(results):
    """
    將分析結果輸出成漂亮的排行榜
    """
    if not results:
        print("⚠️ 目前資料庫中沒有足夠的有效交易紀錄可供分析。(請先執行 screening.py 跑一次回測)")
        return

    # 將字典轉為 DataFrame 方便排序
    df = pd.DataFrame.from_dict(results, orient='index')
    # 依照期望值 (EV) 降序排列
    df = df.sort_values(by="真實期望值(EV%)", ascending=False)
    
    print("\n" + "═"*30 + " 🏆 策略模組績效排行榜 " + "═"*30)
    pd.set_option('display.unicode.east_asian_width', True) 
    
    # 調整顯示欄位順序
    display_cols = ["交易次數", "勝率(%)", "平均獲利(%)", "平均虧損(%)", "實現風報比", "真實期望值(EV%)", "累計總報酬(%)"]
    print(df[display_cols].to_string())
    print("═"*85)
    
    # 給予交易員 AI 診斷建議
    best_strat = df.iloc[0]
    worst_strat = df.iloc[-1]
    
    print(f"\n💡 [AI 診斷結論]")
    print(f"🥇 最強護城河：【{best_strat.name}】")
    print(f"   👉 每次發動預期可獲利 {best_strat['真實期望值(EV%)']}%，請在實戰中對此訊號給予最高信任度。")
    
    if worst_strat['真實期望值(EV%)'] < 0:
        print(f"🚨 毒藥策略警告：【{worst_strat.name}】")
        print(f"   👉 長期期望值為負 ({worst_strat['真實期望值(EV%)']}%)！這代表越做越賠，強烈建議在 config 中禁用此環境下的該陣型！")



# 🌟 全域快取記憶體，防止對 SQL 進行 DDoS 攻擊
_EV_CACHE = {}
_LAST_UPDATE = 0

def get_strategy_ev(setup_tag, regime):
    """
    提供給實戰機台呼叫的 API。
    加入 Cache 機制：每 1 小時才重新查一次資料庫，其他時間直接秒回快取數據！
    """
    global _EV_CACHE, _LAST_UPDATE
    
    current_time = time.time()
    # 如果距離上次更新超過 3600 秒 (1小時)，才重新查詢 SQL
    if current_time - _LAST_UPDATE > 3600:
        results = analyze_performance("backtest_history")
        if results:
            _EV_CACHE = {k: v["真實期望值(EV%)"] for k, v in results.items()}
        _LAST_UPDATE = current_time
        
    strat_key = f"{setup_tag} ({regime})"
    return _EV_CACHE.get(strat_key, 0.0)

if __name__ == "__main__":
    print("啟動 Layer 2 績效透視引擎...")
    # 預設分析大腦的回測歷史庫
    stats = analyze_performance("backtest_history")
    print_report(stats)