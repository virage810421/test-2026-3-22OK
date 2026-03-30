import pyodbc
import pandas as pd

# ==========================================
# ⚙️ 資料庫連線設定 (請確認與您的設定一致)
# ==========================================
DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'  
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)

def generate_strategy_report(table_name="backtest_history"):
    """
    從資料庫讀取交易紀錄，並依照「進場陣型」與「市場狀態」進行績效歸因分析
    """
    print(f"\n🔍 正在從 [{table_name}] 提取機構級績效歸因數據...")
    
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            # 使用 SQL 讀取資料
            query = f"SELECT * FROM {table_name} WHERE [進場陣型] IS NOT NULL AND [進場陣型] != '無'"
            df = pd.read_sql(query, conn)
            
            if df.empty:
                print("⚠️ 目前資料庫中沒有足夠的交易紀錄可供分析。")
                return
            
            # 確保報酬率是數字
            df['報酬率(%)'] = pd.to_numeric(df['報酬率(%)'], errors='coerce')
            
            # 建立統計清單
            report_data = []
            
            # 依照「陣型」與「狀態」進行分組統計
            grouped = df.groupby(['進場陣型', '市場狀態'])
            
            for name, group in grouped:
                setup_tag = name[0]
                regime = name[1]
                total_trades = len(group)
                
                # 過濾掉極端少數的偶然交易 (例如少於 3 筆的無參考價值)
                if total_trades < 3:
                    continue
                
                wins = group[group['報酬率(%)'] > 0]
                losses = group[group['報酬率(%)'] <= 0]
                
                win_rate = len(wins) / total_trades
                avg_win = wins['報酬率(%)'].mean() if len(wins) > 0 else 0
                avg_loss = losses['報酬率(%)'].mean() if len(losses) > 0 else 0
                
                # 計算真實期望值 (EV)
                ev = (win_rate * avg_win) + ((1 - win_rate) * avg_loss)
                
                # 計算實現風報比 (RR)
                rr_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
                
                report_data.append({
                    "進場陣型": setup_tag,
                    "市場狀態": regime,
                    "總次數": total_trades,
                    "勝率(%)": round(win_rate * 100, 1),
                    "平均賺(%)": round(avg_win, 2),
                    "平均賠(%)": round(avg_loss, 2),
                    "實現風報比": round(rr_ratio, 2),
                    "真實期望值(EV)": round(ev, 3),
                    "累計總報酬(%)": round(group['報酬率(%)'].sum(), 2)
                })
            
            # 轉換成 DataFrame 並依期望值排序
            report_df = pd.DataFrame(report_data)
            if not report_df.empty:
                report_df = report_df.sort_values(by="真實期望值(EV)", ascending=False)
                
                print("\n" + "="*25 + " 🏆 策略陣型績效排行榜 " + "="*25)
                pd.set_option('display.unicode.east_asian_width', True) 
                print(report_df.to_string(index=False))
                print("="*75)
                
                # 給予人類可讀的建議
                best_setup = report_df.iloc[0]
                worst_setup = report_df.iloc[-1]
                print(f"\n💡 [系統建議] 目前最強護城河為在【{best_setup['市場狀態']}】發動【{best_setup['進場陣型']}】，每次進場預期獲利 {best_setup['真實期望值(EV)']}%。")
                
                if worst_setup['真實期望值(EV)'] < 0:
                    print(f"🚨 [毒藥警告] 請避免在【{worst_setup['市場狀態']}】執行【{worst_setup['進場陣型']}】，此舉長期期望值為負 ({worst_setup['真實期望值(EV)']}%)，會侵蝕本金！")
            else:
                print("⚠️ 符合統計門檻 (單一陣型 > 3 次交易) 的數據不足。")

    except Exception as e:
        print(f"❌ 產生報表時發生錯誤: {e}")

if __name__ == "__main__":
    # 直接執行這個檔案，就會印出大腦回測庫的績效
    generate_strategy_report("backtest_history")