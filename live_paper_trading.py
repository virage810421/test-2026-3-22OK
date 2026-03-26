import time
import pandas as pd
from datetime import datetime
import yfinance as yf
from advanced_chart import draw_chart
from screening import inspect_stock, add_chip_data
from config import PARAMS

# ==========================================
# 💼 虛擬帳戶與機台設定
# ==========================================
portfolio = {}       
trade_history = []   
SCAN_INTERVAL = 300  
FEE_SLIPPAGE = 0.0025 

watch_list = [
    # --- 權值大咖 (測試流動性與穩定趨勢) ---
    "2330.TW", "2454.TW", "2317.TW", "2303.TW", "2308.TW",
    
    # --- AI 與伺服器供應鏈 (測試高波動與背離) ---
    "2382.TW", "3231.TW", "6669.TW", "2357.TW", "3034.TW",
    
    # --- 航運三雄 (測試週期性強趨勢) ---
    "2603.TW", "2609.TW", "2615.TW",
    
    # --- 金融特攻隊 (測試低波動與基本面加持) ---
    "2881.TW", "2882.TW", "2891.TW",
    
    # --- 強勢重電與傳產 (測試突破與爆量) ---
    "1519.TW", "1513.TW", "2618.TW", "2002.TW"
]

def run_live_simulation():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🟢 啟動盤中實戰模擬引擎...")
    
    while True:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")

        print(f"\n[{current_time}] 📡 啟動定時海選雷達，掃描 {len(watch_list)} 檔標的...")
        
        try:
            # 🛡️ 拉長期間至 1y，確保 MA60 算得出來且有足夠資料畫圖
            batch_data = yf.download(watch_list, period="2y", progress=False) # 👈 改成 2 年
        except Exception as e:
            print(f"⚠️ 網路連線失敗: {e}"); time.sleep(60); continue

        for ticker in watch_list:
            time.sleep(1) # 👈 保護 API，每檔停 1 秒
            
            ticker_df = batch_data.xs(ticker, axis=1, level=1).copy() if isinstance(batch_data.columns, pd.MultiIndex) else batch_data.copy()
            ticker_df.dropna(how='all', inplace=True)
            if ticker_df.empty: continue
                
            ticker_df = add_chip_data(ticker_df, ticker)
            
            # 1. 呼叫大腦算分數
            result = inspect_stock(ticker, preloaded_df=ticker_df)
        
            # 🛡️ 第一層防護：確保大腦有回傳結果，且有帶「計算後資料」
            if result and "計算後資料" in result:
                computed_df = result['計算後資料']
            
            # 🛡️ 第二層防護：立刻檢查講義是不是空的 (避免 MA60 過濾掉所有資料)
            # 必須在執行任何動作前先 check，才不會引發 IndexError
            if computed_df.empty or len(computed_df) < PARAMS['MA_LONG']: 
                continue 
            
            # 提取回測數據與狀態
            status = result['今日系統燈號']
            current_price = result['最新收盤價']
            
            
            # 2. 執行模擬交易決策 (傳入大腦算好的 computed_df)
            # 💡 專業建議：把 draw_chart 放進 handle_paper_trade 函數裡面
            # 這樣只有在「真的有買賣訊號」時才會彈出圖表，不會每 5 分鐘彈出一次
            
            handle_paper_trade(ticker, current_price, status, computed_df, result)
        else:
            # 如果大腦沒給資料 (可能價格太低或量太小)，就換下一檔
            continue
print(f"[{datetime.now().strftime('%H:%M:%S')}] 掃描完成。進入冷卻等待 {SCAN_INTERVAL} 秒...")
time.sleep(SCAN_INTERVAL)

def handle_paper_trade(ticker, current_price, status, ticker_df, result_dict):
    """處理模擬買賣與同步大腦的動態停損/停利"""
    has_position = ticker in portfolio
    win_rate = result_dict["系統勝率(%)"]
    total_prof = result_dict["累計報酬率(%)"]
    
    # 這裡接收的 ticker_df 是大腦算好的 computed_df，擁有 Buy_Score 等指標
    latest_row = ticker_df.iloc[-1] 
    
    BASE_CAPITAL = 100000 
    
    # --- 狀況 A：進場觸發 (強/弱訊資金控管) ---
    if ("買訊" in status or "賣訊" in status) and not has_position:
        trade_dir = '做多(Long)' if "買" in status else '放空(Short)'
        invest_amount = BASE_CAPITAL * (1.0 if "強" in status else 0.5)
        
        # 紀錄趨勢狀態供未來停利用
        trend_is_bull = (latest_row['Close'] > latest_row['BBI']) and (latest_row['BBI'] > ticker_df.iloc[-2]['BBI'])
        
        portfolio[ticker] = {
            '進場價': current_price, '方向': trade_dir, '投入資金': invest_amount,
            '進場時間': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            '進場分數': int(latest_row['Buy_Score'] if "買" in status else latest_row['Sell_Score']),
            '進場趨勢多頭': trend_is_bull
        }
        print(f"⚡ [進場] {ticker} ({status}) | 佈局: ${invest_amount:,.0f} | 價格: {current_price}")
        
        # ✅ 成功進場後，呼叫儀表板畫圖！(傳入算好的 DataFrame)
        draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)

    # --- 狀況 B：部位控管 (停損/停利/反手) ---
    elif has_position:
        d = portfolio[ticker]
        is_long = d['方向'] == '做多(Long)'
        
        # 1. 損益計算 (包含摩擦成本)
        raw_p = (current_price - d['進場價']) / d['進場價'] if is_long else (d['進場價'] - current_price) / d['進場價']
        net_p = (raw_p * 100) - (FEE_SLIPPAGE * 100 * 2)
        
        # 2. 同步大腦的「動態防線計算」
        vol = (latest_row['BB_std'] * 1.5) / latest_row['Close']
        sl_line = max(0.030, min(vol, 0.100)) * 100
        tp_line = 25.0 if (d['進場趨勢多頭'] and latest_row['ADX14'] > 25) else 10.0
        if d['進場分數'] >= 8: tp_line = 999.0
            
        # 3. 判斷出場原因
        exit_msg = ""
        if net_p <= -sl_line: exit_msg = f"🛑 停損 (-{sl_line:.1f}%)"
        elif net_p >= tp_line: exit_msg = f"🎯 停利 (+{tp_line:.1f}%)"
        elif (is_long and "賣訊" in status) or (not is_long and "買訊" in status): exit_msg = f"🔄 反轉 ({status})"
            
        if exit_msg:
            d = portfolio.pop(ticker)
            net_amt = d['投入資金'] * (net_p / 100)
            trade_history.append({
                'Ticker SYMBOL': ticker, '方向': d['方向'], '淨損益': round(net_amt, 0), '報酬率(%)': f"{net_p:.3f}", '原因': exit_msg
            })
            print(f"💸 [出場] {ticker} {exit_msg} | 結算: ${net_amt:,.0f} ({net_p:.3f}%)")
            
            # ✅ 成功出場後，呼叫儀表板畫圖截圖！
            draw_chart(ticker, preloaded_df=ticker_df, win_rate=win_rate, total_profit=total_prof)

if __name__ == "__main__":
    try:
        run_live_simulation()
    except KeyboardInterrupt:
        print("\n🛑 結束引擎。")
        if trade_history:
            print("\n" + "="*20 + " 模擬交易明細表 " + "="*20)
            print(pd.DataFrame(trade_history).to_string(index=False))