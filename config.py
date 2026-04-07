PARAMS = {
    # --- 1. 核心技術指標天數 (決定指標的靈敏度) ---
    "RSI_PERIOD": 14,               # RSI 相對強弱指標的計算天數 (標準為 14 天)
    "MACD_FAST": 12,                # MACD 快線 (EMA) 的計算天數
    "MACD_SLOW": 26,                # MACD 慢線 (EMA) 的計算天數
    "MACD_SIGNAL": 9,               # MACD 訊號線 (DIF 的移動平均) 天數
    "BB_WINDOW": 20,                # 布林通道中軌 (MA) 的計算天數
    "BB_STD": 2.0,                  # 布林通道標準差倍數 (決定通道寬度)
    "VOL_WINDOW": 20,               # 成交量均量的計算天數
    "MA_LONG": 60,                  # 長期趨勢參考線 (通常指 60 日季線)
    "BBI_PERIODS": [3, 6, 12, 24],  # 多空指標 (BBI) 所融合的四組均線週期
    "DMI_PERIOD": 14,               # DMI 趨勢指標與 ATR 波動率的計算天數
    
    # --- 2. 策略濾網與觸發閥值 ---
    "TRIGGER_SCORE": 1,             # 訊號發動最低分數 (總分超過此值才顯示買/賣訊)-----------------------------------
    "ADX_TREND_THRESHOLD": 20,      # ADX 趨勢發動閥值 (高於此值代表趨勢成形)
    "MIN_PRICE": 10.0,              # 最低股價濾網 (過濾掉低價雞蛋股)
    "MIN_VOL_MA20": 1000000,        # 最低 20 日均量濾網 (確保流動性，單位：元)
    "VOL_BREAKOUT_MULTIPLIER": 1.1, # 爆量定義 (當日量大於均量的 1.1 倍)
    
    # # --- 3. 動態讓點與滑價 (回檔接/溢價賣) ---
    # "BUY_PULLBACK_RATE": 0.97,      # 買進讓點 (設 0.97 代表觸發後跌 3% 才成交)
    # "SELL_PREMIUM_RATE": 1.03,      # 賣出溢價 (設 1.03 代表觸發後漲 3% 才賣出)
    
    # --- 4. 摩擦成本 (台股預設) ---
    "FEE_RATE": 0.001425,           # 證券商公定手續費率 (0.1425%)
    "FEE_DISCOUNT": 0.6,            # 手續費折扣 (如 6 折則設 0.6)
    "TAX_RATE": 0.003,              # 證券交易稅率 (0.3%)
    
    # --- 5. 動態防線極限值 (依波動率縮放) ---
    "SL_MIN_PCT": 0.030,            # 最小停損百分比 (強制保命線)
    "SL_MAX_PCT": 0.080,            # 最大停損百分比 (波動劇烈時的極限)
    "TP_BASE_PCT": 0.10,            # 基礎停利百分比 (達到 10% 考慮出場)
    "TP_TREND_PCT": 0.250,          # 趨勢加成停利百分比 (強勢趨勢時放長線)
    
    # --- 6. 資金控管 ---
    "TOTAL_BUDGET": 10000000,       # 模擬帳戶總預算金額
    "MAX_POSITIONS": 20,            # 系統同時允許的最大持倉總檔數
    "MIN_RR_RATIO": 1.5,            # 最小風報比濾網 (潛在獲利必須是停損風險的 1.5 倍才進場)
    # ==========================================
    # 🌟 以下為本次升級新增的高階法人參數
    # ==========================================

    # --- 7. 分級風控 (MDD 熔斷防禦網) ---
    "MDD_LEVEL_1": 0.10,            # 一級防護觸發閥值 (回撤 10%)
    "MDD_MULTIPLIER_1": 0.5,        # 一級防護降載乘數 (資金砍半)
    "MDD_LEVEL_2": 0.15,            # 二級防護觸發閥值 (回撤 15%)
    "MDD_MULTIPLIER_2": 0.2,        # 二級防護降載乘數 (剩下 20%)
    "MDD_LIMIT": 0.20,              # 絕對熔斷極限 (回撤 20% 停止進場)

    # --- 8. 期望值 (EV) 資金配置權重 ---
    "EV_HIGH_THRESHOLD": 2.0,       # 極高勝算門檻 (EV >= 2.0%)
    "EV_HIGH_MULTIPLIER": 1.5,      # 極高勝算資金乘數 (動用 1.5 倍資金)
    "EV_BASE_THRESHOLD": 1.0,       # 標準勝算門檻 (EV >= 1.0%)
    "EV_BASE_MULTIPLIER": 1.0,      # 標準勝算資金乘數
    "EV_LOW_MULTIPLIER": 0.5,       # 邊緣勝算試單乘數 (低於標準時只用一半資金)

    # --- 9. 系統底層運作設定 ---
    "SCAN_INTERVAL": 300,           # 實戰機台掃描間隔 (秒)
    "MAX_BATCHES": 3,               # 單一股票最多允許的分批進場次數
    "MARKET_SLIPPAGE": 0.0015,       # 市價單預設滑價耗損 (0.15%)

    # ==========================================
    # 🌟 11. 策略因子開關 (Feature Toggles) 🌟      True 改成 False開關
    # ==========================================
    "USE_BBANDS": True,             # 啟用布林通道觸軌條件 (c1)
    "USE_RSI": True,                # 啟用 RSI 超買超賣條件 (c2)
    "USE_VOL_BREAKOUT": True,       # 啟用爆量條件 (c3)
    "USE_MACD": True,               # 啟用 MACD 轉強/轉弱條件 (c4)
    "USE_DIVERGENCE_RSI": True,     # 啟用 RSI 背離條件 (c5)
    "USE_BBI_BREAKOUT": True,       # 啟用突破 BBI 條件 (c6)
    "USE_CHIPS": True,              # 啟用三大昨日法人同買/同賣條件 (c7)
    "USE_DMI": True,                # 啟用 DMI 趨勢成型條件 (c8)
    "USE_DIVERGENCE_CHIPS": True,    # 啟用籌碼背離條件 (c9)
    
    # 🛰️ 戰略方向控制
    'ALLOW_LONG': True,   # 是否允許做多 (買入)
    'ALLOW_SHORT': False,  # 是否允許放空 (賣出)
    
    # ==========================================
    # 🌟 [新增] 測試模式控制
    # ==========================================
    # 是否忽略餘額限制 (True: 無限資金測試模式 / False: 嚴格依照帳戶餘額下單)
    'IGNORE_CASH_LIMIT': True,
    
    
    # 1. 單筆資金風險承受度 (原本寫死 0.01，現在放寬到 0.02 = 單筆承擔總資金 2% 風險)
    'BASE_RISK_PCT': 0.02,  
    # 2. 實戰心電圖淘汰底線 (原本寫死 0.35，現在降到 0.30 = 勝率跌破 30% 才拔插頭)
    'LIVE_MONITOR_WIN_RATE': 0.30,  
    # 3. AI 訓練專用：風報比(RR)探索宇宙 (加入 1.0 讓 AI 能挑選獲利空間較小的策略)
    'AI_RR_SEARCH_SPACE': [1.0, 1.2, 1.5, 2.0]
}

 # ==========================================
# 🌟 12. 多重產業專屬參數 (Sector-Specific Overrides)
# ==========================================
# 使用 .copy() 繼承原本的 PARAMS，然後只針對特定產業覆寫關鍵數值

# 💻 1. 科技半導體專用 (特徵：趨勢延續性強，允許較大的停利空間)
TECH_PARAMS = PARAMS.copy()
TECH_PARAMS.update({
    "RSI_PERIOD": 14,               # 維持標準
    "TP_TREND_PCT": 0.25,           # 科技股趨勢出來很驚人，放長線 (25%)
    "VOL_BREAKOUT_MULTIPLIER": 1.2  # 稍微爆量就可以視為表態
})

# 🚢 2. 航運週期股專用 (特徵：波動極大、暴漲暴跌，需要極端敏銳的指標)
SHIPPING_PARAMS = PARAMS.copy()
SHIPPING_PARAMS.update({
    "RSI_PERIOD": 10,               # 縮短天數，讓指標反應更快
    "SL_MAX_PCT": 0.10,             # 放寬停損極限 (航運洗盤很深)
    "BB_STD": 2.2,                  # 布林通道標準差調寬，避免假突破
    "TP_BASE_PCT": 0.15             # 基礎停利拉高，不吃魚頭魚尾只吃大魚身
})

# 🏦 3. 金融權值股專用 (特徵：牛皮、緩漲急跌，需要過濾雜訊)
FINANCE_PARAMS = PARAMS.copy()
FINANCE_PARAMS.update({
    "RSI_PERIOD": 20,               # 拉長天數，過濾日常雜訊
    "MIN_RR_RATIO": 1.2,            # 金融股肉不多，風報比要求可稍微降低
    "MDD_LIMIT": 0.10               # 金融股如果回撤 10% 通常代表大盤要崩了，提早熔斷
})
# ==========================================
# 🔐 系統核心金鑰區
# ==========================================
FINMIND_API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wNC0wNiAxNDo0OTozNyIsInVzZXJfaWQiOiJob25kYSIsImVtYWlsIjoiaG9uZGEyMTMxMTMwQGdtYWlsLmNvbSIsImlwIjoiMTEwLjI4LjUwLjE4NyJ9.Xf_ioecEyOt6LmILfaF7TVkCPDY8G72y2w9au6zBIAY"
# ==========================================
# 🎯 終極實戰與訓練觀察清單 (全局統一管理)
# ==========================================

import os
import pandas as pd

WATCH_LIST = [
    # 💻 TECH (科技)
    "2330.TW", "2317.TW", "2454.TW", "2382.TW", "2308.TW", "3231.TW",
    # 🏦 FINANCE (金融)
    "2881.TW", "2882.TW", "2891.TW", "2886.TW", "2884.TW", "2892.TW",
    # 🚢 SHIPPING (航運)
    "2603.TW", "2609.TW", "2615.TW", "2618.TW", "2610.TW", "2606.TW",
    # 🏭 OTHERS (傳產/重電)
    "1519.TW", "1513.TW", "1504.TW", "1101.TW", "2002.TW", "8996.TW",
    # 🧬 BIO (生技醫療 - 波動大)
    "6472.TW", "1795.TW", "6446.TW", "4162.TW", "4743.TW", "3176.TW",
    # 🔄 CYCLICAL (景氣循環 - 面板/記憶體)
    "2409.TW", "3481.TW", "6116.TW", "2344.TW", "2408.TW", "2337.TW"
]

# ==========================================
# 💀 破壞性測試對照組 (解決 AI 存活者偏差)
# ==========================================
# 故意放入過去幾年長年虧損、或是景氣大起大落的股票
# 讓 AI 學習「籌碼潰散、均線死亡交叉」的危險長相
LOSERS_LIST = [
    "2498.TW", # 宏達電 
    "3481.TW", # 群創 
    "2349.TW", # 錸德 
    "2888.TW"  # 新光金 
]

# ==========================================
# 🎯 智能名單樞紐 (提供給全產線呼叫)
# ==========================================
def get_dynamic_watch_list():
  
    # 這是您 fundamental_screener.py 產出的名單檔案
    target_csv = "stock_list_cache_listed.csv" 
    dynamic_list = []
    
    if os.path.exists(target_csv):
        try:
            df = pd.read_csv(target_csv)
            if 'Ticker SYMBOL' in df.columns:
                dynamic_list = df['Ticker SYMBOL'].dropna().unique().tolist()
                print(f"🎯 [名單樞紐] 成功匯入海選名單：共 {len(dynamic_list)} 檔")
        except Exception as e:
            print(f"⚠️ 讀取海選名單失敗: {e}")
            
    # 若無檔案或讀取失敗，啟動上方的靜態 WATCH_LIST 備援
    if not dynamic_list:
        print("⚠️ 啟動備用靜態名單 WATCH_LIST")
        dynamic_list = WATCH_LIST.copy()

    # 🌟 混合破壞性測試名單，建立最終作戰名單
    final_list = list(set(dynamic_list + LOSERS_LIST))
    return final_list