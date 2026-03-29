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
    "TRIGGER_SCORE": 3,             # 訊號發動最低分數 (總分超過此值才顯示買/賣訊)
    "ADX_TREND_THRESHOLD": 20,      # ADX 趨勢發動閥值 (高於此值代表趨勢成形)
    "MIN_PRICE": 10.0,              # 最低股價濾網 (過濾掉低價雞蛋股)
    "MIN_VOL_MA20": 1000000,        # 最低 20 日均量濾網 (確保流動性，單位：元)
    "VOL_BREAKOUT_MULTIPLIER": 1.1, # 爆量定義 (當日量大於均量的 1.1 倍)
    
    # --- 3. 動態讓點與滑價 (回檔接/溢價賣) ---
    "BUY_PULLBACK_RATE": 0.97,      # 買進讓點 (設 0.97 代表觸發後跌 3% 才成交)
    "SELL_PREMIUM_RATE": 1.03,      # 賣出溢價 (設 1.03 代表觸發後漲 3% 才賣出)
    
    # --- 4. 摩擦成本 (台股預設) ---
    "FEE_RATE": 0.001425,           # 證券商公定手續費率 (0.1425%)
    "FEE_DISCOUNT": 0.6,            # 手續費折扣 (如 6 折則設 0.6)
    "TAX_RATE": 0.003,              # 證券交易稅率 (0.3%)
    
    # --- 5. 動態防線極限值 (依波動率縮放) ---
    "SL_MIN_PCT": 0.030,            # 最小停損百分比 (強制保命線)
    "SL_MAX_PCT": 0.100,            # 最大停損百分比 (波動劇烈時的極限)
    "TP_BASE_PCT": 0.20,            # 基礎停利百分比 (達到 20% 考慮出場)
    "TP_TREND_PCT": 0.250,          # 趨勢加成停利百分比 (強勢趨勢時放長線)
    
    # --- 6. 資金控管 ---
    "TOTAL_BUDGET": 10000000,       # 模擬帳戶總預算金額
    "MAX_POSITIONS": 20,            # 系統同時允許的最大持倉總檔數

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
    # 🌟 策略切換開關
    'USE_SNIPER_MODE': True,  # True = 啟動黃金陣型狙擊 (嚴格) | False = 退回傳統 3 分制 (寬鬆)
    # 🛰️ 戰略方向控制
    'ALLOW_LONG': True,   # 是否允許做多 (買入)
    'ALLOW_SHORT': True,  # 是否允許放空 (賣出)

}

