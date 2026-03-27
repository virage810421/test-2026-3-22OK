PARAMS = {
    # --- 1. 核心技術指標天數 ---
    "RSI_PERIOD": 14, 
    "MACD_FAST": 12, 
    "MACD_SLOW": 26, 
    "MACD_SIGNAL": 9,
    "BB_WINDOW": 20, 
    "BB_STD": 2.0, 
    "VOL_WINDOW": 20, 
    "MA_LONG": 60,
    "BBI_PERIODS": [3, 6, 12, 24], 
    "DMI_PERIOD": 14,           # 新增：DMI 與 ATR 天數
    
    # --- 2. 策略濾網與觸發閥值 ---
    "TRIGGER_SCORE": 3,         # 訊號發動最低分數
    "ADX_TREND_THRESHOLD": 20,  # 新增：ADX 趨勢發動閥值
    "MIN_PRICE": 10.0,          # 新增：最低股價濾網
    "MIN_VOL_MA20": 1000000,    # 新增：最低 20 日均量濾網
    "VOL_BREAKOUT_MULTIPLIER": 1.1, # 新增：爆量定義 (均量的 1.1 倍)
    
    # --- 3. 動態讓點與滑價 (回檔接/溢價賣) ---
    "BUY_PULLBACK_RATE": 0.97,  # 新增：買進時的讓點 (0.97 = 跌 3% 才接)
    "SELL_PREMIUM_RATE": 1.03,  # 新增：賣出時的溢價 (1.03 = 漲 3% 才賣)
    
    # --- 4. 摩擦成本 (台股預設) ---
    "FEE_RATE": 0.001425,       # 新增：公定手續費
    "FEE_DISCOUNT": 0.6,        # 新增：手續費折讓
    "TAX_RATE": 0.003,          # 新增：證交稅
    
    # --- 5. 動態防線極限值 ---
    "SL_MIN_PCT": 0.030,        # 新增：最小停損 % 數
    "SL_MAX_PCT": 0.100,        # 新增：最大停損 % 數
    "TP_BASE_PCT": 0.100,       # 新增：基礎停利 % 數
    "TP_TREND_PCT": 0.250,      # 新增：趨勢加成停利 % 數
    
    # --- 6. 資金控管 ---
    "TOTAL_BUDGET": 10000000,    # 新增：總預算
    "MAX_POSITIONS": 20         # 新增：最大持倉檔數
}