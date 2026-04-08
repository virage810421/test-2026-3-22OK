# HISTORY MOUNT GUIDE v72

這版新增兩個可選環境變數：

- `FTS_HISTORY_SCAN_DIRS`：提供外部 K 線 / OHLCV 歷史資料夾
- `FTS_PRICE_SCAN_DIRS`：提供外部價格 / 快照資料夾

Windows 範例：

```bat
set FTS_BASE_DIR=C:	est	est-2026-3-22OK
set FTS_HISTORY_SCAN_DIRS=C:\market_cache;D:ackup\ohlcv
set FTS_PRICE_SCAN_DIRS=C:\market_cache\quotes;D:ackup\prices
python formal_trading_system_v72.py
```

建議價格 CSV 至少有兩欄：

- `Ticker`
- `Reference_Price` 或 `Close`

建議歷史 CSV 至少包含：

- `Ticker`
- `Date`
- `Open`
- `High`
- `Low`
- `Close`
- `Volume`
