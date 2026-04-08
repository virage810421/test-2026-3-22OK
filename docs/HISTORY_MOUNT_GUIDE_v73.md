# HISTORY / PRICE 掛載說明 v73

這版新增兩種掛載方式：

1. `FTS_SOURCE_MOUNT_DIRS`
- 給整包來源資料夾。
- 系統會同時把它當成價格掃描與歷史掃描的額外來源。

2. `FTS_PRICE_SCAN_DIRS`
- 只給價格/報價/快照 CSV。

3. `FTS_HISTORY_SCAN_DIRS`
- 只給 K 線 / OHLCV / history CSV。

## Windows PowerShell 範例

```powershell
$env:FTS_SOURCE_MOUNT_DIRS = 'C:\market_data;D:\backup_quotes'
$env:FTS_PRICE_SCAN_DIRS = 'C:\market_data\prices'
$env:FTS_HISTORY_SCAN_DIRS = 'C:\market_data\history'
python .\formal_trading_system_v73.py
```

## 手動價格覆寫

這版新增：
- `data/manual_price_snapshot_overrides.csv`

欄位：
- `Ticker`
- `Reference_Price`

用途：
- 當自動掃描不到價格時，你可以手動填在這份 override 檔。
- 系統會優先採用這份檔案，不會再被 template 覆蓋。
