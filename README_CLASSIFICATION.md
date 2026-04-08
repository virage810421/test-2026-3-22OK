# Clean Trading Code Package

這份壓縮包是依照你上傳的多個 zip 重新整理後產出的「乾淨版專案」。

## 目錄
- `ACTIVE_CODE/`：目前保留的主幹程式碼
- `old/`：移出的舊版 / 重複版 / legacy inventory 類模組
- `docs/`：說明文件、README、HTML 架構圖
- `supporting_assets/`：範例 CSV / JSON / 模板 / 參考輸出
- `CODE_CLASSIFICATION.csv`：完整檔案分類表

## 我採用的分類原則
1. **保留 active**：主入口、ETL、訓練、paper broker、核心支援模組、v79 主控相關模組。
2. **移到 old**：`formal_trading_system_v75~v78`、明顯標示為 legacy inventory 的盤點/過渡模組、測試檔。
3. **注意**：這些被移到 `old/` 的檔案不代表語法壞掉；它們大多是 **歷史版本、重複階段產物、或不再建議當主幹入口**。

## 建議優先入口
- `formal_trading_system_v79.py`：FTS 主控版 v79 入口
- `launcher.py`：日常排程/批次啟動器
- `master_pipeline.py`：主研究/海選/訓練流水線
- `daily_chip_etl.py`：法人籌碼 + 月營收 + 季財報 ETL 協調
- `monthly_revenue_simple.py`：月營收 ETL
- `yahoo_csv_to_sql.py`：Yahoo 財報 CSV/SQL 匯入
- `live_paper_trading.py`：Paper Trading 執行層
- `ml_data_generator.py`：ML 訓練資料生成
- `ml_trainer.py`：ML 模型訓練
- `db_setup.py`：SQL 資料表建立/修補

## 建議你接下來怎麼用
1. 先以 `ACTIVE_CODE/` 當新專案根目錄。
2. 先跑 `db_setup.py` 檢查資料表。
3. 再視需求啟動：
   - ETL：`daily_chip_etl.py`
   - 研究/訓練：`master_pipeline.py`、`ml_data_generator.py`、`ml_trainer.py`
   - 主控：`formal_trading_system_v79.py`
   - 模擬交易：`live_paper_trading.py`
4. 若有某支舊檔你之後還想救回來，可以從 `old/` 再拿回來比對。

## 這次整理結果
- Active Python 檔：127
- Old Python 檔：24
- Docs：18
- Supporting assets：24
