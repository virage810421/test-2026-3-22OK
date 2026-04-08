# FTS 升級狀態報告 v65

## 這一輪實際補強
- 新增 `fts_training_orchestrator.py`
  - 把 AI 訓練從「知道有腳本」升成「知道資料、模型、是否可訓練、是否可推論」
  - 會檢查 `data/ml_training_data.csv`、`models/*.pkl`、label 分布、regime 分布、特徵數量
- 新增 `fts_decision_execution_bridge.py`
  - 把 normalized decision 往 executable payload 再推一步
  - 能合併價格快照、風險金額、停損欄位，計算 `Target_Qty`
  - 會用台股 lot/tick 規則檢查 payload
- 新增 `formal_trading_system_v65.py`
  - 在主控內納入 training orchestrator + execution bridge

## 目前真實狀態
- 架構成熟度：高
- 舊核心納管：高
- AI 訓練治理：中高（治理有了，資料與模型仍缺）
- 決策到執行橋接：中（橋已補，缺價格快照）
- Paper execution：仍未進到可送單
- Real API：仍是 stub / contract 階段

## 目前卡住的硬缺口
1. `data/ml_training_data.csv` 尚未生成
2. `models/selected_features.pkl` 與 regime models 尚未生成
3. `last_price_snapshot.csv` 尚未提供
4. `daily_decision_desk.csv` 目前 3 筆訊號都沒有價格

## 已生成的新輸出
- `runtime/training_orchestrator.json`
- `runtime/decision_execution_bridge.json`
- `data/executable_order_payloads.csv`
- `formal_trading_system_v65.py`

## 下一輪最值錢的升級
1. 讓 `ml_data_generator.py` 真的產出 `ml_training_data.csv`
2. 讓 `ml_trainer.py` 真的產出 models artifacts
3. 補 `last_price_snapshot.csv` 或在 decision stage 直接寫入 `Reference_Price`
4. 再往下接真券商 adapter
