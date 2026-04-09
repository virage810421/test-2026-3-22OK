# v83 de-screening 更新摘要

## 本次目標
讓 A 幾乎不再直接依賴舊 `screening.py`。

## 這次完成的事
1. `fts_market_data_service.py` 不再反向 import 舊 `screening.py`
2. `fts_feature_service.py` 不再反向 import 舊 `screening.py`
3. `fts_chip_enrichment_service.py` 不再反向 import 舊 `screening.py`
4. `fts_screening_engine.py` 不再反向 import 舊 `screening.py`
5. 新增 `screening.py` wrapper：舊入口保留，但內部改走新 service
6. 新增 `fts_screening_detachment_audit.py`：檢查目前是否還有新 service 反向依賴舊 `screening.py`
7. 更新 `formal_trading_system_v83_official_main.py` 與 `fts_tests.py`

## 完整升級判定
- `screening` service 去 legacy 依賴：**完整升級**
- 舊 `screening.py` 相容入口：**完整升級**
- 真券商實盤：**不在本輪範圍內**
