# v83 percentile + feature mounting 升級摘要

## 本輪目標
1. 將既有近似 rank 升級為 **全市場 percentile 正式版**。
2. 讓 `selected_features.pkl` **真正掛載到 live pipeline**，不再只是模型旁邊的一份檔案。
3. 將橫截面 percentile 與 mounted features 落到 runtime / data 可追蹤輸出。

## 完整升級的段落
- 全市場 percentile 正式版：完整升級
- selected_features 掛載 live pipeline：完整升級
- cross-sectional snapshot 持久化：完整升級
- feature stack audit：完整升級
- v83 主控接入 percentile/mount 狀態：完整升級

## 新增/更新檔案
- `fts_feature_catalog.py`
- `fts_cross_sectional_percentile_service.py`
- `fts_feature_service.py`
- `fts_screening_engine.py`
- `fts_feature_stack_audit.py`
- `formal_trading_system_v83_official_main.py`
- `run_full_market_percentile_snapshot.py`

## 使用方式
### 1. 先建立全市場 percentile snapshot
```powershell
python run_full_market_percentile_snapshot.py
```

### 2. 再跑主控
```powershell
python formal_trading_system_v83_official_main.py
```

## 主要輸出
- `data/feature_cross_section_snapshot.csv`
- `data/selected_live_feature_mounts.csv`
- `runtime/cross_sectional_percentile_service.json`
- `runtime/live_feature_mount.json`
- `runtime/feature_stack_audit.json`

## 範圍說明
本輪「升級成 100%」是指 **feature stack / percentile / mounting 範圍**。
不代表真券商、全市場即時資料、所有事件資料源都已 100%。
