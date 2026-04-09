# v83 單一入口三模式版

## 這次做了什麼

把 `formal_trading_system_v83_official_main.py` 收成單一入口三模式：

- `python formal_trading_system_v83_official_main.py --bootstrap`
  - 第一次建置 / 新電腦初始化
  - 會依序嘗試：
    - `db_setup_research_plus.py`
    - `run_full_market_percentile_snapshot.py`
    - `run_precise_event_calendar_build.py`
    - `run_sync_feature_snapshots_to_sql.py`
  - 然後再跑 daily 主控與 completion audit

- `python formal_trading_system_v83_official_main.py`
  - 預設 daily 模式
  - 跑 fundamentals ETL local sync、training governance、feature stack audit、cross percentile、event calendar、project completion audit、三階段主控

- `python formal_trading_system_v83_official_main.py --train`
  - 先跑 `ml_data_generator.py`
  - 再跑 `ml_trainer.py`
  - 最後再做 training governance summary

## 哪一段完整升級

- 單一入口三模式：完整升級
- bootstrap / daily / train 三模式主控收口：完整升級
- 手動 8 指令縮成 1 入口：完整升級

## 建議使用方式

### 第一次安裝 / 新電腦
```powershell
python formal_trading_system_v83_official_main.py --bootstrap
```

### 平常日常執行
```powershell
python formal_trading_system_v83_official_main.py
```

### 重訓
```powershell
python formal_trading_system_v83_official_main.py --train
```
