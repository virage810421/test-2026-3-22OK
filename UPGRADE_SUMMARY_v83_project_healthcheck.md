
# v83 全專案健康檢查補丁

這包新增一套「所有 `.py` 有沒有都在工作」的健康檢查流程。

## 新增檔案
- `fts_project_healthcheck.py`
- `run_project_healthcheck.py`

## 能檢查什麼
1. **全部 Python 檔可編譯檢查**
   - 用 `py_compile` 掃描整個專案。
2. **核心模組 import 煙霧測試**
   - `formal_trading_system_v83_official_main`
   - `fts_fundamentals_etl_mainline`
   - `fts_training_governance_mainline`
   - `fts_feature_service`
   - `fts_screening_engine`
   - `fts_cross_sectional_percentile_service`
   - `fts_event_calendar_service`
   - `fts_decision_execution_bridge`
   - `fts_phase1_upgrade`
   - `fts_phase2_mock_broker_stage`
   - `fts_phase3_real_cutover_stage`
   - `model_governance`
   - `db_setup`
3. **本地 import 串聯檢查**
   - 用 AST 掃每支 `.py` 的本地模組引用。
4. **wrapper → service 串聯檢查**
   - `advanced_chart.py` → `fts_chart_service.py`
   - `daily_chip_etl.py` → `fts_etl_daily_chip_service.py`
   - `monthly_revenue_simple.py` → `fts_etl_monthly_revenue_service.py`
   - `ml_data_generator.py` → `fts_training_data_builder.py`
   - `ml_trainer.py` → `fts_trainer_backend.py`
   - `screening.py` → `fts_screening_engine.py`
5. **單一入口三模式檔案就緒檢查**
6. **db_setup 是否宣告核心表**
7. **runtime 核心 JSON 是否存在**

## 使用方式

### 快速健康檢查
```powershell
python run_project_healthcheck.py
```

### 深度健康檢查
```powershell
python run_project_healthcheck.py --deep
```

深度模式會額外嘗試：
- `python formal_trading_system_v83_official_main.py`
- `python formal_trading_system_v83_official_main.py --train`

## 報告輸出
- `runtime/project_healthcheck.json`

## 這次哪一段完整升級
- 全專案 `.py` 可編譯掃描：完整升級
- 核心模組 import smoke test：完整升級
- 本地模組串聯檢查：完整升級
- wrapper / service linkage 檢查：完整升級
- 單一入口三模式就緒檢查：完整升級
