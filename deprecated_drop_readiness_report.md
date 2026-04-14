# Deprecated Scan + Drop Readiness Report

生成時間：2026-04-14T05:49:23
狀態：`not_ready_core_findings`

## 摘要

- Python 檔案數：155
- Findings：106，severity={'medium': 60, 'high': 43, 'low': 3}
- Drop readiness：{'NOT_READY': 3, 'READY_CLEAN': 1, 'UNKNOWN_DB_NOT_CHECKED': 5, 'KEEP_COMPAT_DB_NOT_CHECKED': 2, 'NOT_READY_FOR_GLOBAL_FAIL_CLOSED': 1}
- Ticker SYMBOL refs：218
- ticker_symbol refs：165
- except Exception：468
- pass：51
- fallback：221
- DB 檢查：not_checked_static_only

## Drop / Retire Candidates

### file:system_guard.py
- current_status：`wrapper_only`
- drop_readiness：`NOT_READY`
- blockers：
  - 仍有其他檔案 import system_guard；刪檔前需改 import 到 fts_system_guard_service。
- required_steps_before_drop：
  - 改掉 import refs
  - 確認 fts_system_guard_service.py 已是唯一主線
  - 跑 healthcheck/bootstrap/daily

### duplicate_defs:fts_model_layer.py
- current_status：`duplicate_defs_found`
- drop_readiness：`NOT_READY`
- blockers：
  - fts_model_layer.py 仍有重複函式定義
- required_steps_before_drop：
  - 刪除被後段覆寫的舊函式
  - 重新跑 py_compile
  - 確認 exit runtime 欄位存在

### config:exit_model_hazard_fallback
- current_status：`fallback_disabled`
- drop_readiness：`READY_CLEAN`

### code:execution_Ticker_SYMBOL_references
- current_status：`58 refs, 22 unapproved`
- drop_readiness：`NOT_READY`
- blockers：
  - 核心/execution 檔案仍有未標示 alias/backfill/compat 的 Ticker SYMBOL 使用。
- required_steps_before_drop：
  - 跑 fts_db_migrations.py upgrade 回填 DB
  - 確認所有 execution runtime/output 都有 ticker_symbol
  - 觀察 3~5 輪 daily/paper 無舊欄位讀取告警

### db:execution_orders.[Ticker SYMBOL]
- current_status：`db_not_checked`
- drop_readiness：`UNKNOWN_DB_NOT_CHECKED`
- blockers：
  - 需要在本機 SQL Server 跑本工具才知道舊欄位是否存在。
- required_steps_before_drop：
  - python fts_db_migrations.py upgrade
  - python fts_admin_cli.py drop-readiness

### db:execution_fills.[Ticker SYMBOL]
- current_status：`db_not_checked`
- drop_readiness：`UNKNOWN_DB_NOT_CHECKED`
- blockers：
  - 需要在本機 SQL Server 跑本工具才知道舊欄位是否存在。
- required_steps_before_drop：
  - python fts_db_migrations.py upgrade
  - python fts_admin_cli.py drop-readiness

### db:execution_positions_snapshot.[Ticker SYMBOL]
- current_status：`db_not_checked`
- drop_readiness：`UNKNOWN_DB_NOT_CHECKED`
- blockers：
  - 需要在本機 SQL Server 跑本工具才知道舊欄位是否存在。
- required_steps_before_drop：
  - python fts_db_migrations.py upgrade
  - python fts_admin_cli.py drop-readiness

### db:execution_position_lots.[Ticker SYMBOL]
- current_status：`db_not_checked`
- drop_readiness：`UNKNOWN_DB_NOT_CHECKED`
- blockers：
  - 需要在本機 SQL Server 跑本工具才知道舊欄位是否存在。
- required_steps_before_drop：
  - python fts_db_migrations.py upgrade
  - python fts_admin_cli.py drop-readiness

### db:execution_broker_callbacks.[Ticker SYMBOL]
- current_status：`db_not_checked`
- drop_readiness：`UNKNOWN_DB_NOT_CHECKED`
- blockers：
  - 需要在本機 SQL Server 跑本工具才知道舊欄位是否存在。
- required_steps_before_drop：
  - python fts_db_migrations.py upgrade
  - python fts_admin_cli.py drop-readiness

### db:active_positions.[Ticker SYMBOL]
- current_status：`legacy_table_compat_layer`
- drop_readiness：`KEEP_COMPAT_DB_NOT_CHECKED`
- blockers：
  - active_positions/trade_history 是舊相容層；目前不建議破壞式 drop。
- required_steps_before_drop：
  - 若未來要 drop：先改所有報表/SQL/CSV/training 讀 ticker_symbol，再連跑多輪確認。

### db:trade_history.[Ticker SYMBOL]
- current_status：`legacy_table_compat_layer`
- drop_readiness：`KEEP_COMPAT_DB_NOT_CHECKED`
- blockers：
  - active_positions/trade_history 是舊相容層；目前不建議破壞式 drop。
- required_steps_before_drop：
  - 若未來要 drop：先改所有報表/SQL/CSV/training 讀 ticker_symbol，再連跑多輪確認。

### policy:global_exception_fail_closed
- current_status：`except_exception=468, pass=51, fallback=221, high_findings=43`
- drop_readiness：`NOT_READY_FOR_GLOBAL_FAIL_CLOSED`
- blockers：
  - ETL/research/legacy wrapper 不應全改 fail-closed；只需 diagnostics。
  - core_high_findings=43
- required_steps_before_drop：
  - 核心交易路徑 fail-closed
  - ETL/research fail-open + diagnostics
  - 不要全域硬刪 except Exception

## Top Findings

- `medium` `legacy_symbol_in_execution_context` db_logger.py:383 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `high` `core_except_exception` db_logger.py:9 - db_logger.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` db_logger.py:27 - db_logger.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` db_logger.py:401 - db_logger.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` db_logger.py:460 - db_logger.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` db_logger.py:463 - db_logger.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` db_logger.py:510 - db_logger.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` db_logger.py:525 - db_logger.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_pass` db_logger.py:526 - db_logger.py 核心路徑出現 \bpass\b 且附近未見 diagnostics/fail-closed。
- `medium` `legacy_symbol_in_execution_context` execution_engine.py:70 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` execution_engine.py:238 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` execution_engine.py:341 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` execution_engine.py:342 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` execution_engine.py:357 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `high` `core_except_exception` execution_engine.py:145 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:153 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:169 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:191 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:208 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:222 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:225 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:290 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:299 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:434 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:442 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:451 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:562 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:647 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:670 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:718 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:723 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:733 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` execution_engine.py:742 - execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_pass` execution_engine.py:815 - execution_engine.py 核心路徑出現 \bpass\b 且附近未見 diagnostics/fail-closed。
- `medium` `duplicate_function_definitions` fts_admin_suite.py:0 - 同一檔案存在重複函式定義：__init__x18, savex2, loadx3, buildx14, evaluatex2
- `medium` `duplicate_function_definitions` fts_broker_core.py:0 - 同一檔案存在重複函式定義：__init__x7, buildx6
- `medium` `duplicate_function_definitions` fts_chart_suite.py:0 - 同一檔案存在重複函式定義：__init__x7, buildx6
- `high` `core_except_exception` fts_control_tower.py:144 - fts_control_tower.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `medium` `legacy_symbol_in_execution_context` fts_db_migrations.py:136 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` fts_db_migrations.py:140 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
