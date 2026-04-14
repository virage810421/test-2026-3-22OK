# Deprecated Scan + Drop Readiness Report

生成時間：2026-04-14T07:00:38
狀態：`ready_with_manual_db_review`

## 摘要

- Python 檔案數：155
- archived/reference .py skipped：0
- include_archived：False
- Findings：0，severity={}
- Drop readiness：{'READY_TO_RETIRE': 1, 'READY_CLEAN': 2, 'CONDITIONAL_READY': 1, 'UNKNOWN_DB_NOT_CHECKED': 5, 'KEEP_COMPAT_DB_NOT_CHECKED': 2, 'CORE_READY_KEEP_ETL_FAIL_OPEN': 1}
- Ticker SYMBOL refs：218
- ticker_symbol refs：170
- except Exception：471
- pass：49
- fallback：235
- DB 檢查：not_checked_static_only

## Drop / Retire Candidates

### file:system_guard.py
- current_status：`wrapper_only`
- drop_readiness：`READY_TO_RETIRE`

### duplicate_defs:fts_model_layer.py
- current_status：`clean`
- drop_readiness：`READY_CLEAN`

### config:exit_model_hazard_fallback
- current_status：`fallback_disabled`
- drop_readiness：`READY_CLEAN`

### code:execution_Ticker_SYMBOL_references
- current_status：`58 refs, 0 unapproved`
- drop_readiness：`CONDITIONAL_READY`
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
- current_status：`except_exception=471, pass=49, fallback=235, high_findings=0`
- drop_readiness：`CORE_READY_KEEP_ETL_FAIL_OPEN`
- blockers：
  - ETL/research/legacy wrapper 不應全改 fail-closed；只需 diagnostics。
- required_steps_before_drop：
  - 核心交易路徑 fail-closed
  - ETL/research fail-open + diagnostics
  - 不要全域硬刪 except Exception

## Top Findings

