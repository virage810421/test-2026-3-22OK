# Deprecated Scan + Drop Readiness Report

生成時間：2026-04-15T09:38:01
狀態：`not_ready_core_findings`

## 摘要

- Python 檔案數：205
- archived/reference .py skipped：34
- include_archived：False
- Findings：35，severity={'medium': 16, 'high': 17, 'low': 2}
- Drop readiness：{'READY_TO_RETIRE': 1, 'READY_CLEAN': 2, 'NOT_READY': 1, 'NO_OLD_COLUMN': 5, 'KEEP_COMPAT_NOT_DROP': 2, 'NOT_READY_FOR_GLOBAL_FAIL_CLOSED': 1}
- Ticker SYMBOL refs：315
- ticker_symbol refs：228
- except Exception：637
- pass：68
- fallback：243
- DB 檢查：checked

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
- current_status：`79 refs, 18 unapproved`
- drop_readiness：`NOT_READY`
- blockers：
  - 核心/execution 檔案仍有未標示 alias/backfill/compat 的 Ticker SYMBOL 使用。
- required_steps_before_drop：
  - 跑 fts_db_migrations.py upgrade 回填 DB
  - 確認所有 execution runtime/output 都有 ticker_symbol
  - 觀察 3~5 輪 daily/paper 無舊欄位讀取告警

### db:execution_orders.[Ticker SYMBOL]
- current_status：`exists=True old=False new=True null_new=None`
- drop_readiness：`NO_OLD_COLUMN`

### db:execution_fills.[Ticker SYMBOL]
- current_status：`exists=True old=False new=True null_new=None`
- drop_readiness：`NO_OLD_COLUMN`

### db:execution_positions_snapshot.[Ticker SYMBOL]
- current_status：`exists=True old=False new=True null_new=None`
- drop_readiness：`NO_OLD_COLUMN`

### db:execution_position_lots.[Ticker SYMBOL]
- current_status：`exists=True old=False new=True null_new=None`
- drop_readiness：`NO_OLD_COLUMN`

### db:execution_broker_callbacks.[Ticker SYMBOL]
- current_status：`exists=True old=False new=True null_new=None`
- drop_readiness：`NO_OLD_COLUMN`

### db:active_positions.[Ticker SYMBOL]
- current_status：`legacy_table_compat_layer`
- drop_readiness：`KEEP_COMPAT_NOT_DROP`
- blockers：
  - active_positions/trade_history 是舊相容層；目前不建議破壞式 drop。
- required_steps_before_drop：
  - 若未來要 drop：先改所有報表/SQL/CSV/training 讀 ticker_symbol，再連跑多輪確認。

### db:trade_history.[Ticker SYMBOL]
- current_status：`legacy_table_compat_layer`
- drop_readiness：`KEEP_COMPAT_NOT_DROP`
- blockers：
  - active_positions/trade_history 是舊相容層；目前不建議破壞式 drop。
- required_steps_before_drop：
  - 若未來要 drop：先改所有報表/SQL/CSV/training 讀 ticker_symbol，再連跑多輪確認。

### policy:global_exception_fail_closed
- current_status：`except_exception=637, pass=68, fallback=243, high_findings=17`
- drop_readiness：`NOT_READY_FOR_GLOBAL_FAIL_CLOSED`
- blockers：
  - ETL/research/legacy wrapper 不應全改 fail-closed；只需 diagnostics。
  - core_high_findings=17
- required_steps_before_drop：
  - 核心交易路徑 fail-closed
  - ETL/research fail-open + diagnostics
  - 不要全域硬刪 except Exception

## Top Findings

- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\execution_engine.py:52 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\execution_engine.py:115 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\execution_engine.py:122 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\execution_engine.py:260 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\execution_engine.py:270 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\execution_engine.py:280 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\execution_engine.py:290 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\execution_engine.py:300 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `high` `core_except_exception` advanced_chart1_runtime_variants\execution_engine.py:193 - advanced_chart1_runtime_variants\execution_engine.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\execution_engine.py:253 - advanced_chart1_runtime_variants\execution_engine.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\live_paper_trading.py:134 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\live_paper_trading.py:294 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\live_paper_trading.py:363 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\live_paper_trading.py:389 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\live_paper_trading.py:398 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\live_paper_trading.py:408 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\live_paper_trading.py:466 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` advanced_chart1_runtime_variants\live_paper_trading.py:507 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:61 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:71 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:83 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:94 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:103 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:115 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:169 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:279 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:453 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_pass` advanced_chart1_runtime_variants\live_paper_trading.py:72 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 pass 且附近未見 diagnostics/intentional no-op。
- `high` `core_pass` advanced_chart1_runtime_variants\live_paper_trading.py:84 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 pass 且附近未見 diagnostics/intentional no-op。
- `high` `core_except_exception` advanced_chart1_runtime_variants\live_paper_trading.py:435 - advanced_chart1_runtime_variants\live_paper_trading.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\system_guard.py:25 - advanced_chart1_runtime_variants\system_guard.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\system_guard.py:34 - advanced_chart1_runtime_variants\system_guard.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `high` `core_except_exception` advanced_chart1_runtime_variants\system_guard.py:42 - advanced_chart1_runtime_variants\system_guard.py 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。
- `low` `legacy_symbol_in_execution_context` fts_reconciliation_runtime.py:26 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` fts_sql_chinese_column_views.py:101 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
