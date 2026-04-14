# Deprecated Scan + Drop Readiness Report

生成時間：2026-04-14T14:08:55
狀態：`not_ready_core_findings`

## 摘要

- Python 檔案數：222
- Findings：193，severity={'low': 22, 'medium': 94, 'high': 77}
- Drop readiness：{'NOT_READY': 3, 'READY_CLEAN': 1, 'NO_OLD_COLUMN': 5, 'KEEP_COMPAT_NOT_DROP': 2, 'NOT_READY_FOR_GLOBAL_FAIL_CLOSED': 1}
- Ticker SYMBOL refs：428
- ticker_symbol refs：253
- except Exception：678
- pass：71
- fallback：270
- DB 檢查：checked

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
- current_status：`118 refs, 73 unapproved`
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
- current_status：`exists=False old=False new=False null_new=None`
- drop_readiness：`NO_OLD_COLUMN`

### db:execution_broker_callbacks.[Ticker SYMBOL]
- current_status：`exists=False old=False new=False null_new=None`
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
- current_status：`except_exception=678, pass=71, fallback=270, high_findings=77`
- drop_readiness：`NOT_READY_FOR_GLOBAL_FAIL_CLOSED`
- blockers：
  - ETL/research/legacy wrapper 不應全改 fail-closed；只需 diagnostics。
  - core_high_findings=77
- required_steps_before_drop：
  - 核心交易路徑 fail-closed
  - ETL/research fail-open + diagnostics
  - 不要全域硬刪 except Exception

## Top Findings

- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:90 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:111 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:136 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:154 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:178 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:185 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:191 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:211 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:226 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:246 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:250 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:258 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:274 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:295 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:306 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `low` `legacy_symbol_in_execution_context` _backup_monthly_fix\db_setup.py:322 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\execution_engine.py:52 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\execution_engine.py:115 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\execution_engine.py:122 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\execution_engine.py:260 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\execution_engine.py:270 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\execution_engine.py:280 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\execution_engine.py:290 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\execution_engine.py:300 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `high` `core_except_exception` absorbed_references\advanced_chart1_original\execution_engine.py:193 - absorbed_references\advanced_chart1_original\execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` absorbed_references\advanced_chart1_original\execution_engine.py:253 - absorbed_references\advanced_chart1_original\execution_engine.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\live_paper_trading.py:134 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\live_paper_trading.py:294 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\live_paper_trading.py:363 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\live_paper_trading.py:389 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\live_paper_trading.py:398 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\live_paper_trading.py:408 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\live_paper_trading.py:466 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `medium` `legacy_symbol_in_execution_context` absorbed_references\advanced_chart1_original\live_paper_trading.py:507 - core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。
- `high` `core_except_exception` absorbed_references\advanced_chart1_original\live_paper_trading.py:61 - absorbed_references\advanced_chart1_original\live_paper_trading.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` absorbed_references\advanced_chart1_original\live_paper_trading.py:71 - absorbed_references\advanced_chart1_original\live_paper_trading.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` absorbed_references\advanced_chart1_original\live_paper_trading.py:83 - absorbed_references\advanced_chart1_original\live_paper_trading.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` absorbed_references\advanced_chart1_original\live_paper_trading.py:94 - absorbed_references\advanced_chart1_original\live_paper_trading.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` absorbed_references\advanced_chart1_original\live_paper_trading.py:103 - absorbed_references\advanced_chart1_original\live_paper_trading.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
- `high` `core_except_exception` absorbed_references\advanced_chart1_original\live_paper_trading.py:115 - absorbed_references\advanced_chart1_original\live_paper_trading.py 核心路徑出現 except\s+Exception\b 且附近未見 diagnostics/fail-closed。
