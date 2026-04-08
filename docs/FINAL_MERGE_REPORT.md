# Final merged trading code package

This package is the fully merged project built from the previously organized full package,
with later bridge fixes preserved and the updated `sector_classifier.py` overlaid.

## Counts
- Active Python files: 127
- OLD Python files: 24
- Total Python files: 151

## What was kept in ACTIVE_CODE
- Current main control and pipeline files
- Current ETL, bridge, screening, training, broker, guard, and runtime modules
- Files used by the recent bridged package as the main path

## What was moved to OLD
These are legacy, superseded, or less preferred single-file cores and prior versions:
- formal_trading_system_v75.py
- formal_trading_system_v76.py
- formal_trading_system_v77.py
- formal_trading_system_v78.py
- fts_decision_price_bridge_plus.py
- fts_deep_risk_checks.py
- fts_etl_ai_visibility.py
- fts_etl_batch_stats.py
- fts_etl_data_quality_plus.py
- fts_etl_field_completeness.py
- fts_etl_quality.py
- fts_execution.py
- fts_execution_callback_flow.py
- fts_gate_summary.py
- fts_gatekeeper.py
- fts_interface_alignment_plus.py
- fts_interface_audit.py
- fts_legacy_bridge_map.py
- fts_legacy_core_metrics.py
- fts_legacy_core_readiness_board.py
- fts_legacy_core_upgrade_plan.py
- fts_legacy_core_upgrade_wave.py
- fts_legacy_inventory.py
- test_line.py

## Notes
- `sector_classifier.py` has been updated to use fallback lookup tables instead of hard-failing on a missing industry table.
- This is a full package, not an update-only package.
- Use `master_pipeline.py` as the primary operational entry point for data and bridge flow.
