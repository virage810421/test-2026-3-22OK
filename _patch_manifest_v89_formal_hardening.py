# -*- coding: utf-8 -*-
"""
_patch_manifest_v89_formal_hardening.py

更新檔版本：v89 formal hardening
目的：修正 Target_Return 單位、禁止 lane shared fallback、改用 position-day 出場樣本、
      維持真券商 readiness 紅燈、signal path fail-closed、補強舊門牌清理清單。
"""

UPDATED_FILES = [
    'config.py',
    'fts_config.py',
    'fts_training_data_builder.py',
    'fts_data_quality_guard.py',
    'fts_trainer_backend.py',
    'fts_training_orchestrator.py',
    'fts_training_ticker_scoreboard.py',
    'fts_model_layer.py',
    'fts_directional_artifact_bootstrap.py',
    'fts_signal_gate.py',
    'fts_compat.py',
    'fts_decision_execution_bridge.py',
    'fts_live_readiness_gate.py',
    'cleanup_second_merge_retired_py_files.py',
]

FULLY_UPGRADED = [
    'Target_Return canonical unit is decimal return; legacy percent data is auto-normalized and reported.',
    'Independent lane model requirement is enforced; shared-copy lane artifacts are blocked.',
    'Exit AI training source changed to POSITION_DAY samples generated from simulated holding days.',
    'True broker LIVE readiness requires API/callback/ledger/reconcile/kill-switch green lights.',
    'Signal path defaults now fail-closed when DeskUsable/ExecutionEligible/model artifacts are missing.',
]

PARTIALLY_UPGRADED = [
    'Physical removal of duplicate/old-door files remains dependency-aware and must be executed with cleanup_second_merge_retired_py_files.py --apply.',
    'Real broker adapter is still intentionally red-light/stub until a real broker API contract and credentials are provided.',
]
