# -*- coding: utf-8 -*-
"""v92_maturity_gap_closed_loop update manifest.

覆蓋檔案：
- fts_feature_review_service.py：特徵審核 fail-closed + train/live parity。
- fts_trainer_backend.py：approved_features_review 未 ready 時禁止繞過審核。
- fts_entry_tracking_service.py：PREPARE/PILOT/FULL_ENTRY 追蹤、過期 action plan。
- fts_position_lifecycle_service.py：持倉生命週期、移動停損、action plan、錯誤出場 proxy。
- fts_execution_journal_service.py：訊號/擋單/通過 Gate/paper order/fill 全部 journal helper。
- fts_control_tower.py：entry gate、position lifecycle gate 強制寫 execution journal；TRAIN 後重跑 maturity suite。
- fts_model_governance_enhancement.py：walk-forward/OOS/promotion/drift/model retention 檢查。
- fts_true_broker_readiness_gate.py：真券商 API/callback/ledger/reconcile/kill-switch 五紅燈 gate。
- fts_maturity_upgrade_suite.py：整合上述 v92 閉環補強。
"""

PATCH_VERSION = "v92_maturity_gap_closed_loop"
UPDATED_FILES = [
    "fts_feature_review_service.py",
    "fts_trainer_backend.py",
    "fts_entry_tracking_service.py",
    "fts_position_lifecycle_service.py",
    "fts_execution_journal_service.py",
    "fts_control_tower.py",
    "fts_model_governance_enhancement.py",
    "fts_true_broker_readiness_gate.py",
    "fts_maturity_upgrade_suite.py",
]
