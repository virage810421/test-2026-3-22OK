# -*- coding: utf-8 -*-
PATCH_NAME = '設計不良修改5 收尾包'
PATCH_SCOPE = [
    'fts_pipeline.py',
    'fts_signal_gate.py',
    'fts_decision_desk_builder.py',
    'fts_feature_observability.py',
    'fts_live_readiness_gate.py',
    'fts_control_tower.py',
    'fts_model_layer.py',
    'fts_project_healthcheck.py',
    'fts_broker_real_stub.py',
]
PATCH_COMPLETED = {
    'legacy_pipeline_default_disabled': True,
    'heuristic_role_demoted_to_diagnostic': True,
    'fallback_decision_rows_require_review': True,
    'live_feature_observability_added': True,
    'prelive_vs_true_broker_scoring_split': True,
    'healthcheck_retired_wrapper_false_failures_removed': True,
    'true_broker_stub_transparency_improved': True,
}
PATCH_NOT_FULLY_COMPLETED = {
    'physical_module_count_reduction': '未直接合併/刪除大量模組，這是結構整理工程，不是單純 patch 可一次完成。',
    'real_broker_sdk_integration': '仍需實際券商 SDK/API、callback、真對帳才能 100% 完成。',
}
