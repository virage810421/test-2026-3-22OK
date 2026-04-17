# -*- coding: utf-8 -*-
"""Patch manifest: entry/exit strictness parameter closed loop upgrade."""
PATCH_NAME = "entry_exit_param_closed_loop_upgrade"
PATCH_VERSION = "20260417_v1"
SUMMARY = {
    "goal": "Close the loop for entry/exit strictness parameters without allowing AI candidates to affect production directly.",
    "completed": [
        "Centralized entry/exit strictness defaults and hard safety bounds.",
        "Unified screening / decision desk / control tower thresholds.",
        "Connected PREPARE/PILOT/MISSING lifecycle days to approved params.",
        "Blocked out-of-bounds or protected approved params during mount.",
        "Added judge_candidate_by_id so optimizer auto-judge branches no longer fail.",
        "Added strictness diagnostic for too-loose / too-strict / mixed-unstable behaviour.",
        "Release gate now blocks live promotion when entry/exit strictness is not balanced.",
        "RANGE stale probes no longer jump directly to EXIT; default is DEFEND/REDUCE only.",
    ],
    "not_changed": [
        "Real broker connectivity remains intentionally untouched.",
        "Live auto promotion remains disabled unless explicit release gate and config allow it.",
        "Candidate params still do not write config.py or mutate production config directly.",
    ],
    "updated_files": [
        "config.py",
        "fts_entry_exit_param_policy.py",
        "fts_entry_exit_strictness_diagnostic.py",
        "fts_approved_param_mount.py",
        "fts_candidate_ai_judge.py",
        "fts_control_tower.py",
        "fts_decision_desk_builder.py",
        "fts_entry_tracking_service.py",
        "fts_execution_param_candidate_judge.py",
        "fts_label_policy_candidate_judge.py",
        "fts_param_governance_orchestrator.py",
        "fts_param_release_gate.py",
        "fts_position_lifecycle_service.py",
        "fts_screening_engine.py",
        "fts_strategy_param_candidate_judge.py",
        "fts_training_data_builder.py",
    ],
}
