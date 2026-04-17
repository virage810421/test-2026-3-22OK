# -*- coding: utf-8 -*-
"""Central entry/exit strictness parameter policy.

This module closes the candidate->approved->mount loop for entry/exit
strictness parameters.

Rules
-----
1. All entry/exit modules should read the same keys through this module.
2. AI/optimizer candidates may suggest values, but mount only accepts values
   inside explicit safety bounds.
3. The module diagnoses too-loose / too-strict behaviour so optimizers do not
   chase only return or only trade count.
4. It never writes production config and never enables live trading.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any
import json

try:
    from fts_utils import now_str  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec="seconds")

# Conservative but not over-tight defaults. These are deliberately suitable for
# daily/paper/shadow. Live still requires release-gate + promoted_for_live.
ENTRY_EXIT_DEFAULTS: dict[str, Any] = {
    "PREENTRY_WATCH_THRESHOLD": 0.42,
    "PREENTRY_PILOT_THRESHOLD": 0.55,
    "CONFIRM_FULL_THRESHOLD": 0.63,
    "ENTRY_READINESS_PREPARE_MIN": 0.40,
    "PILOT_CONFIRM_MARGIN": 0.02,
    "PILOT_CONFIRM_MIN": 0.48,
    "PILOT_MAX_BREAKOUT_RISK": 0.86,
    "FULL_MAX_BREAKOUT_RISK": 0.78,
    "EXIT_WARN_HAZARD": 0.45,
    "STATE_EXIT_REDUCE_HAZARD": 0.60,
    "STATE_EXIT_DEFEND_HAZARD": 0.72,
    "STATE_EXIT_HARD_EXIT": 0.88,
    "PREPARE_MAX_DAYS": 7,
    "PILOT_MAX_DAYS": 6,
    "MISSING_SIGNAL_DAYS": 3,
    # Position lifecycle: RANGE should not jump directly to EXIT on stale probes.
    "RANGE_STALE_ACTION": "DEFEND",
    "RANGE_REDUCE_FRACTION": 0.45,
    "PILOT_REDUCE_FRACTION": 0.30,
    "FULL_REDUCE_FRACTION": 0.50,
}

# Hard safety ranges. Candidate params outside these values must be rejected
# or excluded from approved mount instead of silently changing production risk.
ENTRY_EXIT_PARAM_BOUNDS: dict[str, tuple[float, float]] = {
    "PREENTRY_WATCH_THRESHOLD": (0.38, 0.46),
    "PREENTRY_PILOT_THRESHOLD": (0.52, 0.60),
    "CONFIRM_FULL_THRESHOLD": (0.60, 0.68),
    "ENTRY_READINESS_PREPARE_MIN": (0.35, 0.48),
    "PILOT_CONFIRM_MARGIN": (0.00, 0.05),
    "PILOT_CONFIRM_MIN": (0.45, 0.52),
    "PILOT_MAX_BREAKOUT_RISK": (0.78, 0.90),
    "FULL_MAX_BREAKOUT_RISK": (0.68, 0.84),
    "EXIT_WARN_HAZARD": (0.40, 0.52),
    "STATE_EXIT_REDUCE_HAZARD": (0.58, 0.65),
    "STATE_EXIT_DEFEND_HAZARD": (0.70, 0.80),
    "STATE_EXIT_HARD_EXIT": (0.84, 0.92),
    "PREPARE_MAX_DAYS": (5, 9),
    "PILOT_MAX_DAYS": (4, 8),
    "MISSING_SIGNAL_DAYS": (2, 4),
    "RANGE_REDUCE_FRACTION": (0.25, 0.60),
    "PILOT_REDUCE_FRACTION": (0.15, 0.45),
    "FULL_REDUCE_FRACTION": (0.30, 0.65),
}

ENTRY_EXIT_ALLOWED_ENUMS: dict[str, set[str]] = {
    "RANGE_STALE_ACTION": {"DEFEND", "REDUCE"},  # explicitly blocks direct stale EXIT in RANGE.
}

ENTRY_EXIT_TUNABLE_KEYS = set(ENTRY_EXIT_DEFAULTS) | set(ENTRY_EXIT_PARAM_BOUNDS) | set(ENTRY_EXIT_ALLOWED_ENUMS)

# Non-negotiable safety keys. Candidate/approved entry-exit params may never
# open these switches or weaken hard live governance through this mount path.
PROTECTED_PARAM_PREFIXES = {
    "CANDIDATE_",
    "PARAM_RELEASE_",
    "LIVE_REQUIRE_",
    "MODEL_MIN_OOT_",
    "MODEL_MIN_PROMOTION_",
    "KILL_SWITCH",
    "ALLOW_LIVE",
    "BROKER_",
    "TRUE_BROKER",
}
PROTECTED_PARAM_KEYS = {
    "ALLOW_LIVE_TRADING",
    "LIVE_TRADING_ENABLED",
    "ENABLE_REAL_BROKER",
    "FEATURE_PARITY_MODE",
    "LIVE_FEATURE_PARITY_MODE",
    "MODEL_BLOCK_LIVE_ON_UNPROMOTED",
}


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except Exception:
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(round(float(value)))
    except Exception:
        return int(default)


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _is_protected_key(key: str) -> bool:
    key = str(key)
    return key in PROTECTED_PARAM_KEYS or any(key.startswith(prefix) for prefix in PROTECTED_PARAM_PREFIXES)


def _base_with_defaults(params: dict[str, Any] | None = None) -> dict[str, Any]:
    merged = deepcopy(ENTRY_EXIT_DEFAULTS)
    if isinstance(params, dict):
        merged.update(params)
    return merged


def validate_entry_exit_params(params: dict[str, Any] | None, *, strict_unknown: bool = False) -> dict[str, Any]:
    """Validate a candidate or approved payload.

    Returns a diagnostic object. This does not mutate inputs.
    """
    params = dict(params or {})
    hard_failures: list[str] = []
    warnings: list[str] = []
    validated: dict[str, Any] = {}
    rejected_keys: list[str] = []

    for key, value in params.items():
        k = str(key)
        if _is_protected_key(k):
            hard_failures.append(f"protected_key_not_mountable:{k}")
            rejected_keys.append(k)
            continue
        if k in ENTRY_EXIT_ALLOWED_ENUMS:
            enum_value = str(value).upper().strip()
            if enum_value not in ENTRY_EXIT_ALLOWED_ENUMS[k]:
                hard_failures.append(f"enum_out_of_bounds:{k}={value}")
                rejected_keys.append(k)
                continue
            validated[k] = enum_value
            continue
        if k in ENTRY_EXIT_PARAM_BOUNDS:
            low, high = ENTRY_EXIT_PARAM_BOUNDS[k]
            numeric_value = _num(value, None)  # type: ignore[arg-type]
            if numeric_value is None:
                hard_failures.append(f"non_numeric_param:{k}")
                rejected_keys.append(k)
                continue
            if numeric_value < low or numeric_value > high:
                hard_failures.append(f"param_out_of_bounds:{k}={numeric_value} allowed=[{low},{high}]")
                rejected_keys.append(k)
                continue
            validated[k] = int(round(numeric_value)) if k.endswith("_DAYS") else float(numeric_value)
            continue
        if strict_unknown and k not in ENTRY_EXIT_TUNABLE_KEYS:
            warnings.append(f"unknown_non_entry_exit_key:{k}")
        validated[k] = value

    # Cross-parameter invariants prevent hidden too-loose / too-tight states.
    merged = _base_with_defaults(validated)
    watch = _num(merged.get("PREENTRY_WATCH_THRESHOLD"), ENTRY_EXIT_DEFAULTS["PREENTRY_WATCH_THRESHOLD"])
    pilot = _num(merged.get("PREENTRY_PILOT_THRESHOLD"), ENTRY_EXIT_DEFAULTS["PREENTRY_PILOT_THRESHOLD"])
    full = _num(merged.get("CONFIRM_FULL_THRESHOLD"), ENTRY_EXIT_DEFAULTS["CONFIRM_FULL_THRESHOLD"])
    reduce_h = _num(merged.get("STATE_EXIT_REDUCE_HAZARD"), ENTRY_EXIT_DEFAULTS["STATE_EXIT_REDUCE_HAZARD"])
    defend_h = _num(merged.get("STATE_EXIT_DEFEND_HAZARD"), ENTRY_EXIT_DEFAULTS["STATE_EXIT_DEFEND_HAZARD"])
    hard_h = _num(merged.get("STATE_EXIT_HARD_EXIT"), ENTRY_EXIT_DEFAULTS["STATE_EXIT_HARD_EXIT"])
    pilot_risk = _num(merged.get("PILOT_MAX_BREAKOUT_RISK"), ENTRY_EXIT_DEFAULTS["PILOT_MAX_BREAKOUT_RISK"])
    full_risk = _num(merged.get("FULL_MAX_BREAKOUT_RISK"), ENTRY_EXIT_DEFAULTS["FULL_MAX_BREAKOUT_RISK"])
    prepare_days = _int(merged.get("PREPARE_MAX_DAYS"), ENTRY_EXIT_DEFAULTS["PREPARE_MAX_DAYS"])
    pilot_days = _int(merged.get("PILOT_MAX_DAYS"), ENTRY_EXIT_DEFAULTS["PILOT_MAX_DAYS"])
    missing_days = _int(merged.get("MISSING_SIGNAL_DAYS"), ENTRY_EXIT_DEFAULTS["MISSING_SIGNAL_DAYS"])

    if not (watch <= pilot <= full):
        hard_failures.append("entry_threshold_order_invalid:watch<=pilot<=full_required")
    if not (reduce_h < defend_h < hard_h):
        hard_failures.append("exit_hazard_order_invalid:reduce<defend<hard_required")
    if full - pilot < 0.04:
        warnings.append("entry_threshold_gap_small:full_minus_pilot_below_0.04")
    if full_risk > pilot_risk:
        hard_failures.append("risk_cap_order_invalid:full_risk_cap_must_not_exceed_pilot_risk_cap")
    if missing_days > min(prepare_days, pilot_days):
        warnings.append("missing_signal_days_longer_than_stage_window")

    return {
        "status": "pass" if not hard_failures else "fail",
        "hard_gate_pass": not hard_failures,
        "hard_failures": sorted(set(hard_failures)),
        "warnings": sorted(set(warnings)),
        "validated_params": validated,
        "rejected_keys": sorted(set(rejected_keys)),
        "bounds": {k: list(v) for k, v in ENTRY_EXIT_PARAM_BOUNDS.items()},
        "allowed_enums": {k: sorted(v) for k, v in ENTRY_EXIT_ALLOWED_ENUMS.items()},
    }


def filter_mountable_params(params: dict[str, Any] | None, *, keep_non_entry_exit: bool = True) -> tuple[dict[str, Any], list[str], dict[str, Any]]:
    """Return params that are safe to mount.

    Entry/exit tunable keys are rejected if outside bounds. Non entry/exit keys
    are kept unless they are protected because trainer/execution scopes may
    legitimately use other approved params.
    """
    params = dict(params or {})
    safe: dict[str, Any] = {}
    rejected: list[str] = []
    diagnostic = validate_entry_exit_params({k: v for k, v in params.items() if k in ENTRY_EXIT_TUNABLE_KEYS or _is_protected_key(str(k))})
    rejected.extend(diagnostic.get("rejected_keys", []))
    rejected.extend([x.split(":", 1)[-1].split("=", 1)[0] for x in diagnostic.get("hard_failures", []) if x.startswith("protected_key") or x.startswith("param_out_of_bounds") or x.startswith("enum_out_of_bounds") or x.startswith("non_numeric_param")])
    for key, value in params.items():
        k = str(key)
        if _is_protected_key(k):
            rejected.append(k)
            continue
        if k in ENTRY_EXIT_TUNABLE_KEYS:
            if k in diagnostic.get("validated_params", {}):
                safe[k] = diagnostic["validated_params"][k]
            else:
                rejected.append(k)
            continue
        if keep_non_entry_exit:
            safe[k] = value
    return safe, sorted(set(str(x) for x in rejected if x)), diagnostic


def coerce_entry_exit_params(params: dict[str, Any] | None) -> dict[str, Any]:
    """Runtime-safe params: merge defaults and clamp only for defensive reading.

    Approved mount should reject invalid candidates before this point. This
    helper prevents module-level fallbacks from drifting if config is missing.
    """
    merged = _base_with_defaults(params)
    for key, (low, high) in ENTRY_EXIT_PARAM_BOUNDS.items():
        value = _num(merged.get(key), ENTRY_EXIT_DEFAULTS.get(key, low))
        value = max(float(low), min(float(high), value))
        merged[key] = int(round(value)) if key.endswith("_DAYS") else float(value)
    for key, allowed in ENTRY_EXIT_ALLOWED_ENUMS.items():
        v = str(merged.get(key, ENTRY_EXIT_DEFAULTS.get(key, ""))).upper().strip()
        merged[key] = v if v in allowed else ENTRY_EXIT_DEFAULTS[key]
    return merged


def entry_thresholds(params: dict[str, Any] | None = None) -> tuple[float, float, float, float, float]:
    p = coerce_entry_exit_params(params)
    return (
        _num(p.get("PREENTRY_WATCH_THRESHOLD"), 0.42),
        _num(p.get("PREENTRY_PILOT_THRESHOLD"), 0.55),
        _num(p.get("CONFIRM_FULL_THRESHOLD"), 0.63),
        _num(p.get("ENTRY_READINESS_PREPARE_MIN"), 0.40),
        _num(p.get("PILOT_CONFIRM_MIN"), 0.48),
    )


def exit_thresholds(params: dict[str, Any] | None = None) -> tuple[float, float, float, float]:
    p = coerce_entry_exit_params(params)
    return (
        _num(p.get("EXIT_WARN_HAZARD"), 0.45),
        _num(p.get("STATE_EXIT_REDUCE_HAZARD"), 0.60),
        _num(p.get("STATE_EXIT_DEFEND_HAZARD"), 0.72),
        _num(p.get("STATE_EXIT_HARD_EXIT"), 0.88),
    )


def risk_caps(params: dict[str, Any] | None = None) -> tuple[float, float]:
    p = coerce_entry_exit_params(params)
    return (_num(p.get("PILOT_MAX_BREAKOUT_RISK"), 0.86), _num(p.get("FULL_MAX_BREAKOUT_RISK"), 0.78))


def lifecycle_limits(params: dict[str, Any] | None = None) -> tuple[int, int, int]:
    p = coerce_entry_exit_params(params)
    return (_int(p.get("PREPARE_MAX_DAYS"), 7), _int(p.get("PILOT_MAX_DAYS"), 6), _int(p.get("MISSING_SIGNAL_DAYS"), 3))


def evaluate_strictness_health(metrics: dict[str, Any] | None, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Detect too-loose / too-tight param behaviour.

    This is intentionally metric-name tolerant because different optimizers and
    runtime reports use different names.
    """
    m = dict(metrics or {})
    p = coerce_entry_exit_params(params)
    trade_count = _num(m.get("trade_count", m.get("trades", m.get("Signal_Count", 0))), 0.0)
    rejected_rate = _num(m.get("signal_reject_rate", m.get("reject_rate", m.get("gate_reject_rate", 0.0))), 0.0)
    max_drawdown = abs(_num(m.get("max_drawdown", m.get("MDD", 0.0)), 0.0))
    win_rate = _num(m.get("win_rate", m.get("hit_rate", 0.5)), 0.5)
    stop_rate = _num(m.get("stop_loss_rate", m.get("stop_hit_rate", 0.0)), 0.0)
    prepare_count = _num(m.get("prepare_count", 0), 0.0)
    pilot_count = _num(m.get("pilot_count", 0), 0.0)
    full_count = _num(m.get("full_entry_count", m.get("full_count", 0)), 0.0)
    pilot_to_full = _num(m.get("pilot_to_full_rate", (full_count / pilot_count if pilot_count else 0.0)), 0.0)
    empty_ratio = _num(m.get("empty_signal_ratio", m.get("cash_idle_ratio", 0.0)), 0.0)
    missed_after_exit = _num(m.get("exit_rebound_rate", m.get("missed_after_exit_rate", 0.0)), 0.0)
    activity_floor = _num(m.get("expected_min_trades", 8), 8.0)

    too_loose_flags: list[str] = []
    too_strict_flags: list[str] = []
    if trade_count > 0 and win_rate < 0.42:
        too_loose_flags.append("win_rate_low_with_activity")
    if max_drawdown > 0.22:
        too_loose_flags.append("max_drawdown_high")
    if stop_rate > 0.35:
        too_loose_flags.append("stop_loss_rate_high")
    if _num(p.get("PREENTRY_PILOT_THRESHOLD"), 0.55) <= 0.53 and _num(p.get("CONFIRM_FULL_THRESHOLD"), 0.63) <= 0.61:
        too_loose_flags.append("entry_thresholds_near_floor")
    if _num(p.get("STATE_EXIT_REDUCE_HAZARD"), 0.60) >= 0.64 and _num(p.get("STATE_EXIT_HARD_EXIT"), 0.88) >= 0.90:
        too_loose_flags.append("exit_thresholds_near_ceiling")

    if trade_count < activity_floor:
        too_strict_flags.append("trade_count_below_activity_floor")
    if rejected_rate > 0.55:
        too_strict_flags.append("gate_reject_rate_high")
    if prepare_count > 0 and pilot_count == 0 and full_count == 0:
        too_strict_flags.append("prepare_never_converts")
    if pilot_count >= 3 and pilot_to_full < 0.15:
        too_strict_flags.append("pilot_to_full_rate_low")
    if empty_ratio > 0.70:
        too_strict_flags.append("cash_idle_or_empty_signal_ratio_high")
    if missed_after_exit > 0.30:
        too_strict_flags.append("exit_rebound_rate_high")
    if _num(p.get("PREENTRY_PILOT_THRESHOLD"), 0.55) >= 0.59 and _num(p.get("CONFIRM_FULL_THRESHOLD"), 0.63) >= 0.67:
        too_strict_flags.append("entry_thresholds_near_ceiling")
    if _num(p.get("STATE_EXIT_REDUCE_HAZARD"), 0.60) <= 0.59:
        too_strict_flags.append("reduce_hazard_near_floor")

    if too_loose_flags and too_strict_flags:
        status = "mixed_unstable"
    elif too_loose_flags:
        status = "too_loose"
    elif too_strict_flags:
        status = "too_strict"
    else:
        status = "balanced_or_insufficient_evidence"

    strictness_score = min(100.0, 100.0 * (
        0.25 * min(rejected_rate, 1.0)
        + 0.20 * (1.0 if trade_count < activity_floor else 0.0)
        + 0.20 * min(empty_ratio, 1.0)
        + 0.20 * (1.0 - min(pilot_to_full, 1.0) if pilot_count >= 3 else 0.0)
        + 0.15 * min(missed_after_exit, 1.0)
    ))
    looseness_score = min(100.0, 100.0 * (
        0.30 * min(max_drawdown / 0.25, 1.0)
        + 0.25 * min(stop_rate / 0.35, 1.0)
        + 0.20 * (1.0 if trade_count > max(activity_floor * 4, 30) and win_rate < 0.45 else 0.0)
        + 0.15 * (1.0 if _num(p.get("PREENTRY_PILOT_THRESHOLD"), 0.55) <= 0.53 else 0.0)
        + 0.10 * (1.0 if _num(p.get("STATE_EXIT_REDUCE_HAZARD"), 0.60) >= 0.64 else 0.0)
    ))
    return {
        "status": status,
        "strictness_score": round(strictness_score, 4),
        "looseness_score": round(looseness_score, 4),
        "too_strict_flags": sorted(set(too_strict_flags)),
        "too_loose_flags": sorted(set(too_loose_flags)),
        "metrics_used": {
            "trade_count": trade_count,
            "rejected_rate": rejected_rate,
            "max_drawdown": max_drawdown,
            "win_rate": win_rate,
            "stop_rate": stop_rate,
            "prepare_count": prepare_count,
            "pilot_count": pilot_count,
            "full_count": full_count,
            "pilot_to_full_rate": pilot_to_full,
            "empty_signal_ratio": empty_ratio,
            "exit_rebound_rate": missed_after_exit,
        },
    }


def candidate_hard_gate(candidate: dict[str, Any] | None) -> dict[str, Any]:
    candidate = dict(candidate or {})
    params = candidate.get("params", {}) if isinstance(candidate.get("params", {}), dict) else {}
    metrics = candidate.get("metrics", {}) if isinstance(candidate.get("metrics", {}), dict) else {}
    validation = validate_entry_exit_params(params)
    strictness = evaluate_strictness_health(metrics, params)
    hard_failures = list(validation.get("hard_failures", []))
    warnings = list(validation.get("warnings", []))

    # Treat clearly unstable behaviour as hard fail. "Insufficient evidence" is
    # allowed to remain a warning because evidence collector/release gate will
    # fail closed for paper/shadow/live.
    if strictness["status"] == "too_loose":
        hard_failures.append("strictness_health_too_loose")
    elif strictness["status"] == "mixed_unstable":
        hard_failures.append("strictness_health_mixed_unstable")
    elif strictness["status"] == "too_strict":
        warnings.append("strictness_health_too_strict")

    return {
        "hard_gate_pass": not hard_failures,
        "hard_failures": sorted(set(hard_failures)),
        "warnings": sorted(set(warnings)),
        "validation": validation,
        "strictness_health": strictness,
    }


def write_policy_report(path: str | Path = Path("runtime") / "entry_exit_param_policy_report.json", params: dict[str, Any] | None = None, metrics: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "generated_at": now_str(),
        "status": "entry_exit_param_policy_ready",
        "defaults": ENTRY_EXIT_DEFAULTS,
        "bounds": {k: list(v) for k, v in ENTRY_EXIT_PARAM_BOUNDS.items()},
        "allowed_enums": {k: sorted(v) for k, v in ENTRY_EXIT_ALLOWED_ENUMS.items()},
        "effective_params": coerce_entry_exit_params(params or {}),
        "strictness_health": evaluate_strictness_health(metrics or {}, params or {}),
        "writes_production_config": False,
        "promotes_live": False,
    }
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> int:
    payload = write_policy_report()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
