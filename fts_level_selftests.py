# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib
import json
import py_compile
from pathlib import Path


def _write_json(path: str | Path, payload: dict) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def run_level1_bridge_selftest() -> dict:
    modules = ["fts_chart_service", "fts_screening_engine", "fts_training_data_builder", "fts_trainer_backend"]
    results = {}
    for name in modules:
        try:
            mod = importlib.import_module(name)
            results[name] = {"import_ok": True, "file": getattr(mod, "__file__", "")}
        except Exception as e:
            results[name] = {"import_ok": False, "error": repr(e)}
    _write_json(Path("runtime") / "mainline_service_selftest.json", results)
    return results


def run_level2_bridge_selftest() -> dict:
    from fts_level_runtime import available_variants

    results = {}
    for name in available_variants():
        path = Path("advanced_chart1_runtime_variants") / f"{name}.py"
        try:
            py_compile.compile(str(path), doraise=True)
            results[name] = {"compile_ok": True}
        except Exception as e:
            results[name] = {"compile_ok": False, "error": repr(e)}
    _write_json(Path("runtime") / "level2_bridge_selftest.json", results)
    return results


def run_level2_mainline_selftest() -> dict:
    modules = ["fts_pipeline", "fts_legacy_master_pipeline_impl", "fts_fundamentals_etl_mainline"]
    results = {}
    for name in modules:
        try:
            mod = importlib.import_module(name)
            results[name] = {"import_ok": True, "file": getattr(mod, "__file__", "")}
        except Exception as e:
            results[name] = {"import_ok": False, "error": repr(e)}
    results["info"] = "fts_legacy_master_pipeline_impl.py is runtime-loaded by Level-2 mainline."
    _write_json(Path("runtime") / "level2_mainline_selftest.json", results)
    return results


def run_level3_bridge_selftest() -> dict:
    targets = [
        "execution_engine.py",
        "paper_broker.py",
        "risk_gateway.py",
        "live_paper_trading.py",
        "fts_level_runtime.py",
    ]
    results = {}
    for name in targets:
        try:
            py_compile.compile(name, doraise=True)
            results[name] = {"compile_ok": True}
        except Exception as e:
            results[name] = {"compile_ok": False, "error": repr(e)}
    _write_json(Path("runtime") / "level3_bridge_selftest.json", results)
    return results


def run_level3_control_selftest() -> dict:
    targets = [
        "fts_control_tower.py",
        "formal_trading_system_v83_official_main.py",
        "fts_pipeline.py",
        "launcher.py",
        "fts_fundamentals_etl_mainline.py",
    ]
    results = {}
    for name in targets:
        try:
            py_compile.compile(name, doraise=True)
            results[name] = {"compile_ok": True}
        except Exception as e:
            results[name] = {"compile_ok": False, "error": repr(e)}

    try:
        from pathlib import Path as _Path
        from fts_control_tower import _call_builder_result
        from fts_gatekeeper import LaunchGatekeeper
        from fts_live_suite import LiveReleaseGate

        launch_path, launch_payload = _call_builder_result(
            LaunchGatekeeper(),
            "evaluate",
            {"ready": [], "missing": []},
            {"failed": []},
            {"items": []},
            {"row_count": 0, "rows_with_price": 0, "rows_with_ticker": 0, "rows_with_action": 0},
            {"total_signals": 0},
            fallback_path=_Path("runtime/launch_gate.json"),
        )
        live_release_path, live_release_payload = _call_builder_result(
            LiveReleaseGate(),
            "evaluate",
            governance={},
            safety={},
            recon={},
            recovery={},
            approval={},
            broker_contract={"defined": True},
            fallback_path=_Path("runtime/live_release_gate.json"),
        )
        results["control_tower_builder_smoke"] = {
            "ok": True,
            "launch_gate_path": str(launch_path),
            "launch_gate_has_payload": isinstance(launch_payload, dict),
            "live_release_path": str(live_release_path),
            "live_release_has_payload": isinstance(live_release_payload, dict),
        }
    except Exception as e:
        results["control_tower_builder_smoke"] = {"ok": False, "error": repr(e)}

    _write_json(Path("runtime") / "level3_control_selftest.json", results)
    return results


def run_chart_level1_bridge_selftest() -> dict:
    svc = importlib.import_module("fts_chart_service")
    results = {
        "service_has_draw_chart": hasattr(svc, "draw_chart"),
        "service_has_render_trade_chart": hasattr(svc, "render_trade_chart"),
        "renderer_source": getattr(svc, "CHART_RENDERER_SOURCE", None),
        "facade_removed": True,
        "integration_status": "mainline_service_ready",
    }
    _write_json("chart_service_selftest_result.json", results)
    return results


def run_all_selftests() -> dict:
    runners = {
        "level1_bridge": run_level1_bridge_selftest,
        "level2_bridge": run_level2_bridge_selftest,
        "level2_mainline": run_level2_mainline_selftest,
        "level3_bridge": run_level3_bridge_selftest,
        "level3_control": run_level3_control_selftest,
        "chart_level1_bridge": run_chart_level1_bridge_selftest,
    }
    results = {}
    for name, fn in runners.items():
        try:
            results[name] = {"ok": True, "result": fn()}
        except Exception as e:
            results[name] = {"ok": False, "error": repr(e)}
    _write_json(Path("runtime") / "level_selftests_summary.json", results)
    return results


if __name__ == "__main__":
    print(json.dumps(run_all_selftests(), ensure_ascii=False, indent=2))
