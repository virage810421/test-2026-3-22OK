import os
import sys
import json
import time
import traceback
import subprocess
from datetime import datetime

LOG_DIR = "runtime_logs"
os.makedirs(LOG_DIR, exist_ok=True)

PIPELINE_LOG = os.path.join(LOG_DIR, "launcher_runtime.log")
STATUS_JSON = os.path.join(LOG_DIR, "launcher_status.json")


def log(msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{now}] {msg}"
    print(line)
    with open(PIPELINE_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def save_status(stage, status, detail=""):
    payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stage": stage,
        "status": status,
        "detail": detail,
    }
    with open(STATUS_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def run_command(cmd, stage_name=None, timeout=3600, critical=True):
    stage_name = stage_name or " ".join(cmd)

    log(f"🚀 啟動 {stage_name}")
    save_status(stage_name, "RUNNING")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
        )

        if result.stdout:
            log(f"📄 {stage_name} STDOUT 開始")
            for line in result.stdout.splitlines():
                log(f"   {line}")
            log(f"📄 {stage_name} STDOUT 結束")

        if result.stderr:
            log(f"⚠️ {stage_name} STDERR 開始")
            for line in result.stderr.splitlines():
                log(f"   {line}")
            log(f"⚠️ {stage_name} STDERR 結束")

        if result.returncode != 0:
            msg = f"{stage_name} 執行失敗，returncode={result.returncode}"
            log(f"❌ {msg}")
            save_status(stage_name, "FAILED", msg)
            if critical:
                raise RuntimeError(msg)
            return False

        log(f"✅ 完成 {stage_name}")
        save_status(stage_name, "SUCCESS")
        return True

    except subprocess.TimeoutExpired:
        msg = f"{stage_name} 執行超時"
        log(f"⏰ {msg}")
        save_status(stage_name, "TIMEOUT", msg)
        if critical:
            raise
        return False
    except Exception as e:
        msg = f"{stage_name} 發生例外：{e}"
        log(f"❌ {msg}")
        save_status(stage_name, "EXCEPTION", msg)
        if critical:
            raise
        return False


def run_script(script_name, timeout=3600, critical=True, extra_args=None):
    if not os.path.exists(script_name):
        msg = f"找不到檔案：{script_name}"
        log(f"❌ {msg}")
        if critical:
            raise FileNotFoundError(msg)
        return False

    cmd = [sys.executable, script_name]
    if extra_args:
        cmd.extend(extra_args)
    return run_command(cmd, stage_name=script_name, timeout=timeout, critical=critical)


def write_daily_summary(start_time, final_status):
    duration = round(time.time() - start_time, 2)
    summary_path = os.path.join(LOG_DIR, "daily_launcher_summary.json")
    payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "final_status": final_status,
        "duration_sec": duration,
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    log(f"📦 已輸出摘要：{summary_path}")


def main():
    start_time = time.time()
    today = datetime.now().weekday()
    is_weekend = today >= 5

    log("=" * 70)
    log("🛰️ 系統總司令部啟動：全自動部署 / 排程 / 巡航啟動器")
    log("=" * 70)

    try:
        # 1) schema / 欄位完整性（真正自動化）
        if os.path.exists("db_setup.py"):
            log("🔧 階段 1：自動執行 schema 補欄檢查")
            run_script("db_setup.py", timeout=1800, critical=True, extra_args=["--mode", "upgrade"])
        else:
            log("⚠️ 找不到 db_setup.py，略過 schema 檢查")

        # 2) 主流程
        log("🧠 階段 2：執行主流程 master_pipeline.py")
        run_script("master_pipeline.py", timeout=7200, critical=True)

        # 3) 監控中心
        if os.path.exists("monitor_center.py"):
            log("🩺 階段 3：執行監控中心")
            run_script("monitor_center.py", timeout=1800, critical=False)

        # 4) 守門員
        if os.path.exists("system_guard.py"):
            log("🛡️ 階段 4：執行系統守門員")
            run_script("system_guard.py", timeout=1800, critical=False)

        # 5) 週末加跑事件回測
        if is_weekend and os.path.exists("event_backtester.py"):
            log("📚 階段 5：週末事件驅動回測")
            run_script("event_backtester.py", timeout=7200, critical=False)

        write_daily_summary(start_time, "SUCCESS")
        save_status("launcher", "SUCCESS", "全流程完成")
        log("🎉 今日巡航完成")
        return 0

    except Exception as e:
        err = f"主啟動器失敗：{e}"
        log(f"🛑 {err}")
        tb = traceback.format_exc()
        for line in tb.splitlines():
            log(f"   {line}")
        write_daily_summary(start_time, "FAILED")
        save_status("launcher", "FAILED", err)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())