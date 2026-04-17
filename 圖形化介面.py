# -*- coding: utf-8 -*-
"""FTS 視窗化指令中心。
放在專案根目錄後執行：python fts_command_center_gui.py
"""
from __future__ import annotations

import locale
import os
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable or "python"
HEARTBEAT_SECONDS = 5
BOOTSTRAP_DATABASE_NAME = "股票Online"
ZERO_NULL_REPAIR_SCRIPT = "fts_zero_null_sql_csv_repair.py"
ZERO_NULL_REPAIR_DATASETS = "fundamentals,revenue,chip"


# Windows 中文輸出常見是 CP950/Big5；子程序也可能被 PYTHONIOENCODING 強制成 UTF-8。
# GUI 這裡用二進位讀取，再自動嘗試多種解碼，避免中文變成亂碼。
def _decode_output(data: bytes) -> str:
    if not data:
        return ""
    encodings = [
        "utf-8-sig",
        "utf-8",
        locale.getpreferredencoding(False),
        "cp950",
        "big5",
        "mbcs",
    ]
    seen = set()
    for enc in encodings:
        if not enc or enc in seen:
            continue
        seen.add(enc)
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")



def _format_elapsed(seconds: float) -> str:
    total = max(0, int(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


@dataclass(frozen=True)
class CommandItem:
    category: str
    name: str
    args: List[str]
    desc: str
    note: str = ""
    confirm: bool = False
    dangerous: bool = False
    long_running: bool = False


COMMANDS = [
    CommandItem("主程式", "初始化 / 第一次啟動", ["formal_trading_system_v83_official_main.py", "--bootstrap"], "新電腦、重新建資料庫、第一次啟動時使用。會先確保建立資料庫【股票Online】、再建置 runtime、補本地資料、檢查 SQL 與系統狀態。", "此流程可能需要數分鐘；若資料或網路較慢，中途可能短暫沒有新輸出。", True, False, True),
    CommandItem("主程式", "日常執行", ["formal_trading_system_v83_official_main.py"], "日常主流程。", "預設就是日常模式。", False, False, True),
    CommandItem("主程式", "訓練模式", ["formal_trading_system_v83_official_main.py", "--train"], "建立/更新訓練資料並訓練模型。", "需要資料與 labels 充足；第一次可能花較久。", True, False, True),
    CommandItem("資料庫", "資料庫升級 / 建立資料表", ["fts_db_migrations.py", "upgrade"], "建立或升級 SQL 資料表、欄位與中文欄名查詢 view。", "重新建資料庫後第一個要跑。", True, False, True),
    CommandItem("檢查", "深度健康檢查", ["fts_admin_cli.py", "healthcheck", "--deep"], "深度檢查專案：語法編譯、核心 import、三路流程與 exception policy。", "每次覆蓋更新檔後建議執行。", False, False, True),
    CommandItem("檢查", "舊欄位 / 可刪除性檢查（含資料庫）", ["fts_admin_cli.py", "drop-readiness", "--check-db"], "檢查舊欄位 / 可刪除性，並連 SQL Server 檢查欄位狀態。", "用來判斷是否可以做破壞式清理。", False, False, True),
    CommandItem("清理", "第二輪舊檔清理預覽", ["fts_admin_cli.py", "second-merge-cleanup"], "第二輪合併汰除預覽，只產生報告，不刪檔。", "先看 ready / blocked / missing。"),
    CommandItem("清理", "第二輪舊檔清理套用", ["fts_admin_cli.py", "second-merge-cleanup", "--apply"], "實際刪除已判定 ready 的舊檔案。", "危險：請先確認深度健康檢查通過。", True, True),
    CommandItem("實盤前 95%", "券商合約檢查", ["fts_admin_cli.py", "broker-contract-audit"], "檢查券商 adapter 是否具備 connect / place / cancel / replace / query / callback 等必要方法。"),
    CommandItem("實盤前 95%", "券商回報匯入", ["fts_admin_cli.py", "callback-ingest"], "匯入並標準化券商 callback，寫入 callback runtime。"),
    CommandItem("實盤前 95%", "對帳 runtime 檢查", ["fts_admin_cli.py", "reconciliation-runtime"], "執行對帳 runtime，檢查委託、成交、持倉、現金是否一致。"),
    CommandItem("實盤前 95%", "重啟恢復檢查", ["fts_admin_cli.py", "restart-recovery"], "檢查系統重啟後是否能恢復 working orders / positions，並產生 recovery plan。"),
    CommandItem("實盤前 95%", "出場 AI 模型產生", ["fts_admin_cli.py", "exit-artifact-bootstrap"], "產生出場 AI artifacts；沒有 exit labels 時不會產生假模型。", "需要 exit labels。", True, False, True),
    CommandItem("回測", "投組回測 3 年", ["fts_admin_cli.py", "portfolio-backtest", "--period", "3y"], "投組層級回測，輸出 equity curve、drawdown、分股票 / 分策略統計。", "回測可能需要一些時間。", False, False, True),
    CommandItem("實盤前 95%", "實盤前 95% 總檢", ["fts_admin_cli.py", "prebroker-95-audit", "--run-backtest", "--bootstrap-exit"], "實盤前閉環總檢：券商合約、callback、ledger、對帳、重啟恢復、出場模型、回測。", "最重要的總檢，可能需要數分鐘。", True, False, True),
    CommandItem("資料/特徵", "全市場百分位快照", ["fts_admin_cli.py", "full-market-percentile"], "建立全市場 percentile / ranking 快照，供選股與排序使用。", "資料量大時可能需要較久。", False, False, True),
    CommandItem("資料/特徵", "事件日曆建立", ["fts_admin_cli.py", "event-calendar-build"], "建立事件日曆，例如財報、月營收、特殊事件窗口。"),
    CommandItem("資料/特徵", "同步特徵快照到 SQL", ["fts_admin_cli.py", "sync-feature-snapshots"], "把 feature snapshots 同步寫入 SQL。", "資料多時可能需要較久。", False, False, True),
    CommandItem("稽核", "訓練壓力測試", ["fts_admin_cli.py", "training-stress-audit"], "執行訓練壓力測試與穩定性稽核。", "檢查訓練流程安全性，不是正式訓練。", False, False, True),
    CommandItem("稽核", "資料回補韌性檢查", ["fts_admin_cli.py", "backfill-resilience-audit"], "檢查資料回補、缺口修復、local-first 韌性。", "資料多時可能需要較久。", False, False, True),
    CommandItem("資料修補", "零值 / 空值掃描（dry-run）", [ZERO_NULL_REPAIR_SCRIPT, "--datasets", ZERO_NULL_REPAIR_DATASETS], "掃描本地 CSV / SQL 的 0 值與空值，並到網路確認是本來就為 0/空值，還是抓取遺漏。只產生報告，不寫回。", "建議先跑這個確認報告，再決定是否正式補值。會輸出 runtime/zero_null_repair_report.json。", False, False, True),
    CommandItem("資料修補", "零值 / 空值正式補值（apply）", [ZERO_NULL_REPAIR_SCRIPT, "--datasets", ZERO_NULL_REPAIR_DATASETS, "--apply"], "正式把確認屬於遺漏的數值補回本地 CSV 與 SQL。", "危險：會改寫 CSV 與 SQL；腳本會先自動備份 CSV。建議先完成 dry-run。", True, True, True),
    CommandItem("資料修補", "只修基本面（apply）", [ZERO_NULL_REPAIR_SCRIPT, "--datasets", "fundamentals", "--apply"], "只針對 fundamentals_clean / 基本面 CSV 進行 0 值與空值回補。", "適合先小範圍驗證流程。", True, True, True),
]

FLOWS = {
    "新電腦 / 重建資料庫標準流程": [
        ["fts_db_migrations.py", "upgrade"],
        ["fts_admin_cli.py", "healthcheck", "--deep"],
        ["fts_admin_cli.py", "drop-readiness", "--check-db"],
        ["formal_trading_system_v83_official_main.py", "--bootstrap"],
    ],
    "實盤前 95% 總檢流程": [
        ["fts_admin_cli.py", "healthcheck", "--deep"],
        ["fts_admin_cli.py", "prebroker-95-audit", "--run-backtest", "--bootstrap-exit"],
    ],
    "清理前安全檢查流程": [
        ["fts_admin_cli.py", "second-merge-cleanup"],
        ["fts_admin_cli.py", "healthcheck", "--deep"],
    ],
    "零值 / 空值修補標準流程": [
        [ZERO_NULL_REPAIR_SCRIPT, "--datasets", ZERO_NULL_REPAIR_DATASETS],
        [ZERO_NULL_REPAIR_SCRIPT, "--datasets", ZERO_NULL_REPAIR_DATASETS, "--apply"],
    ],
}


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("FTS 視窗化指令中心（全中文按鍵版）")
        self.geometry("1220x800")
        self.proc = None
        self.q = queue.Queue()
        self.selected = COMMANDS[0]
        self.started_at: float | None = None
        self.next_heartbeat_at: float | None = None
        self.current_title = ""
        self._build()
        self.after(100, self._drain)

    def _build(self):
        paned = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        left = ttk.Frame(paned, width=430)
        right = ttk.Frame(paned)
        paned.add(left, weight=1)
        paned.add(right, weight=2)

        ttk.Label(left, text="指令按鍵", font=("Microsoft JhengHei", 13, "bold")).pack(anchor="w")
        nb = ttk.Notebook(left)
        nb.pack(fill=tk.BOTH, expand=True, pady=6)
        cats = []
        for c in [x.category for x in COMMANDS]:
            if c not in cats:
                cats.append(c)
        for cat in cats:
            f = ttk.Frame(nb)
            nb.add(f, text=cat)
            for item in [x for x in COMMANDS if x.category == cat]:
                ttk.Button(f, text=item.name, command=lambda it=item: self.select(it)).pack(fill=tk.X, padx=6, pady=3)
        lf = ttk.LabelFrame(left, text="一鍵流程")
        lf.pack(fill=tk.X, pady=6)
        for name, flow in FLOWS.items():
            ttk.Button(lf, text=name, command=lambda n=name, fl=flow: self.run_flow(n, fl)).pack(fill=tk.X, padx=6, pady=3)

        ttk.Label(right, text="說明", font=("Microsoft JhengHei", 13, "bold")).pack(anchor="w")
        self.info = tk.Text(right, height=11, wrap="word")
        self.info.pack(fill=tk.X, pady=6)

        bar = ttk.Frame(right)
        bar.pack(fill=tk.X)
        ttk.Button(bar, text="執行選取指令", command=self.run_selected).pack(side=tk.LEFT, padx=3)
        ttk.Button(bar, text="停止目前程序", command=self.stop_proc).pack(side=tk.LEFT, padx=3)
        ttk.Button(bar, text="清空紀錄", command=lambda: self.log.delete("1.0", tk.END)).pack(side=tk.LEFT, padx=3)
        self.status = ttk.Label(bar, text="準備就緒")
        self.status.pack(side=tk.RIGHT, padx=(12, 0))
        self.elapsed_label = ttk.Label(bar, text="已執行：00:00")
        self.elapsed_label.pack(side=tk.RIGHT, padx=(12, 0))

        self.progress = ttk.Progressbar(right, mode="indeterminate")
        self.progress.pack(fill=tk.X, pady=(8, 0))

        ttk.Label(right, text="執行紀錄", font=("Microsoft JhengHei", 12, "bold")).pack(anchor="w", pady=(10, 0))
        self.log = tk.Text(right, wrap="word")
        self.log.pack(fill=tk.BOTH, expand=True)
        self.select(self.selected)

    def _is_bootstrap_args(self, args: List[str]) -> bool:
        return "formal_trading_system_v83_official_main.py" in args and "--bootstrap" in args

    def _is_zero_null_repair_args(self, args: List[str]) -> bool:
        return bool(args) and args[0] == ZERO_NULL_REPAIR_SCRIPT

    def _is_zero_null_apply_args(self, args: List[str]) -> bool:
        return self._is_zero_null_repair_args(args) and "--apply" in args

    def _bootstrap_flow(self):
        return [
            ["fts_db_migrations.py", "upgrade"],
            ["formal_trading_system_v83_official_main.py", "--bootstrap"],
        ]

    def select(self, item):
        self.selected = item
        cmd = " ".join([PYTHON, "-u"] + item.args)
        self.info.delete("1.0", tk.END)
        extra = ""
        if self._is_bootstrap_args(item.args):
            extra = (
                f"\nBootstrap 提醒：這個流程會先確保建立資料庫【{BOOTSTRAP_DATABASE_NAME}】，再進行初始化。"
                "若正在讀本地 CSV、連 SQL、補 runtime 或檢查資料，畫面可能短暫沒有新輸出。"
                "APP 會每 5 秒顯示仍在執行。\n"
            )
        elif self._is_zero_null_repair_args(item.args):
            apply_text = "會正式寫回 CSV / SQL。" if self._is_zero_null_apply_args(item.args) else "只掃描與驗證，不會寫回 CSV / SQL。"
            extra = (
                f"\n資料修補提醒：這支會先掃描本地資料，再去網路確認 0 值與空值是真缺漏還是真實值。{apply_text}\n"
                "建議先跑 dry-run 看 runtime/zero_null_repair_report.json，再決定是否 apply。\n"
            )
        self.info.insert(
            tk.END,
            f"名稱：{item.name}\n"
            f"分類：{item.category}\n\n"
            f"用途：{item.desc}\n\n"
            f"備註：{item.note or '無'}\n"
            f"{extra}\n"
            f"實際指令：\n{cmd}\n",
        )

    def run_selected(self):
        item = self.selected
        if self._is_bootstrap_args(item.args):
            message = (
                f"要執行：{item.name}\n\n"
                f"此流程會先建立 / 升級資料庫【{BOOTSTRAP_DATABASE_NAME}】後，再執行 bootstrap。\n"
                "Bootstrap 可能需要數分鐘。\n"
                "執行期間 APP 會顯示計時器，並每 5 秒輸出『仍在執行』。\n\n"
                f"第一步：{PYTHON} -u fts_db_migrations.py upgrade\n"
                f"第二步：{PYTHON} -u formal_trading_system_v83_official_main.py --bootstrap"
            )
            if not messagebox.askyesno("確認執行 Bootstrap", message):
                return
            self.run_flow(f"{item.name}（含建立資料庫 {BOOTSTRAP_DATABASE_NAME}）", self._bootstrap_flow())
            return

        if self._is_zero_null_repair_args(item.args) and not self._is_zero_null_apply_args(item.args):
            cmd_text = " ".join([PYTHON, "-u"] + item.args)
            message = (
                f"要執行：{item.name}\n\n"
                "這是 dry-run，只會掃描本地 CSV / SQL 的 0 值與空值，並到網路確認是否為遺漏資料。\n"
                "不會改寫 CSV / SQL。\n\n"
                f"實際指令：{cmd_text}"
            )
            if not messagebox.askyesno("確認執行資料修補掃描", message):
                return
            self.run_cmd(item.args, title=item.name)
            return

        if self._is_zero_null_apply_args(item.args):
            cmd_text = " ".join([PYTHON, "-u"] + item.args)
            message = (
                f"要執行：{item.name}\n\n"
                "這會先比對網路來源，再把確認屬於遺漏的值寫回 CSV 與 SQL。\n"
                "建議你先跑 dry-run 確認 runtime/zero_null_repair_report.json。\n"
                "腳本會先備份 CSV，但這仍屬於有改寫風險的操作。\n\n"
                f"實際指令：{cmd_text}"
            )
            if not messagebox.askyesno("確認正式補值", message):
                return
            self.run_cmd(item.args, title=item.name)
            return

        if item.confirm or item.dangerous:
            if not messagebox.askyesno("確認執行", f"要執行：{item.name}\n\n{' '.join([PYTHON, '-u'] + item.args)}"):
                return
        self.run_cmd(item.args, title=item.name)

    def run_flow(self, name, flow):
        if not messagebox.askyesno("確認流程", f"要連續執行流程：{name}？\n\n長流程執行時會顯示計時器與每 5 秒心跳訊息。"):
            return

        def worker():
            for args in flow:
                ok = self._run_blocking(args, title="流程步驟")
                if not ok:
                    break

        threading.Thread(target=worker, daemon=True).start()

    def run_cmd(self, args, title=""):
        threading.Thread(target=lambda: self._run_blocking(args, title), daemon=True).start()

    def _run_blocking(self, args, title=""):
        if self.proc is not None:
            self.q.put("\n[圖形介面] 目前已有程序執行中，請等待結束或按『停止目前程序』。\n")
            return False

        cmd = [PYTHON, "-u"] + args
        self.started_at = time.monotonic()
        self.next_heartbeat_at = self.started_at + HEARTBEAT_SECONDS
        self.current_title = title or args[0]

        self.q.put(f"\n========== 執行：{self.current_title} ==========\n")
        self.q.put("$ " + " ".join(cmd) + "\n")
        if self._is_bootstrap_args(args):
            self.q.put("[圖形介面] Bootstrap 可能需要數分鐘。若中途沒有新輸出，請看右上角狀態與已執行時間。\n")
        elif self._is_zero_null_repair_args(args):
            if self._is_zero_null_apply_args(args):
                self.q.put("[圖形介面] 資料修補 apply 模式：會先驗證，再正式回寫 CSV / SQL。\n")
            else:
                self.q.put("[圖形介面] 資料修補 dry-run：只掃描與驗證，不會回寫 CSV / SQL。\n")
        self.q.put("[圖形介面] 已啟動程序，開始計時。\n")

        try:
            env = os.environ.copy()
            env.setdefault("PYTHONIOENCODING", "utf-8")
            env.setdefault("PYTHONUTF8", "1")
            env.setdefault("PYTHONUNBUFFERED", "1")
            self.proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                bufsize=0,
                text=False,
                env=env,
            )
            assert self.proc.stdout is not None
            for raw in iter(self.proc.stdout.readline, b""):
                if raw:
                    self.q.put(_decode_output(raw))
            rc = self.proc.wait()
            elapsed = _format_elapsed(time.monotonic() - (self.started_at or time.monotonic()))
            self.q.put(f"\n[圖形介面] 結束代碼={rc}，總耗時={elapsed}\n")
            return rc == 0
        except Exception as e:
            self.q.put(f"\n[圖形介面錯誤] {e!r}\n")
            return False
        finally:
            self.proc = None
            self.started_at = None
            self.next_heartbeat_at = None
            self.current_title = ""

    def stop_proc(self):
        if self.proc is not None:
            self.proc.terminate()
            self.q.put("\n[圖形介面] 已送出停止訊號。\n")
        else:
            self.q.put("\n[圖形介面] 目前沒有正在執行的程序。\n")

    def _drain(self):
        try:
            while True:
                s = self.q.get_nowait()
                self.log.insert(tk.END, s)
                self.log.see(tk.END)
        except queue.Empty:
            pass

        now = time.monotonic()
        if self.proc is not None and self.started_at is not None:
            elapsed = _format_elapsed(now - self.started_at)
            self.status.config(text=f"執行中：{self.current_title}")
            self.elapsed_label.config(text=f"已執行：{elapsed}")
            if self.next_heartbeat_at is not None and now >= self.next_heartbeat_at:
                self.q.put(f"[圖形介面] 仍在執行：{self.current_title}，已執行 {elapsed}。\n")
                self.next_heartbeat_at = now + HEARTBEAT_SECONDS
            try:
                self.progress.start(10)
            except tk.TclError:
                pass
        else:
            self.status.config(text="準備就緒")
            self.elapsed_label.config(text="已執行：00:00")
            try:
                self.progress.stop()
            except tk.TclError:
                pass

        self.after(100, self._drain)


if __name__ == "__main__":
    App().mainloop()
