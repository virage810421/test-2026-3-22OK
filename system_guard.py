# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 2 files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: alert_manager.py
# ==============================================================================
import atexit
import queue
import threading
from datetime import datetime

import requests


class AlertManager:
    def __init__(self, line_bot_token="", line_user_id="", is_test_mode=True, async_mode=True, request_timeout=6):
        self.line_bot_token = line_bot_token
        self.line_user_id = line_user_id
        self.is_test_mode = is_test_mode
        self.async_mode = async_mode
        self.request_timeout = int(request_timeout)
        self._queue: queue.Queue = queue.Queue(maxsize=500)
        self._stop = threading.Event()
        self._worker = None
        if self.async_mode and not self.is_test_mode:
            self._worker = threading.Thread(target=self._run_worker, name="line-alert-worker", daemon=True)
            self._worker.start()
            atexit.register(self.close)

    def _post_line_message(self, message: str) -> bool:
        if self.is_test_mode:
            print("\n🔇 [測試模式] 攔截告警推播")
            print("-" * 40)
            print(message)
            print("-" * 40)
            return True
        if not self.line_bot_token or not self.line_user_id:
            print("⚠️ LINE token / user id 未設定，無法推播")
            return False
        url = "https://api.line.me/v2/bot/message/push"
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.line_bot_token}"}
        payload = {"to": self.line_user_id, "messages": [{"type": "text", "text": message}]}
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=self.request_timeout)
            if resp.status_code == 200:
                print("📲 LINE 告警已送出")
                return True
            print(f"⚠️ LINE 推播失敗: {resp.status_code} | {resp.text}")
            return False
        except Exception as e:
            print(f"⚠️ LINE 推播例外: {e}")
            return False

    def _run_worker(self):
        while not self._stop.is_set() or not self._queue.empty():
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                self._queue.task_done()
                break
            try:
                self._post_line_message(item)
            finally:
                self._queue.task_done()

    def send_line_message(self, message: str):
        if self.async_mode and not self.is_test_mode:
            try:
                self._queue.put_nowait(message)
                return True
            except queue.Full:
                print("⚠️ LINE 告警佇列已滿，退回同步送出")
                return self._post_line_message(message)
        return self._post_line_message(message)

    def flush(self):
        if self.async_mode and not self.is_test_mode:
            try:
                self._queue.join()
            except Exception:
                pass

    def close(self):
        if self.async_mode and not self.is_test_mode:
            self._stop.set()
            try:
                self._queue.put_nowait(None)
            except Exception:
                pass
            if self._worker and self._worker.is_alive():
                self._worker.join(timeout=2)

    def format_guard_alert(self, payload: dict) -> str:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        overall = payload.get("overall", "UNKNOWN")
        block_new_positions = payload.get("block_new_positions", False)
        alerts = payload.get("alerts", [])
        desk = payload.get("decision_desk", {})
        trades = payload.get("recent_trades", {})
        models = payload.get("model_status", {})
        lines = [
            "🚨【交易系統守門員告警】",
            f"時間：{now_str}",
            f"總體狀態：{overall}",
            f"阻止建倉：{'是' if block_new_positions else '否'}",
            "-" * 20,
            f"模型狀態：{models.get('health', 'UNKNOWN')} ({models.get('ok_count', 0)}/4)",
            f"決策桌：{desk.get('health', 'UNKNOWN')} | rows={desk.get('rows', 0)} | avgEV={desk.get('avg_realized_ev', 0):.3f}",
            f"近期戰績：{trades.get('health', 'UNKNOWN')} | win={trades.get('win_rate', 0):.2%} | PF={trades.get('profit_factor', 0):.2f}",
            "-" * 20,
        ]
        if alerts:
            lines.extend([f"• {msg}" for msg in alerts[:8]])
        else:
            lines.append("• 無異常")
        return "\n".join(lines)

    def maybe_send_guard_alert(self, payload: dict):
        overall = payload.get("overall", "OK")
        if overall not in ("WARN", "BLOCK"):
            print("✅ 守門員狀態正常，略過告警推播")
            return False
        return self.send_line_message(self.format_guard_alert(payload))


# ==============================================================================
# Merged from: system_guard.py
# ==============================================================================
import json
import os
from datetime import datetime

import pandas as pd
import pyodbc

from config import PARAMS
from performance import check_strategy_health, get_strategy_summary
from fts_sql_table_name_map import sql_table

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)


TABLE_TRADE_HISTORY = sql_table('trade_history')


def _safe_float(x, default=0.0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def _read_csv_if_exists(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _read_sql(query):
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


def check_model_artifacts():
    required = {
        "selected_features": os.path.exists("models/selected_features.pkl"),
        "bull_model": os.path.exists("models/model_趨勢多頭.pkl"),
        "side_model": os.path.exists("models/model_區間盤整.pkl"),
        "bear_model": os.path.exists("models/model_趨勢空頭.pkl"),
    }
    ok_count = sum(1 for v in required.values() if v)
    if ok_count == 4:
        health = "OK"
    elif ok_count >= 2:
        health = "WARN"
    else:
        health = "FAIL"

    return {
        "health": health,
        "ok_count": ok_count,
        "items": required,
    }


def check_decision_desk():
    df = _read_csv_if_exists("daily_decision_desk.csv")
    if df.empty:
        return {
            "health": "FAIL",
            "rows": 0,
            "avg_ai_proba": 0.0,
            "avg_realized_ev": 0.0,
            "avg_score_gap": 0.0,
            "message": "決策桌為空",
        }

    avg_ai = _safe_float(df.get("AI_Proba", pd.Series(dtype=float)).mean(), 0.0)
    avg_ev = _safe_float(df.get("Realized_EV", pd.Series(dtype=float)).mean(), 0.0)
    avg_gap = _safe_float(df.get("Score_Gap", pd.Series(dtype=float)).mean(), 0.0)

    health = "OK"
    message = "決策桌正常"

    if len(df) < 3:
        health = "WARN"
        message = "決策桌標的偏少"
    if avg_ev <= 0:
        health = "WARN"
        message = "決策桌平均 EV 不為正"
    if avg_ai < 0.50:
        health = "WARN"
        message = "決策桌平均 AI 勝率偏低"
    if avg_gap <= 0:
        health = "WARN"
        message = "決策桌平均加權分差不為正"

    return {
        "health": health,
        "rows": int(len(df)),
        "avg_ai_proba": round(avg_ai, 4),
        "avg_realized_ev": round(avg_ev, 4),
        "avg_score_gap": round(avg_gap, 4),
        "message": message,
    }


def check_recent_trades(limit=100):
    df = _read_sql(f"""
        SELECT TOP {int(limit)} [報酬率(%)], [淨損益金額]
        FROM {TABLE_TRADE_HISTORY}
        ORDER BY [出場時間] DESC
    """)

    if df.empty:
        return {
            "health": "WARN",
            "sample_size": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "profit_factor": 0.0,
            "message": "近期無成交",
        }

    ret = pd.to_numeric(df["報酬率(%)"], errors="coerce").dropna()
    pnl = pd.to_numeric(df["淨損益金額"], errors="coerce").dropna()

    if ret.empty:
        return {
            "health": "WARN",
            "sample_size": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "profit_factor": 0.0,
            "message": "近期報酬缺失",
        }

    win_rate = float((ret > 0).mean())
    avg_return = float(ret.mean())
    gross_profit = float(pnl[pnl > 0].sum()) if not pnl.empty else 0.0
    gross_loss = abs(float(pnl[pnl < 0].sum())) if not pnl.empty else 0.0
    pf = gross_profit / gross_loss if gross_loss > 0 else 99.9

    health = "OK"
    message = "近期交易正常"

    if win_rate < float(PARAMS.get("LIVE_MONITOR_WIN_RATE", 0.30)):
        health = "WARN"
        message = "近期勝率偏低"
    if avg_return < float(PARAMS.get("LIVE_MONITOR_MIN_AVG_RETURN", -0.20)):
        health = "WARN"
        message = "近期平均報酬偏低"
    if pf < 0.9:
        health = "WARN"
        message = "近期 PF 偏低"

    return {
        "health": health,
        "sample_size": int(len(ret)),
        "win_rate": round(win_rate, 4),
        "avg_return": round(avg_return, 4),
        "profit_factor": round(float(pf), 4),
        "message": message,
    }


def check_strategy_layer():
    tags = ["多方進場", "空方進場", "AI訊號"]
    rows = []
    for tag in tags:
        health, note = check_strategy_health(tag)
        summary = get_strategy_summary(tag)
        rows.append({
            "strategy": tag,
            "health": health,
            "note": note,
            "sample_size": int(summary.get("sample_size", 0)),
            "win_rate": float(summary.get("win_rate", 0.0)),
            "avg_return": float(summary.get("avg_return", 0.0)),
            "profit_factor": float(summary.get("profit_factor", 0.0)),
        })
    return pd.DataFrame(rows)


def evaluate_system_guard():
    model_status = check_model_artifacts()
    desk_status = check_decision_desk()
    trade_status = check_recent_trades()
    strategy_df = check_strategy_layer()

    alerts = []
    block_builds = False

    if model_status["health"] == "FAIL":
        alerts.append("模型核心檔案不足，禁止建倉")
        block_builds = True

    if desk_status["health"] == "FAIL":
        alerts.append("決策桌失敗或空白，禁止建倉")
        block_builds = True

    if desk_status["avg_realized_ev"] <= 0:
        alerts.append("決策桌平均 EV 不為正")
        block_builds = True

    if not strategy_df.empty and (strategy_df["health"] == "KILL").any():
        killed = strategy_df.loc[strategy_df["health"] == "KILL", "strategy"].tolist()
        alerts.append(f"策略健康度 KILL：{', '.join(killed)}")
        block_builds = True

    if trade_status["health"] == "WARN" and trade_status["sample_size"] >= 20:
        alerts.append("近期交易健康度警告")

    overall = "OK"
    if block_builds:
        overall = "BLOCK"
    elif alerts:
        overall = "WARN"

    payload = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "overall": overall,
        "block_new_positions": block_builds,
        "alerts": alerts,
        "model_status": model_status,
        "decision_desk": desk_status,
        "recent_trades": trade_status,
        "strategy_layer": strategy_df.to_dict(orient="records"),
    }
    return payload, strategy_df


def save_guard_report(payload, strategy_df, output_dir="monitor_reports"):
    os.makedirs(output_dir, exist_ok=True)
    json_path = os.path.join(output_dir, "system_guard_report.json")
    csv_path = os.path.join(output_dir, "system_guard_strategy_snapshot.csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    strategy_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return json_path, csv_path


def format_alert_message(payload):
    lines = [
        "🚨【系統自我保護告警】",
        f"時間：{payload['timestamp']}",
        f"總體狀態：{payload['overall']}",
        f"是否阻止建倉：{'是' if payload['block_new_positions'] else '否'}",
        "-" * 20,
    ]
    if payload["alerts"]:
        lines.extend([f"• {msg}" for msg in payload["alerts"]])
    else:
        lines.append("• 無異常")
    return "\n".join(lines)


def run_system_guard():
    payload, strategy_df = evaluate_system_guard()
    json_path, csv_path = save_guard_report(payload, strategy_df)
    print(format_alert_message(payload))
    print(f"📁 已輸出：{json_path}")
    print(f"📁 已輸出：{csv_path}")

    alert_mgr = AlertManager(
        line_bot_token=PARAMS.get("ALERT_LINE_BOT_TOKEN", ""),
        line_user_id=PARAMS.get("ALERT_LINE_USER_ID", ""),
        is_test_mode=bool(PARAMS.get("ALERT_TEST_MODE", True)),
    )
    alert_mgr.maybe_send_guard_alert(payload)
    return payload


if __name__ == "__main__":
    run_system_guard()
