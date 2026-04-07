import json
import os
from datetime import datetime

import pandas as pd
import pyodbc

from config import PARAMS
from performance import check_strategy_health, get_strategy_summary
from alert_manager import AlertManager

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)


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
        FROM trade_history
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
