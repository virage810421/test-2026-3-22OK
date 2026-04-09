import json
import os
from datetime import datetime

import pandas as pd
import pyodbc

from config import PARAMS
from performance import check_strategy_health, get_strategy_summary

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


def load_decision_desk(path="daily_decision_desk.csv"):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def load_active_positions():
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            return pd.read_sql("SELECT * FROM active_positions", conn)
    except Exception:
        return pd.DataFrame()


def load_trade_history(limit=200):
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = f"""
                SELECT TOP {int(limit)}
                    [Ticker SYMBOL], [方向], [報酬率(%)], [淨損益金額],
                    [市場狀態], [進場陣型], [出場時間]
                FROM trade_history
                ORDER BY [出場時間] DESC
            """
            return pd.read_sql(query, conn)
    except Exception:
        return pd.DataFrame()


def check_model_artifacts():
    status = {
        "selected_features": os.path.exists("models/selected_features.pkl"),
        "bull_model": os.path.exists("models/model_趨勢多頭.pkl"),
        "side_model": os.path.exists("models/model_區間盤整.pkl"),
        "bear_model": os.path.exists("models/model_趨勢空頭.pkl"),
    }
    ok_count = sum(1 for v in status.values() if v)
    status["ok_count"] = ok_count
    status["health"] = "OK" if ok_count >= 3 else ("WARN" if ok_count >= 1 else "FAIL")
    return status


def check_decision_desk_quality(df):
    if df is None or df.empty:
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
        message = "決策桌平均 Realized_EV 不為正"
    if avg_ai < 0.50:
        health = "WARN"
        message = "決策桌平均 AI 勝率偏低"

    return {
        "health": health,
        "rows": int(len(df)),
        "avg_ai_proba": round(avg_ai, 4),
        "avg_realized_ev": round(avg_ev, 4),
        "avg_score_gap": round(avg_gap, 4),
        "message": message,
    }


def check_portfolio_exposure(active_df):
    if active_df is None or active_df.empty:
        return {
            "health": "OK",
            "position_count": 0,
            "total_invested": 0.0,
            "max_single_position": 0.0,
            "message": "目前空手",
        }

    invested = pd.to_numeric(active_df.get("投入資金", 0), errors="coerce").fillna(0.0)
    total_invested = float(invested.sum())
    max_single = float(invested.max()) if len(invested) > 0 else 0.0
    pos_count = int(len(active_df))

    max_positions = int(PARAMS.get("MAX_POSITIONS", 20))
    health = "OK"
    message = "持倉曝險正常"

    if pos_count > max_positions:
        health = "WARN"
        message = "持倉檔數超過上限"
    elif total_invested <= 0:
        health = "WARN"
        message = "持倉資金異常"

    return {
        "health": health,
        "position_count": pos_count,
        "total_invested": round(total_invested, 2),
        "max_single_position": round(max_single, 2),
        "message": message,
    }


def check_recent_trade_health(df_trades):
    if df_trades is None or df_trades.empty:
        return {
            "health": "WARN",
            "sample_size": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "profit_factor": 0.0,
            "message": "近期無成交紀錄",
        }

    returns = pd.to_numeric(df_trades["報酬率(%)"], errors="coerce").dropna()
    pnl = pd.to_numeric(df_trades["淨損益金額"], errors="coerce").dropna()

    if returns.empty:
        return {
            "health": "WARN",
            "sample_size": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "profit_factor": 0.0,
            "message": "近期報酬資料缺失",
        }

    win_rate = float((returns > 0).mean())
    avg_return = float(returns.mean())
    gross_profit = float(pnl[pnl > 0].sum()) if not pnl.empty else 0.0
    gross_loss = abs(float(pnl[pnl < 0].sum())) if not pnl.empty else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 99.9

    health = "OK"
    message = "近期戰績正常"

    if win_rate < 0.35:
        health = "WARN"
        message = "近期勝率偏低"
    if avg_return < -0.2:
        health = "WARN"
        message = "近期平均報酬偏低"
    if profit_factor < 0.9:
        health = "WARN"
        message = "近期 PF 偏弱"

    return {
        "health": health,
        "sample_size": int(len(returns)),
        "win_rate": round(win_rate, 4),
        "avg_return": round(avg_return, 4),
        "profit_factor": round(float(profit_factor), 4),
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
            "sample_size": summary.get("sample_size", 0),
            "win_rate": summary.get("win_rate", 0.0),
            "avg_return": summary.get("avg_return", 0.0),
            "profit_factor": summary.get("profit_factor", 0.0),
        })

    return pd.DataFrame(rows)


def build_monitor_report():
    decision_df = load_decision_desk()
    active_df = load_active_positions()
    trade_df = load_trade_history(limit=200)
    strategy_df = check_strategy_layer()

    report = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "model_artifacts": check_model_artifacts(),
        "decision_desk": check_decision_desk_quality(decision_df),
        "portfolio": check_portfolio_exposure(active_df),
        "recent_trades": check_recent_trade_health(trade_df),
        "strategy_layer": strategy_df.to_dict(orient="records"),
    }

    # 總結狀態
    statuses = [
        report["model_artifacts"]["health"],
        report["decision_desk"]["health"],
        report["portfolio"]["health"],
        report["recent_trades"]["health"],
    ]
    if "FAIL" in statuses:
        report["overall_health"] = "FAIL"
    elif "WARN" in statuses:
        report["overall_health"] = "WARN"
    else:
        report["overall_health"] = "OK"

    return report, strategy_df


def save_monitor_outputs(report, strategy_df, output_dir="monitor_reports"):
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(output_dir, "system_monitor_report.json")
    csv_path = os.path.join(output_dir, "strategy_health_snapshot.csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    strategy_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    return json_path, csv_path


def print_human_summary(report, strategy_df):
    print("=" * 70)
    print("🩺 系統監控 / 告警中心")
    print("=" * 70)
    print(f"總體健康度: {report['overall_health']}")
    print(f"時間: {report['timestamp']}")
    print("-" * 70)

    ma = report["model_artifacts"]
    print(f"模型檔案: {ma['health']} | 已就緒 {ma['ok_count']}/4")

    dd = report["decision_desk"]
    print(
        f"決策桌: {dd['health']} | rows={dd['rows']} | "
        f"avgAI={dd['avg_ai_proba']:.2%} | avgEV={dd['avg_realized_ev']:.3f} | {dd['message']}"
    )

    pf = report["portfolio"]
    print(
        f"持倉: {pf['health']} | 檔數={pf['position_count']} | "
        f"總投入=${pf['total_invested']:,.0f} | 最大單筆=${pf['max_single_position']:,.0f} | {pf['message']}"
    )

    rt = report["recent_trades"]
    print(
        f"近期戰績: {rt['health']} | 筆數={rt['sample_size']} | "
        f"勝率={rt['win_rate']:.2%} | avgRet={rt['avg_return']:.3f}% | PF={rt['profit_factor']:.2f} | {rt['message']}"
    )

    print("-" * 70)
    if strategy_df is not None and not strategy_df.empty:
        print("策略健康快照:")
        print(strategy_df.to_string(index=False))
    print("=" * 70)


def run_monitor_center():
    report, strategy_df = build_monitor_report()
    json_path, csv_path = save_monitor_outputs(report, strategy_df)
    print_human_summary(report, strategy_df)
    print(f"📁 已輸出：{json_path}")
    print(f"📁 已輸出：{csv_path}")


if __name__ == "__main__":
    run_monitor_center()
