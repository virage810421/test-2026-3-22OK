import json
import logging
import os
import subprocess
import time
from datetime import datetime

import joblib
import pandas as pd

try:
    from fts_runtime_diagnostics import record_issue, write_summary as write_runtime_diagnostics_summary
except Exception:  # pragma: no cover
    def record_issue(*args, **kwargs):
        return {}
    def write_runtime_diagnostics_summary(*args, **kwargs):
        return None
import pyodbc
import yfinance as yf

from performance import check_strategy_health, get_strategy_ev
from fundamental_screener import get_vip_stock_pool
from fts_service_api import add_chip_data, extract_ai_features, inspect_stock, normalize_ticker_symbol, smart_download
from portfolio_risk import apply_portfolio_risk
from system_guard import run_system_guard
from fts_sql_table_name_map import sql_table
from fts_etl_daily_chip_service import main_scheduler as run_daily_chip_mainline
from fts_training_data_builder import get_dynamic_watchlist as get_training_universe, generate_ml_dataset
from fts_trainer_backend import train_models
try:
    from fts_model_layer import evaluate_exit_signal
except Exception:
    evaluate_exit_signal = None

TABLE_TRADE_HISTORY = sql_table('trade_history')
TABLE_DAILY_CHIP = sql_table('daily_chip_data')

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def log(msg):
    print(msg)
    logging.info(msg)


def run_script(script_name, retries=2, timeout=900):
    if not os.path.exists(script_name):
        log(f"⚠️ 找不到 {script_name}，跳過")
        return False

    custom_env = os.environ.copy()
    custom_env["PYTHONIOENCODING"] = "utf-8"

    for attempt in range(retries + 1):
        log(f"🚀 執行 {script_name}（第 {attempt + 1} 次）")
        start = time.time()
        try:
            result = subprocess.run(
                ["python", script_name],
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout,
                encoding="utf-8",
                env=custom_env,
            )
            if result.stdout:
                log(result.stdout)
            if result.stderr:
                log(result.stderr)
            elapsed = time.time() - start
            log(f"✅ 完成 {script_name}（{elapsed:.1f}s）")
            return True
        except subprocess.TimeoutExpired:
            log(f"⏰ Timeout: {script_name} 執行超時！")
        except subprocess.CalledProcessError as e:
            log(f"❌ 錯誤: {script_name} 執行失敗！")
            if e.stderr:
                log(e.stderr)
        time.sleep(2)
    return False


def run_daily_chip_service() -> bool:
    """舊 daily_chip_etl.py 門牌已退役；直接呼叫法人籌碼主線服務。"""
    try:
        result = run_daily_chip_mainline()
        return result is not False
    except Exception as exc:
        log(f"❌ 法人籌碼主線服務失敗：{exc}")
        return False


def run_training_data_builder() -> bool:
    """舊 ml_data_generator.py 門牌已退役；直接產生 ml_training_data.csv。"""
    try:
        tickers = get_training_universe()
        df = generate_ml_dataset(tickers)
        return df is not None
    except Exception as exc:
        log(f"❌ 訓練資料主線服務失敗：{exc}")
        return False


def run_trainer_backend() -> bool:
    """舊 ml_trainer.py 門牌已退役；直接呼叫 trainer backend。"""
    try:
        path, payload = train_models()
        log(f"🧠 trainer backend report: {path} | status={payload.get('status') if isinstance(payload, dict) else 'unknown'}")
        return True
    except Exception as exc:
        log(f"❌ 模型訓練主線服務失敗：{exc}")
        return False


def validate_outputs():
    if not os.path.exists("data/ml_training_data.csv"):
        log("❌ 驗證失敗：特徵訓練教材 (ml_training_data.csv) 未產出！")
        return False

    os.makedirs("models", exist_ok=True)
    models_found = [f for f in os.listdir("models") if f.startswith("model_") and f.endswith(".pkl")]
    if not models_found:
        log("❌ 驗證失敗：精神時光屋未能鍛造出任何 AI 大腦！")
        return False

    log(f"✅ 驗證通過：兵工廠成功掛載 {len(models_found)} 顆 AI 大腦！")
    return True


def generate_report(start_time, status):
    report = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "duration_sec": round(time.time() - start_time, 2),
        "status": status,
    }
    with open("daily_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)
    log("📊 已生成系統日誌 daily_report.json")


def get_system_performance():
    try:
        DB_CONN_STR = (
            r"DRIVER={ODBC Driver 17 for SQL Server};"
            r"SERVER=localhost;"
            r"DATABASE=股票online;"
            r"Trusted_Connection=yes;"
        )
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = f"SELECT TOP 100 [報酬率(%)], [淨損益金額], [結餘本金] FROM {TABLE_TRADE_HISTORY} ORDER BY [出場時間] DESC"
            df_stats = pd.read_sql(query, conn)

            if not df_stats.empty:
                df_stats = df_stats.iloc[::-1].reset_index(drop=True)

                total_trades = len(df_stats)
                wins = len(df_stats[df_stats["報酬率(%)"] > 0])
                winrate = wins / total_trades if total_trades > 0 else 0

                gross_profit = df_stats[df_stats["淨損益金額"] > 0]["淨損益金額"].sum()
                gross_loss = abs(df_stats[df_stats["淨損益金額"] < 0]["淨損益金額"].sum())
                profit_factor = gross_profit / gross_loss if gross_loss != 0 else 99.9

                if "結餘本金" in df_stats.columns and df_stats["結餘本金"].notna().any():
                    df_stats["Peak"] = df_stats["結餘本金"].cummax()
                    df_stats["Drawdown"] = (df_stats["結餘本金"] - df_stats["Peak"]) / df_stats["Peak"]
                    mdd = abs(df_stats["Drawdown"].min())
                else:
                    mdd = 0.0

                log(f"📊 [系統戰績] 近 {total_trades} 筆 | 勝率: {winrate:.1%} | 獲利因子(PF): {profit_factor:.2f} | 最大回撤(MDD): {mdd:.1%}")
                return winrate, profit_factor, mdd
    except Exception as e:
        log(f"⚠️ 無法連線 SQL 讀取歷史戰績，預設回傳穩態值 ({e})")

    return 0.5, 1.0, 0.0


def should_retrain():
    today = datetime.now().weekday()

    if today == 6:
        log("🗓️ 系統判定：今日為週日，啟動【例行性 AI 大腦重塑】！")
        return True

    recent_winrate, recent_pf, recent_mdd = get_system_performance()

    if recent_winrate < 0.4:
        log(f"🚨 系統警告：近期勝率跌至 {recent_winrate:.1%} (低於 40%)！啟動【緊急重訓】！")
        return True
    if recent_mdd > 0.15:
        log(f"🚨 系統警告：近期資金最大回撤達 {recent_mdd:.1%} (破 15%)！啟動【防禦性重訓】！")
        return True
    if recent_pf < 0.8 and recent_pf != 0:
        log(f"⚠️ 系統警告：獲利因子降至 {recent_pf:.2f}！啟動【校正性重訓】！")
        return True

    return False


def get_dynamic_watchlist():
    log("📡 啟動動態索敵雷達：正在從 SQL 資料庫獲取監控名單...")
    try:
        DB_CONN_STR = (
            r"DRIVER={ODBC Driver 17 for SQL Server};"
            r"SERVER=localhost;"
            r"DATABASE=股票online;"
            r"Trusted_Connection=yes;"
        )
        with pyodbc.connect(DB_CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT DISTINCT [Ticker SYMBOL] FROM {TABLE_DAILY_CHIP}")
            rows = cursor.fetchall()
            dynamic_list = [normalize_ticker_symbol(row[0]) for row in rows if row[0]]

            if dynamic_list:
                log(f"✅ 成功鎖定 {len(dynamic_list)} 檔目標進行掃描！")
                return dynamic_list
            raise ValueError("資料庫中找不到任何股票代碼！請檢查資料表是否為空。")
    except Exception as e:
        log(f"🛑 致命錯誤：無法連線 SQL 或獲取動態名單！詳細原因: {e}")
        raise


def build_final_watchlist():
    raw_watch_list = get_dynamic_watchlist()
    vip_pool = get_vip_stock_pool()

    raw_watch_list = [normalize_ticker_symbol(t) for t in raw_watch_list]
    vip_pool = [normalize_ticker_symbol(t) for t in vip_pool]

    final_watch = [t for t in raw_watch_list if t in set(vip_pool)]

    if final_watch:
        log(f"✅ 雙引擎交集完成：原始監控 {len(raw_watch_list)} 檔 | 基本面精選 {len(vip_pool)} 檔 | 最終清單 {len(final_watch)} 檔")
        return final_watch

    fallback = vip_pool if vip_pool else raw_watch_list
    log(f"⚠️ 無法取得交集，啟用回退名單：{len(fallback)} 檔")
    return fallback


def load_market_data(watch_list):
    log(f"📡 戰情中心：正在同步 {len(watch_list)} 檔標的資料 (⚡啟用智慧快取)...")
    data_dict = {}
    for ticker in watch_list:
        ticker = normalize_ticker_symbol(ticker)
        df = smart_download(ticker, period="2y")
        if df.empty:
            log(f"⚠️ {ticker} 無 K 線資料，跳過")
            continue
        df = add_chip_data(df, ticker)
        data_dict[ticker] = df
    log(f"✅ 已完成 {len(data_dict)} 檔資料載入")
    return data_dict


def analyze_market_climate():
    try:
        twii = yf.download("^TWII", period="6mo", progress=False, auto_adjust=False)
        if twii.empty:
            return "未知", 1.0

        if isinstance(twii.columns, pd.MultiIndex):
            twii = twii.xs("^TWII", axis=1, level=1).copy()

        twii["MA20"] = twii["Close"].rolling(20).mean()
        twii["MA60"] = twii["Close"].rolling(60).mean()

        latest = twii.iloc[-1]
        climate = "中性"
        risk_multiplier = 1.0

        if latest["Close"] > latest["MA20"] > latest["MA60"]:
            climate = "多頭順風"
            risk_multiplier = 1.0
            log("🌤️ 大盤氣候判定：多頭順風，允許標準火力。")
        elif latest["Close"] < latest["MA20"] < latest["MA60"]:
            climate = "空頭逆風"
            risk_multiplier = 0.5
            log("⛈️ 大盤氣候判定：空頭逆風，系統自動降載 50%。")
        else:
            climate = "震盪盤"
            risk_multiplier = 0.75
            log("🌫️ 大盤氣候判定：震盪盤，系統保守降載 25%。")

        return climate, risk_multiplier
    except Exception as e:
        log(f"⚠️ 大盤探測發生異常: {e}")
        return "未知", 1.0


def _load_models():
    ai_models = {}
    selected_features = []

    if os.path.exists("models/selected_features.pkl"):
        try:
            selected_features = joblib.load("models/selected_features.pkl")
        except Exception as e:
            log(f"⚠️ 特徵檔載入失敗：{e}")

    for regime in ["趨勢多頭", "區間盤整", "趨勢空頭"]:
        model_path = f"models/model_{regime}.pkl"
        if os.path.exists(model_path):
            try:
                ai_models[regime] = joblib.load(model_path)
            except Exception as e:
                log(f"⚠️ {regime} 模型載入失敗：{e}")

    return ai_models, selected_features


def _predict_ai_proba(latest_row, regime, hist_win, ai_models, selected_features, ticker):
    model = ai_models.get(regime)
    proba = hist_win

    if model is not None:
        try:
            features_dict = extract_ai_features(latest_row)
            if selected_features:
                X_input = pd.DataFrame([{f: features_dict.get(f, 0) for f in selected_features}])
            else:
                X_input = pd.DataFrame([features_dict])
            proba = float(model.predict_proba(X_input)[0][1])
        except Exception as e:
            log(f"⚠️ {ticker} AI 預測失敗，改用歷史勝率: {e}")
            proba = hist_win

    return max(0.01, min(0.99, float(proba)))


def build_decision_desk(watch_list, market_data, global_risk_multiplier):
    ai_models, selected_features = _load_models()
    rows = []

    for ticker in watch_list:
        if ticker not in market_data:
            continue

        df = market_data[ticker]
        result = inspect_stock(ticker, preloaded_df=df)
        if not result or "計算後資料" not in result:
            continue

        latest_row = result["計算後資料"].iloc[-1]
        regime = result.get("Regime", latest_row.get("Regime", "區間盤整"))
        entry_state = str(result.get('Entry_State', 'NO_ENTRY')).upper()
        setup_tag = result.get('Golden_Type', '無')
        if entry_state == 'NO_ENTRY' or setup_tag == '無':
            continue

        realized_ev = float(result.get("期望值", 0.0))
        hist_win = float(result.get("系統勝率(%)", 50.0)) / 100.0
        signal_conf = float(result.get("訊號信心分數(%)", 50.0)) / 100.0
        sample_size = int(result.get("歷史訊號樣本數", 0))
        kelly_signal = float(result.get('StateMachine_Kelly_Pos', result.get('Kelly建議倉位', 0.0)))

        weighted_buy = float(result.get("Weighted_Buy_Score", latest_row.get("Weighted_Buy_Score", 0.0)))
        weighted_sell = float(result.get("Weighted_Sell_Score", latest_row.get("Weighted_Sell_Score", 0.0)))
        score_gap = float(result.get("Score_Gap", latest_row.get("Score_Gap", 0.0)))

        ai_proba = _predict_ai_proba(latest_row, regime, hist_win, ai_models, selected_features, ticker)
        direction_lane = str(result.get('StateMachine_Direction', 'LONG')).upper()
        action = '做多(Long)' if direction_lane == 'LONG' else ('做空(Short)' if direction_lane == 'SHORT' else '區間(Range)')

        health_status, health_note = check_strategy_health(setup_tag)
        realized_strategy_ev = float(get_strategy_ev(setup_tag, regime))

        sample_boost = min(1.0, sample_size / 20.0)
        ev_norm = max(-0.05, min(0.10, realized_ev / 100.0))
        strategy_ev_norm = max(-0.05, min(0.10, realized_strategy_ev / 100.0))
        legacy_influence = float(PARAMS.get("LEGACY_CONFIRM_INFLUENCE", 0.0))
        gap_norm = max(-0.20, min(0.20, score_gap / 10.0)) * legacy_influence

        base_score = (
            ai_proba * 0.38 +
            hist_win * 0.16 +
            signal_conf * 0.14 +
            max(0.0, ev_norm) * 1.10 +
            max(0.0, strategy_ev_norm) * 0.90 +
            max(0.0, gap_norm) * 0.70 +
            sample_boost * 0.05
        )

        if realized_ev < 0:
            base_score -= 0.08
        if realized_strategy_ev < 0:
            base_score -= 0.05

        base_kelly = max(0.0, min(0.20, kelly_signal))
        if ai_proba < 0.50 or realized_ev <= 0:
            base_kelly *= 0.25
        if sample_size < 8:
            base_kelly *= 0.5
        if legacy_influence > 0 and score_gap <= 0:
            base_kelly *= 0.5

        final_kelly = 0.0 if health_status == "KILL" else round(base_kelly * global_risk_multiplier, 4)

        exit_decision = None
        if evaluate_exit_signal is not None:
            try:
                exit_decision = evaluate_exit_signal(latest_row)
            except Exception:
                exit_decision = None
        exit_state = str(getattr(exit_decision, 'exit_state', result.get('Exit_State', 'HOLD'))).upper() if exit_decision is not None else str(result.get('Exit_State', 'HOLD')).upper()
        exit_action = str(getattr(exit_decision, 'exit_action', result.get('Exit_Action', 'HOLD'))) if exit_decision is not None else str(result.get('Exit_Action', 'HOLD'))
        exit_defend_p = float(getattr(exit_decision, 'defend_proba', result.get('Exit_Defend_Proba', 0.0))) if exit_decision is not None else float(result.get('Exit_Defend_Proba', 0.0))
        exit_reduce_p = float(getattr(exit_decision, 'reduce_proba', result.get('Exit_Reduce_Proba', 0.0))) if exit_decision is not None else float(result.get('Exit_Reduce_Proba', 0.0))
        exit_confirm_p = float(getattr(exit_decision, 'confirm_proba', result.get('Exit_Confirm_Proba', 0.0))) if exit_decision is not None else float(result.get('Exit_Confirm_Proba', 0.0))
        exit_model_source = str(getattr(exit_decision, 'model_source', 'none')) if exit_decision is not None else 'none'
        target_mult = float(getattr(exit_decision, 'target_position_multiplier', result.get('Target_Position_Multiplier', 1.0))) if exit_decision is not None else float(result.get('Target_Position_Multiplier', 1.0))
        stop_mult = float(getattr(exit_decision, 'stop_tighten_multiplier', result.get('Stop_Tighten_Multiplier', 1.0))) if exit_decision is not None else float(result.get('Stop_Tighten_Multiplier', 1.0))

        rows.append({
            "Ticker": ticker,
            "Direction": action,
            "Regime": regime,
            "Structure": setup_tag,
            "AI_Proba": round(ai_proba, 4),
            "Hist_WinRate": round(hist_win, 4),
            "Realized_EV": round(realized_ev, 4),
            "Strategy_EV_SQL": round(realized_strategy_ev, 4),
            "Signal_Confidence": round(signal_conf, 4),
            "Sample_Size": sample_size,
            "Buy_Score": int(result.get("Buy_Score", 0)),
            "Sell_Score": int(result.get("Sell_Score", 0)),
            "Weighted_Buy_Score": round(weighted_buy, 3),
            "Weighted_Sell_Score": round(weighted_sell, 3),
            "Score_Gap": round(score_gap, 3),
            "Kelly_Pos": final_kelly,
            "Score": round(base_score, 4),
            "Risk": health_note,
            "Health": health_status,
            "Entry_State": entry_state,
            "Early_Path_State": result.get('Early_Path_State', entry_state),
            "Confirm_Path_State": result.get('Confirm_Path_State', 'WAIT_CONFIRM'),
            "Entry_Path": result.get('Entry_Path', 'NONE'),
            "PreEntry_Score": round(float(result.get('PreEntry_Score', 0.0)), 4),
            "Confirm_Entry_Score": round(float(result.get('Confirm_Entry_Score', 0.0)), 4),
            "StateMachine_Direction": direction_lane,
            "StateMachine_Kelly_Pos": round(float(result.get('StateMachine_Kelly_Pos', final_kelly)), 4),
            "Legacy_Golden_Type": result.get('Golden_Type_Legacy', ''),
            "Legacy_Confirm_Influence": float(result.get('Legacy_Confirm_Influence', PARAMS.get('LEGACY_CONFIRM_INFLUENCE', 0.0))),
            "Legacy_Score_Alert_Only": int(bool(result.get('Legacy_Score_Alert_Only', PARAMS.get('LEGACY_SCORE_ALERT_ONLY', True)))),
            "Legacy_Long_Confirm_Pressure": round(float(result.get('Legacy_Long_Confirm_Pressure', 0.0)), 4),
            "Legacy_Short_Confirm_Pressure": round(float(result.get('Legacy_Short_Confirm_Pressure', 0.0)), 4),
            "Legacy_Range_Confirm_Pressure": round(float(result.get('Legacy_Range_Confirm_Pressure', 0.0)), 4),
            "Exit_State": exit_state,
            "Exit_Action": exit_action,
            "Exit_Model_Source": exit_model_source,
            "Exit_Defend_Proba": round(exit_defend_p, 4),
            "Exit_Reduce_Proba": round(exit_reduce_p, 4),
            "Exit_Confirm_Proba": round(exit_confirm_p, 4),
            "Target_Position_Multiplier": round(target_mult, 4),
            "Stop_Tighten_Multiplier": round(stop_mult, 4),
        })

    df_report = pd.DataFrame(rows)
    if df_report.empty:
        df_report.to_csv("daily_decision_desk.csv", index=False, encoding="utf-8-sig")
        return df_report

    df_report.sort_values(
        ["Kelly_Pos", "Score", "Score_Gap", "AI_Proba", "Hist_WinRate", "Sample_Size"],
        ascending=False,
        inplace=True
    )
    pre_risk = df_report.copy()
    df_report = apply_portfolio_risk(df_report)
    pre_risk.to_csv("daily_decision_desk_prerisk.csv", index=False, encoding="utf-8-sig")
    df_report.to_csv("daily_decision_desk.csv", index=False, encoding="utf-8-sig")
    return df_report


def main():
    start_time = time.time()
    is_weekend = datetime.now().weekday() >= 5

    log("\n" + "=" * 60)
    log("⚙️ HFA 全自動研究與訓練管線 (Self-Guarded) 啟動")
    log("=" * 60)

    if is_weekend:
        log("\n⏳ 階段：資料更新 (⚠️ 今日為週末休市，跳過 API 爬蟲)")
    else:
        log("\n⏳ 階段：資料更新")
        if not run_daily_chip_service():
            log("🛑 資料庫更新失敗，強制中斷！")
            generate_report(start_time, "FAILED")
            return

    if should_retrain():
        log("\n🧠 階段：重訓 AI 模型")
        for runner, desc in [(run_training_data_builder, "特徵生成"), (run_trainer_backend, "模型訓練")]:
            log(f"\n⏳ 階段：{desc}")
            if not runner():
                log("🛑 AI 訓練異常，管線中斷！")
                generate_report(start_time, "FAILED")
                return
        if not validate_outputs():
            log("🛑 模型驗證失敗")
            generate_report(start_time, "INVALID")
            return
    else:
        log("\n⚡ 今日跳過 AI 重訓，使用現役模型")

    climate_status, global_risk_multiplier = analyze_market_climate()
    log(f"🌍 市場氣候：{climate_status} | 風險倍率：{global_risk_multiplier:.2f}")

    try:
        watch_list = build_final_watchlist()
    except Exception:
        generate_report(start_time, "FAILED")
        return

    market_data = load_market_data(watch_list)
    df_report = build_decision_desk(watch_list, market_data, global_risk_multiplier)

    if df_report.empty:
        log("⚠️ 今日沒有合格標的產出。")
        generate_report(start_time, "SUCCESS")
        return

    # 關鍵升級：生成決策桌後立刻做系統自我保護檢查
    guard_payload = run_system_guard()
    overall = guard_payload.get("overall", "OK")
    block_builds = bool(guard_payload.get("block_new_positions", False))

    log(f"🩺 系統守門員狀態：{overall}")
    for alert in guard_payload.get("alerts", []):
        log(f"🚨 告警：{alert}")

    TOTAL_CAPITAL = 10_000_000
    for _, row in df_report.head(20).iterrows():
        final_allocation_pct = row["Kelly_Pos"]
        target_amount = TOTAL_CAPITAL * final_allocation_pct
        log(f"🎯 標的: {row['Ticker']}")
        log(
            f"   ► AI勝率: {row['AI_Proba']:.1%} | 歷史勝率: {row['Hist_WinRate']:.1%} | "
            f"RealizedEV: {row['Realized_EV']:.3f} | 樣本數: {int(row['Sample_Size'])}"
        )
        log(
            f"   ► 加權燈號: WB={row['Weighted_Buy_Score']:.2f} | "
            f"WS={row['Weighted_Sell_Score']:.2f} | Gap={row['Score_Gap']:.2f} | 綜合分: {row['Score']:.3f}"
        )
        if final_allocation_pct > 0:
            log(f"   💰 建議配置總資金的 {final_allocation_pct:.1%} (約 ${target_amount:,.0f})")
        else:
            log("   💰 建議空手")

    generate_report(start_time, "SUCCESS")

    if not is_weekend:
        if block_builds:
            log("🛡️ 系統守門員已阻止當日建倉，略過 live_paper_trading.py")
        else:
            log("\n⏳ 階段：呼叫自動下單機進行帳戶結算與模擬建倉...")
            run_script("live_paper_trading.py")


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    main()
