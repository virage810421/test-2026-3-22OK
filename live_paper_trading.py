import warnings

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

from datetime import datetime
import requests
import pandas as pd
import pyodbc

from config import PARAMS
from screening import smart_download, apply_slippage, calculate_pnl, inspect_stock, add_chip_data
from strategies import get_active_strategy
from sector_classifier import get_stock_sector
try:
    from fts_level3_runtime_loader import build_level3_services
    _LEVEL3_SERVICES, _LEVEL3_META = build_level3_services()
except Exception:
    _LEVEL3_SERVICES, _LEVEL3_META = ({}, {'status': 'level3_unavailable'})


# ==========================================
# ⚙️ 系統環境設定
# ==========================================
IS_TEST_MODE = True

LINE_BOT_TOKEN = ""
LINE_USER_ID = ""

DB_CONN_STR = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost;"
    r"DATABASE=股票online;"
    r"Trusted_Connection=yes;"
)


def send_line_bot_msg(msg: str):
    if IS_TEST_MODE:
        print("\n🔇 [測試模式已開啟] 攔截 LINE 發送請求。")
        print("-" * 30)
        print(f"📝 原本會發送至手機的內容為：\n{msg}")
        print("-" * 30)
        return

    if not LINE_BOT_TOKEN or not LINE_USER_ID:
        print("⚠️ LINE_BOT_TOKEN 或 LINE_USER_ID 尚未設定，略過推播。")
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_BOT_TOKEN}",
    }
    data = {
        "to": LINE_USER_ID,
        "messages": [{"type": "text", "text": msg}],
    }

    try:
        response = requests.post(url, headers=headers, json=data, timeout=20)
        if response.status_code == 200:
            print("📲 LINE 戰情報告已成功發送至您的手機！")
        else:
            print(f"⚠️ LINE 官方拒絕了發送！狀態碼: {response.status_code}")
            print(f"🔍 錯誤詳情: {response.text}")
    except Exception as e:
        print(f"⚠️ LINE 網路發送發生例外錯誤: {e}")


def get_available_cash(cursor) -> float:
    try:
        cursor.execute("SELECT [可用現金] FROM account_info WHERE [帳戶名稱] = N'我的實戰帳戶'")
        row = cursor.fetchone()
        if row:
            return float(row[0])
    except Exception:
        pass

    try:
        cursor.execute(
            """
            INSERT INTO account_info ([帳戶名稱], [可用現金], [最後更新時間])
            VALUES (N'我的實戰帳戶', 5000000, ?)
            """,
            (datetime.now(),),
        )
        cursor.connection.commit()
    except Exception:
        pass

    return 5000000.0


def _safe_float(x, default=0.0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def _safe_int(x, default=0):
    try:
        if pd.isna(x):
            return default
        return int(float(x))
    except Exception:
        return default


def _direction_bucket(direction_text: str) -> str:
    s = str(direction_text)
    return "SHORT" if ("空" in s or "Short" in s) else "LONG"


def _read_active_positions(conn):
    try:
        return pd.read_sql("SELECT * FROM active_positions", conn)
    except Exception:
        return pd.DataFrame()


def _current_portfolio_state(active_df, total_nav):
    """
    從目前持倉估算組合曝險狀態，給最終下單審核用。
    """
    service = _LEVEL3_SERVICES.get("PositionStateService")
    if service is not None:
        try:
            return service.current_portfolio_state(active_df, total_nav)
        except Exception:
            pass
    state = {
        "total_alloc": 0.0,
        "sector_alloc": {},
        "sector_count": {},
        "direction_alloc": {"LONG": 0.0, "SHORT": 0.0},
    }

    if active_df is None or active_df.empty or total_nav <= 0:
        return state

    for _, pos in active_df.iterrows():
        ticker = str(pos.get("Ticker SYMBOL", "")).strip()
        invested = _safe_float(pos.get("投入資金", 0.0), 0.0)
        if invested <= 0:
            continue

        alloc = invested / total_nav
        direction = _direction_bucket(pos.get("方向", ""))
        sector = get_stock_sector(ticker)

        state["total_alloc"] += alloc
        state["sector_alloc"][sector] = state["sector_alloc"].get(sector, 0.0) + alloc
        state["sector_count"][sector] = state["sector_count"].get(sector, 0) + 1
        state["direction_alloc"][direction] = state["direction_alloc"].get(direction, 0.0) + alloc

    return state


def _build_entry_metrics(row):
    structure = row.get("Structure", "AI訊號")
    regime = row.get("Regime", "未知")
    realized_ev = _safe_float(row.get("Realized_EV", 0.0), 0.0)
    sample_size = _safe_int(row.get("Sample_Size", 0), 0)
    ai_proba = _safe_float(row.get("AI_Proba", 0.5), 0.5)
    weighted_buy = _safe_float(row.get("Weighted_Buy_Score", 0.0), 0.0)
    weighted_sell = _safe_float(row.get("Weighted_Sell_Score", 0.0), 0.0)
    score_gap = _safe_float(row.get("Score_Gap", 0.0), 0.0)

    try:
        dummy_vol = 0.05
        trend_is_with_me = "多頭" in str(regime)
        adx_is_strong = ai_proba >= 0.55
        active_strategy = get_active_strategy(structure)
        dynamic_sl, dynamic_tp, _ = active_strategy.get_exit_rules(
            PARAMS, dummy_vol, trend_is_with_me, adx_is_strong, 0
        )
    except Exception:
        dynamic_sl = float(PARAMS.get("SL_MIN_PCT", 0.03))
        dynamic_tp = float(PARAMS.get("TP_BASE_PCT", 0.10))

    rr_ratio = (dynamic_tp / dynamic_sl) if dynamic_sl > 0 else 0.0

    risk_budget_ratio = 0.05
    if sample_size < 8:
        risk_budget_ratio = 0.03
    if realized_ev <= 0 or ai_proba < 0.5:
        risk_budget_ratio = min(risk_budget_ratio, 0.02)
    if score_gap <= 0:
        risk_budget_ratio = min(risk_budget_ratio, 0.015)

    return {
        "市場狀態": regime,
        "進場陣型": structure,
        "期望值": realized_ev,
        "預期停損(%)": round(dynamic_sl * 100, 3),
        "預期停利(%)": round(dynamic_tp * 100, 3),
        "風報比(RR)": round(rr_ratio, 3),
        "風險金額比率": risk_budget_ratio,
        "Weighted_Buy_Score": weighted_buy,
        "Weighted_Sell_Score": weighted_sell,
        "Score_Gap": score_gap,
    }


def _passes_signal_gate(row):
    kelly_pct = _safe_float(row.get("Kelly_Pos", 0.0), 0.0)
    weighted_buy = _safe_float(row.get("Weighted_Buy_Score", 0.0), 0.0)
    weighted_sell = _safe_float(row.get("Weighted_Sell_Score", 0.0), 0.0)
    score_gap = _safe_float(row.get("Score_Gap", 0.0), 0.0)
    ai_proba = _safe_float(row.get("AI_Proba", 0.0), 0.0)
    realized_ev = _safe_float(row.get("Realized_EV", 0.0), 0.0)
    health = str(row.get("Health", "KEEP")).upper()

    if kelly_pct <= 0:
        return False, "Kelly 倉位為 0"
    if health == "KILL":
        return False, "策略健康度阻斷"
    if realized_ev <= 0:
        return False, "Realized EV <= 0"
    if ai_proba < 0.50:
        return False, "AI 勝率不足"
    if score_gap <= 0:
        return False, "加權分數差為負"
    if weighted_buy < max(2.0, float(PARAMS.get("TRIGGER_SCORE", 2))):
        return False, "多方加權分數不足"
    if weighted_sell >= weighted_buy:
        return False, "空方壓力未解除"

    return True, "通過訊號閘門"


def _passes_portfolio_gate(row, total_nav, portfolio_state):
    """
    最終下單審核層：
    依當下 active_positions / 現金 / 產業集中度重新檢查。
    """
    if total_nav <= 0:
        return False, "總資產異常"

    direction = _direction_bucket(row.get("Direction", ""))
    ticker = str(row.get("Ticker", "")).strip()
    sector = get_stock_sector(ticker)

    requested_alloc = _safe_float(row.get("Kelly_Pos", 0.0), 0.0)

    max_sector_positions = int(PARAMS.get("PORT_MAX_SECTOR_POSITIONS", 2))
    max_sector_alloc = float(PARAMS.get("PORT_MAX_SECTOR_ALLOC", 0.35))
    max_total_alloc = float(PARAMS.get("PORT_MAX_TOTAL_ALLOC", 0.60))
    max_direction_alloc = float(PARAMS.get("PORT_MAX_DIRECTION_ALLOC", 0.45))
    max_single_pos = float(PARAMS.get("PORT_MAX_SINGLE_POS", 0.12))
    min_position = float(PARAMS.get("PORT_MIN_POSITION", 0.01))

    if requested_alloc < min_position:
        return False, "倉位低於最小門檻"

    if requested_alloc > max_single_pos:
        return False, "單筆倉位超過上限"

    current_total = portfolio_state["total_alloc"]
    current_sector_alloc = portfolio_state["sector_alloc"].get(sector, 0.0)
    current_sector_count = portfolio_state["sector_count"].get(sector, 0)
    current_direction_alloc = portfolio_state["direction_alloc"].get(direction, 0.0)

    if current_sector_count >= max_sector_positions:
        return False, f"{sector} 產業持倉數已達上限"

    if current_total + requested_alloc > max_total_alloc:
        return False, "總配置上限不足"

    if current_sector_alloc + requested_alloc > max_sector_alloc:
        return False, f"{sector} 產業資金占比將超限"

    if current_direction_alloc + requested_alloc > max_direction_alloc:
        return False, f"{direction} 方向曝險將超限"

    return True, "通過組合閘門"


def run_eod_broker():
    print("\n" + "=" * 60)
    print("🤖 [自動下單中心] 啟動盤後結算與執行程序（最終審核版）...")
    print("=" * 60)

    try:
        conn = pyodbc.connect(DB_CONN_STR)
        cursor = conn.cursor()
    except Exception as e:
        print(f"🛑 無法連線 SQL 資料庫: {e}")
        return

    try:
        gate = _LEVEL3_SERVICES.get("LiveReadinessGate")
        if gate is not None:
            gate.evaluate(None)
    except Exception:
        pass

    current_cash = get_available_cash(cursor)
    active_df = _read_active_positions(conn)

    stock_value = _safe_float(active_df["投入資金"].sum(), 0.0) if not active_df.empty and "投入資金" in active_df.columns else 0.0
    daily_log = []

    # ==========================================
    # 🛡️ 階段一：掃描持倉
    # ==========================================
    if not active_df.empty:
        for _, pos in active_df.iterrows():
            ticker = pos["Ticker SYMBOL"]
            direction = pos["方向"]
            entry_price = float(pos["進場價"])
            shares = int(pos["進場股數"])
            setup_tag = pos.get("進場陣型", "傳統訊號")

            raw_tp = pos.get("停利階段", 0)
            tp_stage = int(raw_tp) if pd.notna(raw_tp) else 0

            is_long = ("Long" in str(direction)) or ("多" in str(direction))

            df = smart_download(ticker, period="3mo")
            if df.empty:
                continue

            df = add_chip_data(df, ticker)

            result = inspect_stock(ticker, preloaded_df=df, p=PARAMS)
            if not result or "計算後資料" not in result:
                continue

            computed_df = result["計算後資料"]
            latest_row = computed_df.iloc[-1]
            curr_price = float(latest_row["Close"])

            volatility_pct = (latest_row.get("BB_std", 0) * 1.5) / curr_price if curr_price > 0 else 0.05
            trend_is_with_me = (
                (is_long and latest_row["Close"] > latest_row.get("BBI", 0))
                or ((not is_long) and latest_row["Close"] < latest_row.get("BBI", 0))
            )
            adx_is_strong = latest_row.get("ADX14", 0) > PARAMS.get("ADX_TREND_THRESHOLD", 20)

            active_strategy = get_active_strategy(setup_tag)
            dynamic_sl, dynamic_tp, ignore_tp = active_strategy.get_exit_rules(
                PARAMS, volatility_pct, trend_is_with_me, adx_is_strong, 0
            )

            entry_dt = pd.to_datetime(pos["進場時間"]).normalize()
            post_entry_df = df.loc[df.index >= entry_dt].copy()
            if post_entry_df.empty:
                post_entry_df = df.copy()

            if is_long:
                max_price = post_entry_df["High"].max() if not post_entry_df.empty else curr_price
                final_stop = max(entry_price * (1 - dynamic_sl), max_price * (1 - dynamic_sl))
                tp_stage_1 = entry_price * (1 + dynamic_tp * 0.5)
                tp_final = entry_price * (1 + dynamic_tp)
                is_stop = curr_price <= final_stop
                is_tp_half = curr_price >= tp_stage_1 and tp_stage == 0
                is_tp_full = curr_price >= tp_final and not ignore_tp
            else:
                min_price = post_entry_df["Low"].min() if not post_entry_df.empty else curr_price
                final_stop = min(entry_price * (1 + dynamic_sl), min_price * (1 + dynamic_sl))
                tp_stage_1 = entry_price * (1 - dynamic_tp * 0.5)
                tp_final = entry_price * (1 - dynamic_tp)
                is_stop = curr_price >= final_stop
                is_tp_half = curr_price <= tp_stage_1 and tp_stage == 0
                is_tp_full = curr_price <= tp_final and not ignore_tp

            exit_msg = ""
            sell_shares = 0

            if is_stop:
                exit_msg, sell_shares = "🛑 觸發防守線", shares
            elif is_tp_full:
                exit_msg, sell_shares = "🎯 達標最終停利", shares
            elif is_tp_half:
                if trend_is_with_me and adx_is_strong:
                    cursor.execute(
                        "UPDATE active_positions SET [停利階段] = 1 WHERE [Ticker SYMBOL] = ? AND [進場時間] = ?",
                        (ticker, pos["進場時間"]),
                    )
                    daily_log.append(f"🌊 {ticker} 達第一階段，趨勢強抱緊！")
                else:
                    exit_msg, sell_shares = "💰 達第一階段，減碼50%", max(1, shares // 2)

            if sell_shares > 0:
                trade_dir_int = 1 if is_long else -1
                slippage = PARAMS.get("MARKET_SLIPPAGE", 0.001)
                actual_exit_price = apply_slippage(curr_price, -trade_dir_int, slippage)

                pnl, invested = calculate_pnl(
                    trade_dir_int,
                    entry_price,
                    actual_exit_price,
                    sell_shares,
                    PARAMS["FEE_RATE"] * PARAMS["FEE_DISCOUNT"],
                    PARAMS["TAX_RATE"],
                )

                current_cash += (invested + pnl)

                remaining_shares = shares - sell_shares
                if remaining_shares <= 0:
                    cursor.execute(
                        "DELETE FROM active_positions WHERE [Ticker SYMBOL] = ? AND [進場時間] = ?",
                        (ticker, pos["進場時間"]),
                    )
                else:
                    remaining_invested = max(0.0, float(pos["投入資金"]) - invested)
                    cursor.execute(
                        """
                        UPDATE active_positions
                        SET [進場股數] = ?, [投入資金] = ?, [停利階段] = 1
                        WHERE [Ticker SYMBOL] = ? AND [進場時間] = ?
                        """,
                        (remaining_shares, remaining_invested, ticker, pos["進場時間"]),
                    )

                try:
                    profit_pct = (pnl / invested) * 100 if invested > 0 else 0.0
                    cursor.execute(
                        """
                        INSERT INTO trade_history (
                            [策略名稱], [Ticker SYMBOL], [方向], [進場時間], [出場時間],
                            [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金],
                            [市場狀態], [進場陣型], [期望值], [預期停損(%)], [預期停利(%)],
                            [風報比(RR)], [風險金額]
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "實戰自動結算",
                            ticker,
                            direction,
                            pos["進場時間"],
                            datetime.now(),
                            entry_price,
                            actual_exit_price,
                            profit_pct,
                            pnl,
                            current_cash,
                            pos.get("市場狀態", None),
                            setup_tag,
                            float(pos.get("期望值", 0) if pd.notna(pos.get("期望值", 0)) else 0),
                            float(pos.get("預期停損(%)", 0) if pd.notna(pos.get("預期停損(%)", 0)) else 0),
                            float(pos.get("預期停利(%)", 0) if pd.notna(pos.get("預期停利(%)", 0)) else 0),
                            float(pos.get("風報比(RR)", 0) if pd.notna(pos.get("風報比(RR)", 0)) else 0),
                            float(pos.get("風險金額", 0) if pd.notna(pos.get("風險金額", 0)) else 0),
                        ),
                    )
                except Exception as e:
                    print(f"⚠️ {ticker} 寫入歷史戰績表失敗: {e}")

                daily_log.append(f"{exit_msg} {ticker}: 損益 ${pnl:,.0f}")

    conn.commit()

    # 重新讀一次持倉狀態，避免平倉後還沿用舊曝險
    active_df = _read_active_positions(conn)
    stock_value = _safe_float(active_df["投入資金"].sum(), 0.0) if not active_df.empty and "投入資金" in active_df.columns else 0.0
    total_nav = current_cash + stock_value if (current_cash + stock_value) > 0 else 1_000_000
    portfolio_state = _current_portfolio_state(active_df, total_nav)

    # ==========================================
    # ⚔️ 階段二：執行建倉任務
    # ==========================================
    try:
        decisions = pd.read_csv("daily_decision_desk.csv")
    except Exception:
        decisions = pd.DataFrame()
    if decisions.empty:
        try:
            builder = _LEVEL3_SERVICES.get("DecisionExecutionBridge")
            if builder is not None:
                out_path, _ = builder.build()
                decisions = pd.read_csv(out_path, encoding="utf-8-sig")
                if "Ticker SYMBOL" in decisions.columns and "Ticker" not in decisions.columns:
                    decisions["Ticker"] = decisions["Ticker SYMBOL"]
                if "Action" in decisions.columns and "Direction" not in decisions.columns:
                    decisions["Direction"] = decisions["Action"].map({"BUY": "做多(Long)", "SELL": "做空(Short)", "SHORT": "做空(Short)", "COVER": "做多(Long)"}).fillna("做多(Long)")
        except Exception:
            pass

    if not decisions.empty:
        for _, row in decisions.iterrows():
            ticker = row.get("Ticker", "Unknown")
            trade_direction = row.get("Direction", "做多(Long)")

            try:
                kill_manager = _LEVEL3_SERVICES.get("KillSwitchManager")
                if kill_manager is not None:
                    blocked, reasons = kill_manager.is_blocked(symbol=ticker, strategy=str(row.get("Structure", "")))
                    if blocked:
                        daily_log.append(f"🛑 略過 {ticker}: {' | '.join(reasons)}")
                        continue
            except Exception:
                pass

            allow_signal, reason_signal = _passes_signal_gate(row)
            if not allow_signal:
                daily_log.append(f"⛔ 略過 {ticker}: {reason_signal}")
                continue

            cursor.execute("SELECT COUNT(*) FROM active_positions WHERE [Ticker SYMBOL] = ?", (ticker,))
            if cursor.fetchone()[0] > 0:
                daily_log.append(f"⏭️ 略過 {ticker}: 已有持倉")
                continue

            allow_port, reason_port = _passes_portfolio_gate(row, total_nav, portfolio_state)
            if not allow_port:
                daily_log.append(f"⛔ 略過 {ticker}: {reason_port}")
                continue

            kelly_pct = _safe_float(row.get("Kelly_Pos", 0.0), 0.0)

            df = smart_download(ticker, period="5d")
            if df.empty:
                daily_log.append(f"⚠️ 略過 {ticker}: 無最新價格")
                continue

            curr_price = float(df["Close"].iloc[-1])

            shares = int((total_nav * kelly_pct) / curr_price)
            shares = int(shares // 1000) * 1000 if shares >= 1000 else shares
            if shares < 1:
                daily_log.append(f"⚠️ 略過 {ticker}: 換算股數不足")
                continue

            total_cost = curr_price * shares * (1 + PARAMS["FEE_RATE"] * PARAMS["FEE_DISCOUNT"])
            can_afford = current_cash >= total_cost or PARAMS.get("IGNORE_CASH_LIMIT", False)

            if not can_afford:
                daily_log.append(f"⛔ 略過 {ticker}: 現金不足")
                continue

            if not PARAMS.get("IGNORE_CASH_LIMIT", False):
                current_cash -= total_cost

            entry_metrics = _build_entry_metrics(row)
            risk_amount = total_cost * entry_metrics["風險金額比率"]

            cursor.execute(
                """
                INSERT INTO active_positions (
                    [Ticker SYMBOL], [方向], [進場時間], [進場價], [投入資金],
                    [進場股數], [停利階段], [市場狀態], [進場陣型], [期望值],
                    [預期停損(%)], [預期停利(%)], [風報比(RR)], [風險金額]
                )
                VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    trade_direction,
                    datetime.now(),
                    curr_price,
                    total_cost,
                    shares,
                    entry_metrics["市場狀態"],
                    entry_metrics["進場陣型"],
                    entry_metrics["期望值"],
                    entry_metrics["預期停損(%)"],
                    entry_metrics["預期停利(%)"],
                    entry_metrics["風報比(RR)"],
                    risk_amount,
                ),
            )
            conn.commit()

            # 立刻更新組合狀態，避免同一輪連續下單超限
            sector = get_stock_sector(ticker)
            direction_bucket = _direction_bucket(trade_direction)
            alloc = total_cost / total_nav if total_nav > 0 else 0.0
            portfolio_state["total_alloc"] += alloc
            portfolio_state["sector_alloc"][sector] = portfolio_state["sector_alloc"].get(sector, 0.0) + alloc
            portfolio_state["sector_count"][sector] = portfolio_state["sector_count"].get(sector, 0) + 1
            portfolio_state["direction_alloc"][direction_bucket] = portfolio_state["direction_alloc"].get(direction_bucket, 0.0) + alloc

            action_icon = "🟢 做多" if "Long" in str(trade_direction) else "🔴 放空"
            daily_log.append(
                f"{action_icon} {ticker}: {shares}股 "
                f"(花費 ${total_cost:,.0f} | EV {entry_metrics['期望值']:.3f} | "
                f"WB {entry_metrics['Weighted_Buy_Score']:.2f} | "
                f"WS {entry_metrics['Weighted_Sell_Score']:.2f} | Gap {entry_metrics['Score_Gap']:.2f})"
            )

    # ==========================================
    # 📊 階段三：結算與發送 LINE 戰報
    # ==========================================
    cursor.execute(
        """
        UPDATE account_info
        SET [可用現金] = ?, [最後更新時間] = ?
        WHERE [帳戶名稱] = N'我的實戰帳戶'
        """,
        (current_cash, datetime.now()),
    )
    conn.commit()

    final_active_df = _read_active_positions(conn)
    final_stock_value = _safe_float(final_active_df["投入資金"].sum(), 0.0) if not final_active_df.empty and "投入資金" in final_active_df.columns else 0.0
    total_equity = current_cash + final_stock_value

    report_lines = [
        "📊【每日實戰帳戶戰報】",
        f"💰 可用現金：${current_cash:,.0f}",
        f"📦 持股市值：${final_stock_value:,.0f}",
        f"🏦 帳戶總資產：${total_equity:,.0f}",
        "-" * 20,
    ]
    report_lines.extend(daily_log if daily_log else ["✅ 今日無新動作，持倉平穩。"])
    full_report = "\n".join(report_lines)

    print("\n" + "=" * 60)
    print(full_report)
    print("=" * 60)

    send_line_bot_msg(full_report)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    run_eod_broker()
