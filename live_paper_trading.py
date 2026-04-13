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
from fts_strategy_policy_layer import get_strategy_policy
from fts_model_layer import evaluate_model_signal
from fts_execution_layer import build_entry_metrics as _build_entry_metrics_layer, signal_gate as _signal_gate_layer, portfolio_gate as _portfolio_gate_layer, compute_position_plan
from sector_classifier import get_stock_sector
from fts_execution_ledger import ExecutionLedger
from fts_live_watchlist_loader import LiveWatchlistLoader
try:
    from fts_level3_runtime_loader import build_level3_services
    _LEVEL3_SERVICES, _LEVEL3_META = build_level3_services()
except Exception:
    _LEVEL3_SERVICES, _LEVEL3_META = ({}, {'status': 'level3_unavailable'})


# ==========================================
# ⚙️ 系統環境設定
# ==========================================
IS_TEST_MODE = True

LINE_BOT_TOKEN = PARAMS.get("ALERT_LINE_BOT_TOKEN", "")
LINE_USER_ID = PARAMS.get("ALERT_LINE_USER_ID", "")

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




def _infer_candidate_lane(row) -> str:
    direction = str(row.get("Direction", "")).upper()
    structure = str(row.get("Structure", row.get("Setup_Tag", ""))).upper()
    regime = str(row.get("Regime", "")).strip()
    if "SHORT" in direction or "空" in direction or "SHORT" in structure:
        return "SHORT"
    if regime == "區間盤整" or "RANGE" in structure:
        return "RANGE"
    return "LONG"


def _load_directional_allow_map():
    try:
        _, payload = LiveWatchlistLoader().resolve_live_watchlist()
        lanes = payload.get("lanes", {}) if isinstance(payload, dict) else {}
        allow_map = {}
        for lane, items in lanes.items():
            for item in items or []:
                ticker = str(item.get("ticker") or "").strip()
                if not ticker:
                    continue
                allow_map.setdefault(ticker, set()).add(str(lane).upper())
        return allow_map
    except Exception:
        return {}

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
    return _build_entry_metrics_layer(row, params=PARAMS)


def _passes_signal_gate(row, candidate_lane: str = 'LONG'):
    regime = row.get("Regime", "未知")
    structure = row.get("Structure", row.get("Setup_Tag", "AI訊號"))
    policy = get_strategy_policy(structure, regime=regime)
    model_decision = evaluate_model_signal(
        row,
        regime,
        min_proba=float(policy.get("min_proba", 0.5)),
        base_multiplier=float(policy.get("multiplier", 1.0)),
        direction_scope=str(candidate_lane).upper(),
    )
    gate = _signal_gate_layer(row, model_decision=model_decision, params=PARAMS)
    reason = "通過訊號閘門" if gate.allowed else " | ".join(gate.reasons)
    return gate.allowed, reason


def _passes_portfolio_gate(row, total_nav, portfolio_state):
    ticker = str(row.get("Ticker", "")).strip()
    sector = get_stock_sector(ticker)
    gate = _portfolio_gate_layer(row, total_nav, portfolio_state, sector_name=sector, params=PARAMS)
    reason = "通過組合閘門" if gate.allowed else " | ".join(gate.reasons)
    return gate.allowed, reason


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

            active_strategy = get_active_strategy(setup_tag, regime=latest_row.get('Regime', '未知'))
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
        directional_allow_map = _load_directional_allow_map()
        decisions = _augment_directional_decisions(decisions, directional_allow_map)
        for _, row in decisions.iterrows():
            ticker = row.get("Ticker", "Unknown")
            trade_direction = row.get("Direction", "做多(Long)")
            candidate_lane = _infer_candidate_lane(row)
            row['Candidate_Long'] = int(candidate_lane == 'LONG')
            row['Candidate_Short'] = int(candidate_lane == 'SHORT')
            row['Candidate_Range'] = int(candidate_lane == 'RANGE')
            allowed_lanes = directional_allow_map.get(str(ticker).strip(), set())
            row['Approved_Long'] = int('LONG' in allowed_lanes)
            row['Approved_Short'] = int('SHORT' in allowed_lanes)
            row['Approved_Range'] = int('RANGE' in allowed_lanes)
            if allowed_lanes and candidate_lane not in allowed_lanes:
                daily_log.append(f"⛔ 略過 {ticker}: lane {candidate_lane} 不在 approved directional watchlist {sorted(allowed_lanes)}")
                continue

            try:
                kill_manager = _LEVEL3_SERVICES.get("KillSwitchManager")
                if kill_manager is not None:
                    blocked, reasons = kill_manager.is_blocked(symbol=ticker, strategy=str(row.get("Structure", "")))
                    if blocked:
                        daily_log.append(f"🛑 略過 {ticker}: {' | '.join(reasons)}")
                        continue
            except Exception:
                pass

            allow_signal, reason_signal = _passes_signal_gate(row, candidate_lane)
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

            entry_metrics = _build_entry_metrics(row)
            plan = compute_position_plan(
                row=row,
                curr_price=curr_price,
                total_nav=total_nav,
                current_cash=current_cash,
                entry_metrics=entry_metrics,
                params=PARAMS,
            )
            if not plan.allowed:
                daily_log.append(f"⚠️ 略過 {ticker}: {plan.reason}")
                continue

            shares = int(plan.shares)
            total_cost = float(plan.total_cost)
            risk_amount = float(plan.risk_amount)

            if not PARAMS.get("IGNORE_CASH_LIMIT", False):
                current_cash -= total_cost

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
                f"(花費 ${total_cost:,.0f} | 策略 {entry_metrics['策略名稱']} | EV {entry_metrics['期望值']:.3f} | "
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


def _record_directional_execution_event(event_type: str, payload: dict):
    try:
        ExecutionLedger().record(event_type, payload)
    except Exception:
        pass

def _augment_directional_decisions(decisions: pd.DataFrame, allow_map: dict[str, set[str]]) -> pd.DataFrame:
    if not bool(PARAMS.get("DIRECTIONAL_DECISION_AUGMENT", True)):
        return decisions
    try:
        _, payload = LiveWatchlistLoader().resolve_live_watchlist()
    except Exception:
        return decisions
    lanes_payload = payload.get('lanes', {}) if isinstance(payload, dict) else {}
    max_per_lane = int(PARAMS.get('DIRECTIONAL_DECISION_AUGMENT_MAX_PER_LANE', 2))
    use_screening = bool(PARAMS.get('DIRECTIONAL_DECISION_AUGMENT_USE_SCREENING', True))
    existing = set(str(x).strip() for x in decisions.get('Ticker', pd.Series(dtype=str)).astype(str).tolist()) if isinstance(decisions, pd.DataFrame) else set()
    aug_rows = []
    trigger = float(PARAMS.get('TRIGGER_SCORE', 2.0))
    for lane in ['SHORT', 'RANGE', 'LONG']:
        items = lanes_payload.get(lane, []) if isinstance(lanes_payload, dict) else []
        take = 0
        for item in items:
            if take >= max_per_lane:
                break
            ticker = str(item.get('ticker') or '').strip()
            if not ticker or ticker in existing:
                continue
            row = {}
            if use_screening:
                try:
                    row = inspect_stock(ticker) or {}
                except Exception:
                    row = {}
            row = dict(row or {})
            row['Ticker'] = ticker
            row.setdefault('Ticker SYMBOL', ticker)
<<<<<<< HEAD
            chosen_side = 'LONG'
            if lane == 'SHORT':
                chosen_side = 'SHORT'
=======
            if lane == 'SHORT':
>>>>>>> ad1db6bec225a276b4ad4c7df6c049d994a30092
                row['Direction'] = '做空(Short)'
                row.setdefault('Regime', '趨勢空頭')
                row.setdefault('Structure', '趨勢空頭追擊')
            elif lane == 'RANGE':
<<<<<<< HEAD
                range_side = str(item.get('preferred_side') or item.get('range_side') or '').upper()
                weighted_buy_existing = float(row.get('Weighted_Buy_Score', 0.0) or 0.0)
                weighted_sell_existing = float(row.get('Weighted_Sell_Score', 0.0) or 0.0)
                if range_side not in {'LONG', 'SHORT'}:
                    if weighted_sell_existing > weighted_buy_existing:
                        range_side = 'SHORT'
                    elif weighted_buy_existing > weighted_sell_existing:
                        range_side = 'LONG'
                    else:
                        range_side = 'SHORT' if (take % 2 == 1) else 'LONG'
                chosen_side = range_side
                row['Regime'] = '區間盤整'
                if chosen_side == 'SHORT':
                    row['Direction'] = '做空(Short)'
                    row.setdefault('Structure', '盤整高拋')
                else:
                    row['Direction'] = '做多(Long)'
                    row.setdefault('Structure', '盤整低吸')
            else:
                chosen_side = 'LONG'
=======
                row['Direction'] = '做多(Long)'
                row['Regime'] = '區間盤整'
                row.setdefault('Structure', '盤整均值回歸')
            else:
>>>>>>> ad1db6bec225a276b4ad4c7df6c049d994a30092
                row['Direction'] = '做多(Long)'
                row.setdefault('Regime', '趨勢多頭')
                row.setdefault('Structure', '趨勢多頭攻堅')
            row.setdefault('AI_Proba', max(0.5, float(item.get('hit_rate', 0.5) or 0.5)))
            row.setdefault('Realized_EV', float(item.get('oot_ev', 0.0) or 0.0))
            row.setdefault('Kelly_Pos', float(PARAMS.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03)))
            row.setdefault('Health', 'KEEP')
<<<<<<< HEAD
            if chosen_side == 'SHORT':
                row.setdefault('Weighted_Sell_Score', trigger + 0.25)
                row.setdefault('Weighted_Buy_Score', max(0.0, trigger - 0.75))
            else:
                row.setdefault('Weighted_Buy_Score', trigger + 0.25)
                row.setdefault('Weighted_Sell_Score', max(0.0, trigger - 0.75))
            row.setdefault('Score_Gap', float(row['Weighted_Buy_Score']) - float(row['Weighted_Sell_Score']))
=======
            row.setdefault('Weighted_Buy_Score', trigger + 0.25)
            row.setdefault('Weighted_Sell_Score', max(0.0, trigger - 0.75))
            row.setdefault('Score_Gap', max(0.1, float(row['Weighted_Buy_Score']) - float(row['Weighted_Sell_Score'])))
>>>>>>> ad1db6bec225a276b4ad4c7df6c049d994a30092
            aug_rows.append(row)
            existing.add(ticker)
            take += 1
    if not aug_rows:
        return decisions
    aug_df = pd.DataFrame(aug_rows)
    if decisions is None or decisions.empty:
        return aug_df
    return pd.concat([decisions, aug_df], ignore_index=True, sort=False)



if __name__ == "__main__":
    run_eod_broker()

