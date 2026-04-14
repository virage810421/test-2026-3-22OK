import warnings

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

from datetime import datetime
import os
import requests
import pandas as pd
import pyodbc

from config import PARAMS
from fts_service_api import smart_download, apply_slippage, calculate_pnl, inspect_stock, add_chip_data
from fts_service_api import get_active_strategy
from fts_strategy_policy_layer import get_strategy_policy
from fts_model_layer import evaluate_model_signal
from fts_execution_layer import build_entry_metrics as _build_entry_metrics_layer, signal_gate as _signal_gate_layer, portfolio_gate as _portfolio_gate_layer, compute_position_plan
from sector_classifier import get_stock_sector
from fts_execution_ledger import ExecutionLedger
try:
    from db_logger import SQLServerExecutionLogger
except Exception:
    SQLServerExecutionLogger = None
from fts_live_watchlist_loader import LiveWatchlistLoader
try:
    from fts_level_runtime import build_level3_services
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



def _table_has_column(cursor, table_name: str, column_name: str) -> bool:
    try:
        cursor.execute("""
            SELECT 1
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
        """, (table_name, column_name))
        return cursor.fetchone() is not None
    except Exception:
        return False


def _ensure_exit_sql_columns(cursor):
    additions = {
        '目前停損價': 'FLOAT NULL',
        '停損單號': 'NVARCHAR(64) NULL',
        '目標倉位倍率': 'DECIMAL(10,4) NULL',
        'Exit_State': 'NVARCHAR(20) NULL',
        'Exit_Action': 'NVARCHAR(32) NULL',
        '最後停損更新時間': 'DATETIME NULL',
    }
    for col, typ in additions.items():
        try:
            cursor.execute(f"IF COL_LENGTH('dbo.active_positions', N'{col}') IS NULL ALTER TABLE dbo.active_positions ADD [{col}] {typ}")
        except Exception:
            pass
    try:
        cursor.execute("IF COL_LENGTH('dbo.trade_history', N'出場原因') IS NULL ALTER TABLE dbo.trade_history ADD [出場原因] NVARCHAR(100) NULL")
    except Exception:
        pass
    try:
        cursor.connection.commit()
    except Exception:
        pass


def _load_stop_replace_payloads() -> pd.DataFrame:
    candidates = [
        os.path.join('data', str(PARAMS.get('EXIT_SQL_STOP_SOURCE_FILE', 'stop_replace_payloads.csv'))),
        os.path.join('runtime', str(PARAMS.get('EXIT_SQL_STOP_SOURCE_FILE', 'stop_replace_payloads.csv'))),
        os.path.join('data', 'stop_replace_payloads.csv'),
        os.path.join('runtime', 'stop_replace_payloads.csv'),
    ]
    for path in candidates:
        if path and os.path.exists(path):
            try:
                return pd.read_csv(path, encoding='utf-8-sig')
            except Exception:
                try:
                    return pd.read_csv(path)
                except Exception:
                    continue
    return pd.DataFrame()


def _build_stop_payload_map(stop_df: pd.DataFrame) -> dict[str, dict]:
    mp: dict[str, dict] = {}
    if stop_df is None or stop_df.empty:
        return mp
    for _, row in stop_df.iterrows():
        ticker = str(row.get('Ticker', '') or '').strip()
        if not ticker:
            continue
        mp[ticker] = dict(row)
    return mp


def _sync_stop_plan_to_sql(cursor, pos, stop_row: dict) -> None:
    if not bool(PARAMS.get('EXIT_SQL_STOP_SYNC_ENABLE', True)):
        return
    ticker = str(pos.get('Ticker SYMBOL', '') or '').strip()
    entry_time = pos.get('進場時間')
    if not ticker or entry_time is None:
        return
    desired_stop = _safe_float(stop_row.get('Desired_Stop_Price', 0), 0.0)
    if desired_stop <= 0:
        return
    stop_oid = str(stop_row.get('Broker_Stop_Order_ID', '') or stop_row.get('Client_Order_ID', '') or '')
    target_mult = _safe_float(stop_row.get('Target_Position_Multiplier', 1.0), 1.0)
    exit_state = str(stop_row.get('Exit_State', 'DEFEND') or 'DEFEND')
    exit_action = str(stop_row.get('Exit_Action', 'TIGHTEN_STOP') or 'TIGHTEN_STOP')
    updates = []
    params = []
    if _table_has_column(cursor, 'active_positions', '目前停損價'):
        updates.append('[目前停損價] = ?')
        params.append(desired_stop)
    if _table_has_column(cursor, 'active_positions', '停損單號'):
        updates.append('[停損單號] = ?')
        params.append(stop_oid)
    if _table_has_column(cursor, 'active_positions', '目標倉位倍率'):
        updates.append('[目標倉位倍率] = ?')
        params.append(target_mult)
    if _table_has_column(cursor, 'active_positions', 'Exit_State'):
        updates.append('[Exit_State] = ?')
        params.append(exit_state)
    if _table_has_column(cursor, 'active_positions', 'Exit_Action'):
        updates.append('[Exit_Action] = ?')
        params.append(exit_action)
    if _table_has_column(cursor, 'active_positions', '最後停損更新時間'):
        updates.append('[最後停損更新時間] = ?')
        params.append(datetime.now())
    if not updates:
        return
    sql = f"UPDATE active_positions SET {', '.join(updates)} WHERE [Ticker SYMBOL] = ? AND [進場時間] = ?"
    params.extend([ticker, entry_time])
    try:
        cursor.execute(sql, tuple(params))
    except Exception:
        pass


def _execute_sql_position_exit(cursor, pos, sell_shares: int, actual_exit_price: float, current_cash: float, daily_log: list[str], exit_msg: str, exit_reason: str = '', execution_logger=None) -> float:
    ticker = str(pos.get('Ticker SYMBOL', '') or '')
    direction = pos.get('方向', '')
    entry_price = _safe_float(pos.get('進場價', 0), 0.0)
    shares = _safe_int(pos.get('進場股數', 0), 0)
    setup_tag = pos.get('進場陣型', '傳統訊號')
    sell_shares = min(max(int(sell_shares), 0), max(shares, 0))
    if sell_shares <= 0:
        return current_cash
    is_long = ('Long' in str(direction)) or ('多' in str(direction))
    trade_dir_int = 1 if is_long else -1
    pnl, invested = calculate_pnl(
        trade_dir_int,
        entry_price,
        actual_exit_price,
        sell_shares,
        PARAMS['FEE_RATE'] * PARAMS['FEE_DISCOUNT'],
        PARAMS['TAX_RATE'],
    )
    current_cash += (invested + pnl)
    remaining_shares = shares - sell_shares
    if remaining_shares <= 0:
        cursor.execute(
            'DELETE FROM active_positions WHERE [Ticker SYMBOL] = ? AND [進場時間] = ?',
            (ticker, pos['進場時間']),
        )
    else:
        remaining_invested = max(0.0, float(pos.get('投入資金', 0) or 0) - invested)
        updates = ['[進場股數] = ?', '[投入資金] = ?', '[停利階段] = 1']
        params = [remaining_shares, remaining_invested]
        if _table_has_column(cursor, 'active_positions', '目標倉位倍率'):
            updates.append('[目標倉位倍率] = ?')
            params.append(round(remaining_shares / max(shares, 1), 4))
        sql = f"UPDATE active_positions SET {', '.join(updates)} WHERE [Ticker SYMBOL] = ? AND [進場時間] = ?"
        params.extend([ticker, pos['進場時間']])
        cursor.execute(sql, tuple(params))
    try:
        profit_pct = (pnl / invested) * 100 if invested > 0 else 0.0
        has_reason_col = _table_has_column(cursor, 'trade_history', '出場原因')
        if has_reason_col:
            cursor.execute(
                """
                INSERT INTO trade_history (
                    [策略名稱], [Ticker SYMBOL], [方向], [進場時間], [出場時間],
                    [進場價], [出場價], [報酬率(%)], [淨損益金額], [結餘本金],
                    [市場狀態], [進場陣型], [期望值], [預期停損(%)], [預期停利(%)],
                    [風報比(RR)], [風險金額], [出場原因]
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    '實戰自動結算', ticker, direction, pos['進場時間'], datetime.now(), entry_price, actual_exit_price, profit_pct, pnl, current_cash,
                    pos.get('市場狀態', None), setup_tag,
                    float(pos.get('期望值', 0) if pd.notna(pos.get('期望值', 0)) else 0),
                    float(pos.get('預期停損(%)', 0) if pd.notna(pos.get('預期停損(%)', 0)) else 0),
                    float(pos.get('預期停利(%)', 0) if pd.notna(pos.get('預期停利(%)', 0)) else 0),
                    float(pos.get('風報比(RR)', 0) if pd.notna(pos.get('風報比(RR)', 0)) else 0),
                    float(pos.get('風險金額', 0) if pd.notna(pos.get('風險金額', 0)) else 0),
                    str(exit_reason or exit_msg),
                ),
            )
        else:
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
                    '實戰自動結算', ticker, direction, pos['進場時間'], datetime.now(), entry_price, actual_exit_price, profit_pct, pnl, current_cash,
                    pos.get('市場狀態', None), setup_tag,
                    float(pos.get('期望值', 0) if pd.notna(pos.get('期望值', 0)) else 0),
                    float(pos.get('預期停損(%)', 0) if pd.notna(pos.get('預期停損(%)', 0)) else 0),
                    float(pos.get('預期停利(%)', 0) if pd.notna(pos.get('預期停利(%)', 0)) else 0),
                    float(pos.get('風報比(RR)', 0) if pd.notna(pos.get('風報比(RR)', 0)) else 0),
                    float(pos.get('風險金額', 0) if pd.notna(pos.get('風險金額', 0)) else 0),
                ),
            )
    except Exception as e:
        print(f"⚠️ {ticker} 寫入歷史戰績表失敗: {e}")
    if execution_logger is not None:
        try:
            side = 'SELL' if is_long else 'BUY'
            ts = datetime.now().isoformat(timespec='seconds')
            order_id = f'SQL-EXIT-{ticker}-{str(pos.get('進場時間', ts)).replace(':', '').replace(' ', '_')}'
            execution_logger.insert_order({
                'order_id': order_id,
                'client_order_id': order_id,
                'ticker_symbol': ticker,
                'direction_bucket': side,
                'strategy_bucket': 'live_paper_trading_sql_exit',
                'status': 'FILLED',
                'qty': int(sell_shares),
                'filled_qty': int(sell_shares),
                'remaining_qty': 0,
                'avg_fill_price': float(actual_exit_price),
                'order_type': 'MARKET',
                'submitted_price': float(actual_exit_price),
                'ref_price': float(actual_exit_price),
                'signal_id': str(pos.get('Ticker SYMBOL', ticker)),
                'note': str(exit_reason or exit_msg),
                'created_at': ts,
                'updated_at': ts,
            })
            execution_logger.insert_fill({
                'fill_id': f'{order_id}-{ts}',
                'order_id': order_id,
                'ticker_symbol': ticker,
                'direction_bucket': side,
                'fill_qty': int(sell_shares),
                'fill_price': float(actual_exit_price),
                'fill_time': ts,
                'commission': 0.0,
                'tax': 0.0,
                'slippage': 0.0,
                'strategy_name': 'live_paper_trading_sql_exit',
                'signal_id': str(pos.get('Ticker SYMBOL', ticker)),
                'note': str(exit_reason or exit_msg),
            })
        except Exception:
            pass
    daily_log.append(f"{exit_msg} {ticker}: 損益 ${pnl:,.0f}")
    return current_cash


def _maybe_apply_protective_stop(cursor, pos, curr_price: float, stop_row: dict, current_cash: float, daily_log: list[str], execution_logger=None) -> tuple[float, bool]:
    ticker = str(pos.get('Ticker SYMBOL', '') or '').strip()
    direction = str(pos.get('方向', '') or '')
    shares = _safe_int(pos.get('進場股數', 0), 0)
    is_long = ('Long' in direction) or ('多' in direction)
    desired_stop = _safe_float(stop_row.get('Desired_Stop_Price', 0), 0.0)
    if desired_stop <= 0:
        desired_stop = _safe_float(pos.get('目前停損價', pos.get('Current_Stop_Price', 0)), 0.0)
    if desired_stop <= 0 or shares <= 0:
        return current_cash, False
    if is_long:
        triggered = curr_price <= desired_stop
        trigger_ref = min(curr_price, desired_stop)
    else:
        triggered = curr_price >= desired_stop
        trigger_ref = max(curr_price, desired_stop)
    if not triggered:
        return current_cash, False
    slippage = PARAMS.get('MARKET_SLIPPAGE', 0.001)
    trade_dir_int = 1 if is_long else -1
    actual_exit_price = apply_slippage(trigger_ref, -trade_dir_int, slippage)
    current_cash = _execute_sql_position_exit(
        cursor,
        pos,
        shares,
        actual_exit_price,
        current_cash,
        daily_log,
        exit_msg='🛡️ 觸發保護停損',
        exit_reason=str(stop_row.get('Exit_Action', 'TIGHTEN_STOP') or 'TIGHTEN_STOP'),
        execution_logger=execution_logger,
    )
    return current_cash, True





def _sync_execution_sql_runtime_tables(conn, current_cash: float, execution_logger=None, note: str = '') -> None:
    if execution_logger is None or not bool(PARAMS.get('EXECUTION_SQL_SYNC_ENABLED', True)):
        return
    try:
        active_df = _read_active_positions(conn)
        rows = []
        for _, pos in active_df.iterrows():
            ticker = str(pos.get('Ticker SYMBOL', '') or '').strip()
            qty = _safe_int(pos.get('進場股數', 0), 0)
            if not ticker or qty <= 0:
                continue
            avg_cost = _safe_float(pos.get('進場價', 0), 0.0)
            market_price = avg_cost
            market_value = _safe_float(pos.get('投入資金', 0), avg_cost * qty)
            rows.append({
                'ticker': ticker,
                'qty': qty if ('Long' in str(pos.get('方向', '')) or '多' in str(pos.get('方向', ''))) else -qty,
                'available_qty': qty,
                'avg_cost': avg_cost,
                'market_price': market_price,
                'market_value': market_value,
                'unrealized_pnl': 0.0,
                'realized_pnl': 0.0,
                'strategy_name': 'live_paper_trading',
                'industry': get_stock_sector(ticker),
                'note': str(pos.get('Exit_State', 'HOLD') or 'HOLD'),
            })
        execution_logger.sync_runtime_snapshot({
            'snapshot_time': datetime.now().isoformat(timespec='seconds'),
            'account_name': '我的實戰帳戶',
            'cash': float(current_cash or 0.0),
            'market_value': float(sum(float(r.get('market_value', 0) or 0) for r in rows)),
            'equity': float(current_cash or 0.0) + float(sum(float(r.get('market_value', 0) or 0) for r in rows)),
            'buying_power': float(current_cash or 0.0),
            'broker_type': 'live_paper_sql',
            'note': note or 'live_paper_trading_sync',
        }, rows, snapshot_time=datetime.now().isoformat(timespec='seconds'), note=note or 'live_paper_trading_sync')
    except Exception:
        pass

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
        execution_logger = SQLServerExecutionLogger(enabled=bool(PARAMS.get('EXECUTION_SQL_SYNC_ENABLED', True))) if SQLServerExecutionLogger is not None else None
        _ensure_exit_sql_columns(cursor)
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
    stop_payload_df = _load_stop_replace_payloads() if bool(PARAMS.get('EXIT_SQL_STOP_SYNC_ENABLE', True)) else pd.DataFrame()
    stop_payload_map = _build_stop_payload_map(stop_payload_df)

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

            stop_row = stop_payload_map.get(ticker)
            if stop_row:
                _sync_stop_plan_to_sql(cursor, pos, stop_row)
                current_cash, stop_handled = _maybe_apply_protective_stop(cursor, pos, curr_price, stop_row, current_cash, daily_log)
                if stop_handled:
                    continue

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
                current_cash = _execute_sql_position_exit(cursor, pos, sell_shares, actual_exit_price, current_cash, daily_log, exit_msg, exit_msg, execution_logger=execution_logger)

    conn.commit()
    _sync_execution_sql_runtime_tables(conn, current_cash, execution_logger=execution_logger, note='after_exit_phase')

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
            if str(row.get('Exit_Action', '') or '').strip().upper() in {'REDUCE', 'EXIT', 'TIGHTEN_STOP', 'MOVE_TO_BREAK_EVEN'} or str(row.get('Exit_State', '') or '').strip().upper() in {'WATCH_EXIT', 'DEFEND', 'REDUCE', 'EXIT'}:
                continue
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

            entry_time = datetime.now()
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
                    entry_time,
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
            try:
                stop_px = curr_price * (1 - float(entry_metrics['預期停損(%)'])) if 'Long' in str(trade_direction) or '多' in str(trade_direction) else curr_price * (1 + float(entry_metrics['預期停損(%)']))
                updates = []
                params = []
                if _table_has_column(cursor, 'active_positions', '目前停損價'):
                    updates.append('[目前停損價] = ?')
                    params.append(float(stop_px))
                if _table_has_column(cursor, 'active_positions', '目標倉位倍率'):
                    updates.append('[目標倉位倍率] = ?')
                    params.append(1.0)
                if _table_has_column(cursor, 'active_positions', 'Exit_State'):
                    updates.append('[Exit_State] = ?')
                    params.append('HOLD')
                if _table_has_column(cursor, 'active_positions', 'Exit_Action'):
                    updates.append('[Exit_Action] = ?')
                    params.append('HOLD')
                if _table_has_column(cursor, 'active_positions', '最後停損更新時間'):
                    updates.append('[最後停損更新時間] = ?')
                    params.append(datetime.now())
                if updates:
                    sql = f"UPDATE active_positions SET {', '.join(updates)} WHERE [Ticker SYMBOL] = ? AND [進場時間] = ?"
                    params.extend([ticker, entry_time])
                    try:
                        cursor.execute(sql, tuple(params))
                        conn.commit()
                    except Exception:
                        pass
            except Exception:
                pass

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
    _sync_execution_sql_runtime_tables(conn, current_cash, execution_logger=execution_logger, note='end_of_run')

    cursor.close()
    conn.close()
    if execution_logger is not None:
        try:
            execution_logger.close()
        except Exception:
            pass


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
            if lane == 'SHORT':
                row['Direction'] = '做空(Short)'
                row.setdefault('Regime', '趨勢空頭')
                row.setdefault('Structure', '趨勢空頭追擊')
            elif lane == 'RANGE':
                row['Direction'] = '做多(Long)'
                row['Regime'] = '區間盤整'
                row.setdefault('Structure', '盤整均值回歸')
            else:
                row['Direction'] = '做多(Long)'
                row.setdefault('Regime', '趨勢多頭')
                row.setdefault('Structure', '趨勢多頭攻堅')
            row.setdefault('AI_Proba', max(0.5, float(item.get('hit_rate', 0.5) or 0.5)))
            row.setdefault('Realized_EV', float(item.get('oot_ev', 0.0) or 0.0))
            row.setdefault('Kelly_Pos', float(PARAMS.get('DIRECTIONAL_SYNTHETIC_KELLY', 0.03)))
            row.setdefault('Health', 'KEEP')
            row.setdefault('Weighted_Buy_Score', trigger + 0.25)
            row.setdefault('Weighted_Sell_Score', max(0.0, trigger - 0.75))
            row.setdefault('Score_Gap', max(0.1, float(row['Weighted_Buy_Score']) - float(row['Weighted_Sell_Score'])))
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

