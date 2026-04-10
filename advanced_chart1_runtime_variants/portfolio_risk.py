import pandas as pd
from .config import PARAMS
from .sector_classifier import get_stock_sector


def _safe_float(x, default=0.0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def apply_portfolio_risk(df_report: pd.DataFrame, total_capital=None, p=PARAMS):
    """
    對決策桌做投資組合層級風控：
    1. 單一產業最多幾檔
    2. 單一產業最大資金占比
    3. 單日總風險上限
    4. 多/空方向集中度限制
    5. 單筆倉位上限
    """
    if df_report is None or df_report.empty:
        return pd.DataFrame()

    if total_capital is None:
        total_capital = float(p.get("TOTAL_BUDGET", 10_000_000))

    max_sector_positions = int(p.get("PORT_MAX_SECTOR_POSITIONS", 2))
    max_sector_alloc = float(p.get("PORT_MAX_SECTOR_ALLOC", 0.35))
    max_total_alloc = float(p.get("PORT_MAX_TOTAL_ALLOC", 0.60))
    max_direction_alloc = float(p.get("PORT_MAX_DIRECTION_ALLOC", 0.45))
    max_single_pos = float(p.get("PORT_MAX_SINGLE_POS", 0.12))
    min_position = float(p.get("PORT_MIN_POSITION", 0.01))

    working = df_report.copy().reset_index(drop=True)
    if "Score" in working.columns:
        working = working.sort_values(
            ["Kelly_Pos", "Score", "Score_Gap", "AI_Proba", "Hist_WinRate", "Sample_Size"],
            ascending=False
        ).reset_index(drop=True)

    selected_rows = []
    sector_alloc_map = {}
    sector_count_map = {}
    direction_alloc_map = {"LONG": 0.0, "SHORT": 0.0}
    total_alloc = 0.0

    for _, row in working.iterrows():
        ticker = row["Ticker"]
        direction_raw = str(row.get("Direction", ""))
        direction_key = "SHORT" if ("空" in direction_raw or "Short" in direction_raw) else "LONG"
        sector = get_stock_sector(ticker)

        raw_kelly = _safe_float(row.get("Kelly_Pos", 0.0), 0.0)
        requested_alloc = max(0.0, min(max_single_pos, raw_kelly))

        if requested_alloc < min_position:
            continue

        if sector_count_map.get(sector, 0) >= max_sector_positions:
            continue

        if total_alloc >= max_total_alloc:
            break

        remaining_total = max(0.0, max_total_alloc - total_alloc)
        remaining_sector = max(0.0, max_sector_alloc - sector_alloc_map.get(sector, 0.0))
        remaining_direction = max(0.0, max_direction_alloc - direction_alloc_map.get(direction_key, 0.0))

        final_alloc = min(requested_alloc, remaining_total, remaining_sector, remaining_direction)

        if final_alloc < min_position:
            continue

        row = row.copy()
        row["Sector"] = sector
        row["Direction_Bucket"] = direction_key
        row["PreRisk_Kelly"] = raw_kelly
        row["Kelly_Pos"] = round(final_alloc, 4)
        row["Risk_Adjusted"] = 1 if abs(final_alloc - raw_kelly) > 1e-9 else 0
        row["Target_Amount"] = round(total_capital * final_alloc, 2)

        selected_rows.append(row)

        total_alloc += final_alloc
        sector_alloc_map[sector] = sector_alloc_map.get(sector, 0.0) + final_alloc
        sector_count_map[sector] = sector_count_map.get(sector, 0) + 1
        direction_alloc_map[direction_key] = direction_alloc_map.get(direction_key, 0.0) + final_alloc

    if not selected_rows:
        return working.iloc[0:0].copy()

    result = pd.DataFrame(selected_rows)
    result.sort_values(
        ["Kelly_Pos", "Score", "Score_Gap", "AI_Proba", "Hist_WinRate", "Sample_Size"],
        ascending=False,
        inplace=True
    )
    result.reset_index(drop=True, inplace=True)
    return result