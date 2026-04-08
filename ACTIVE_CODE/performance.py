import numpy as np
import pandas as pd
import pyodbc
import warnings

warnings.filterwarnings('ignore', category=UserWarning)

from config import PARAMS

DB_CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=localhost;'
    r'DATABASE=股票online;'
    r'Trusted_Connection=yes;'
)


def get_strategy_ev(setup_tag, current_regime=None):
    """
    從 SQL 歷史交易總帳 (trade_history) 中，計算該陣型的真實平均期望值 (EV)。
    優先使用近 60 筆；若樣本不足，再退回全部樣本。
    若 current_regime 有提供，會優先看同 regime 的樣本，但不足時不強制。
    """
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            base_query = """
                SELECT TOP 60 [報酬率(%)], [市場狀態]
                FROM trade_history
                WHERE [進場陣型] = ?
                  AND [報酬率(%)] IS NOT NULL
                ORDER BY [出場時間] DESC
            """
            df = pd.read_sql(base_query, conn, params=(setup_tag,))

            if df.empty:
                return 0.0

            if current_regime and '市場狀態' in df.columns:
                regime_df = df[df['市場狀態'] == current_regime].copy()
                if len(regime_df) >= 8:
                    df = regime_df

            ev = pd.to_numeric(df['報酬率(%)'], errors='coerce').dropna().mean()
            return float(ev) if pd.notna(ev) else 0.0

    except Exception as e:
        print(f"⚠️ 讀取策略期望值 (EV) 失敗: {e}")
        return 0.0


def get_strategy_summary(setup_tag, lookback=60):
    """
    回傳某陣型最近 lookback 筆的真實摘要，方便主流程或報表使用。
    """
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = f"""
                SELECT TOP {int(lookback)}
                    [報酬率(%)], [淨損益金額], [市場狀態], [進場時間], [出場時間]
                FROM trade_history
                WHERE [進場陣型] = ?
                  AND [報酬率(%)] IS NOT NULL
                ORDER BY [出場時間] DESC
            """
            df = pd.read_sql(query, conn, params=(setup_tag,))

        if df.empty:
            return {
                'sample_size': 0,
                'win_rate': 0.0,
                'avg_return': 0.0,
                'profit_factor': 0.0,
                'sharpe_like': 0.0,
                'mdd_like': 0.0,
            }

        df['報酬率(%)'] = pd.to_numeric(df['報酬率(%)'], errors='coerce')
        df['淨損益金額'] = pd.to_numeric(df['淨損益金額'], errors='coerce')
        df = df.dropna(subset=['報酬率(%)', '淨損益金額'])

        if df.empty:
            return {
                'sample_size': 0,
                'win_rate': 0.0,
                'avg_return': 0.0,
                'profit_factor': 0.0,
                'sharpe_like': 0.0,
                'mdd_like': 0.0,
            }

        returns = df['報酬率(%)'].values
        pnl = df['淨損益金額'].values

        gross_profit = pnl[pnl > 0].sum() if len(pnl[pnl > 0]) > 0 else 0.0
        gross_loss = abs(pnl[pnl < 0].sum()) if len(pnl[pnl < 0]) > 0 else 0.0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 99.9

        std_r = np.std(returns)
        sharpe_like = np.mean(returns) / std_r if std_r > 0 else 0.0

        cum = pd.Series(returns).cumsum()
        peak = cum.cummax()
        dd = cum - peak
        mdd_like = abs(dd.min()) if len(dd) > 0 else 0.0

        return {
            'sample_size': int(len(df)),
            'win_rate': float(np.mean(returns > 0)),
            'avg_return': float(np.mean(returns)),
            'profit_factor': float(profit_factor),
            'sharpe_like': float(sharpe_like),
            'mdd_like': float(mdd_like),
        }

    except Exception as e:
        print(f"⚠️ 讀取策略摘要失敗: {e}")
        return {
            'sample_size': 0,
            'win_rate': 0.0,
            'avg_return': 0.0,
            'profit_factor': 0.0,
            'sharpe_like': 0.0,
            'mdd_like': 0.0,
        }


def check_strategy_health(setup_tag, min_trades=10):
    """
    讀取 SQL 中該陣型最近 20 筆真實交易紀錄。
    勝率過低、報酬為負、或報酬波動太差時，阻斷新資金投入。
    """
    try:
        with pyodbc.connect(DB_CONN_STR) as conn:
            query = """
                SELECT TOP 20 [報酬率(%)], [淨損益金額]
                FROM trade_history 
                WHERE [進場陣型] = ?
                  AND [報酬率(%)] IS NOT NULL
                ORDER BY [出場時間] DESC
            """
            df = pd.read_sql(query, conn, params=(setup_tag,))

        if len(df) < min_trades:
            return 'KEEP', f'樣本數不足 ({len(df)}/{min_trades})，熱身中'

        df['報酬率(%)'] = pd.to_numeric(df['報酬率(%)'], errors='coerce')
        df['淨損益金額'] = pd.to_numeric(df['淨損益金額'], errors='coerce')
        df = df.dropna(subset=['報酬率(%)', '淨損益金額'])

        if len(df) < min_trades:
            return 'KEEP', f'有效樣本不足 ({len(df)}/{min_trades})，熱身中'

        ret_array = df['報酬率(%)'].values

        win_rate = np.mean(ret_array > 0)
        avg_ret = np.mean(ret_array)

        std_ret = np.std(ret_array)
        sharpe_like = avg_ret / std_ret if std_ret > 0 else 0.0

        kill_rate = PARAMS.get('LIVE_MONITOR_WIN_RATE', 0.30)
        min_avg_ret = PARAMS.get('LIVE_MONITOR_MIN_AVG_RETURN', -0.20)

        if win_rate < kill_rate:
            return 'KILL', f'近20筆勝率過低 ({win_rate:.1%})'
        if avg_ret < min_avg_ret:
            return 'KILL', f'近20筆平均報酬過低 ({avg_ret:.2f}%)'
        if sharpe_like < -0.2:
            return 'KILL', f'近20筆風險報酬惡化 (SharpeLike={sharpe_like:.2f})'

        recent5 = df.head(5)['報酬率(%)'].values
        if len(recent5) == 5 and np.all(recent5 <= 0):
            return 'KILL', '最近 5 筆全數虧損'

        return 'KEEP', f'正常運作 | 勝率 {win_rate:.1%} | 平均報酬 {avg_ret:.2f}%'

    except Exception as e:
        return 'KEEP', f'健康檢查失敗，保守放行 ({e})'


if __name__ == '__main__':
    test_tags = ['多方進場', '空方進場', 'AI訊號']
    for tag in test_tags:
        ev = get_strategy_ev(tag, None)
        action, note = check_strategy_health(tag)
        summary = get_strategy_summary(tag)
        print('=' * 60)
        print(f'策略: {tag}')
        print(f'EV: {ev:.3f}')
        print(f'健康度: {action} | {note}')
        print(f'摘要: {summary}')
