# ==========================================
# 策略模組化工廠 (Strategy Factory)
# 這裡定義了所有的交易邏輯、資金權重與出場規則
# ==========================================

class BaseStrategy:
    """
    【基礎傳統策略】(父類別)
    所有未被特別定義的陣型，都會預設套用這個中規中矩的邏輯。
    """
    def __init__(self):
        self.strategy_name = "傳統波段策略"

    def get_conviction_multiplier(self):
        """決定資金權重：預設 1.0 倍"""
        return 1.0

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        """
        決定停利損規則
        回傳: (動態停損%, 動態停利%, 是否無視停利(Trailing Stop))
        """
        sl = max(sys_params['SL_MIN_PCT'], min(volatility_pct, sys_params['SL_MAX_PCT']))
        tp = sys_params['TP_TREND_PCT'] if (trend_is_with_me and adx_is_strong) else sys_params['TP_BASE_PCT']
        ignore_tp = entry_score >= 3 # 分數大於 3 啟動移動停損
        return sl, tp, ignore_tp


class TrendStrategy(BaseStrategy):
    """
    【趨勢突破策略】(繼承並覆寫基礎策略)
    針對 "TREND"、"點火" 等陣型。
    """
    def __init__(self):
        self.strategy_name = "趨勢突破策略"

    def get_conviction_multiplier(self):
        """高度信心，資金放大至 1.2 倍"""
        return 1.2 

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        """死咬趨勢模式：無視傳統目標價，純靠移動停損"""
        sl = max(sys_params['SL_MIN_PCT'], min(volatility_pct, sys_params['SL_MAX_PCT']))
        tp = sys_params['TP_TREND_PCT']
        ignore_tp = True 
        return sl, tp, ignore_tp


class ReversalStrategy(BaseStrategy):
    """
    【逆勢反轉策略】(繼承並覆寫基礎策略)
    針對 "REVERSAL"、"抄底" 等陣型。
    """
    def __init__(self):
        self.strategy_name = "逆勢反轉策略"

    def get_conviction_multiplier(self):
        """逆勢摸底，資金降載至 0.7 倍"""
        return 0.7 

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        """打帶跑模式：停損極緊，賺 8% 就強制平倉"""
        sl = sys_params['SL_MIN_PCT']
        tp = 0.08 
        ignore_tp = False 
        return sl, tp, ignore_tp


# ==========================================
# 策略派發中心 (Router)
# 根據大腦傳來的 tag，自動分發對應的策略模組
# ==========================================
def get_active_strategy(setup_tag):
    if "TREND" in setup_tag or "點火" in setup_tag or "倒貨" in setup_tag:
        return TrendStrategy()
    elif "REVERSAL" in setup_tag or "抄底" in setup_tag or "摸頭" in setup_tag:
        return ReversalStrategy()
    else:
        return BaseStrategy()