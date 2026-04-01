# ==========================================
# 策略模組化工廠 (Strategy Factory) - 職業法人版
# 實裝 4 大核心邏輯：趨勢、均值回歸、突破、籌碼因子
# ==========================================

class BaseStrategy:
    """【基礎傳統策略】(父類別) 中規中矩的預設邏輯"""
    def __init__(self):
        self.strategy_name = "傳統波段策略"

    def get_conviction_multiplier(self):
        return 1.0

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params['SL_MIN_PCT'], min(volatility_pct, sys_params['SL_MAX_PCT']))
        tp = sys_params['TP_TREND_PCT'] if (trend_is_with_me and adx_is_strong) else sys_params['TP_BASE_PCT']
        ignore_tp = entry_score >= 3
        return sl, tp, ignore_tp

# 🟢 1. 趨勢跟隨 (Trend Following)
class TrendStrategy(BaseStrategy):
    """【趨勢策略】牛市超強，順勢而為，讓利潤奔跑"""
    def __init__(self):
        self.strategy_name = "📈 趨勢跟隨策略 (Trend)"

    def get_conviction_multiplier(self):
        return 1.0 # 標準資金

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        # 趨勢策略核心：給予較寬的停損容錯，並且絕對不預設高點 (ignore_tp=True)
        sl = max(sys_params['SL_MIN_PCT'] * 1.5, min(volatility_pct, sys_params['SL_MAX_PCT'])) 
        tp = sys_params['TP_TREND_PCT']
        ignore_tp = True 
        return sl, tp, ignore_tp

# 🔴 2. 均值回歸 (Mean Reversion)
class MeanReversionStrategy(BaseStrategy):
    """【均值回歸策略】震盪盤打乒乓球，布林通道/RSI超賣抄底"""
    def __init__(self):
        self.strategy_name = "🏓 均值回歸策略 (Reversion)"

    def get_conviction_multiplier(self):
        return 0.8 # 逆勢接刀風險較高，資金降載

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        # 均值回歸核心：打帶跑！停損極緊，碰到目標價立刻獲利了結 (ignore_tp=False)
        sl = sys_params['SL_MIN_PCT'] 
        tp = sys_params['TP_BASE_PCT'] 
        ignore_tp = False 
        return sl, tp, ignore_tp

# 🟡 3. 動能突破 (Breakout)
class BreakoutStrategy(BaseStrategy):
    """【動能突破策略】帶量突破整理區間，追求爆發力"""
    def __init__(self):
        self.strategy_name = "🚀 動能突破策略 (Breakout)"

    def get_conviction_multiplier(self):
        return 1.2 # 突破瞬間勝率極高，資金放大重壓

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        # 突破策略核心：防守要嚴格(假突破馬上停損)，看對了就死咬不放
        sl = sys_params['SL_MIN_PCT'] 
        tp = sys_params['TP_TREND_PCT']
        ignore_tp = True 
        return sl, tp, ignore_tp

# 🔵 4. 籌碼因子 (Chip / Factor)
class ChipFactorStrategy(BaseStrategy):
    """【法人籌碼策略】主力大戶進駐，基本面/籌碼面雙重護航"""
    def __init__(self):
        self.strategy_name = "🐋 主力籌碼策略 (Chip/Factor)"

    def get_conviction_multiplier(self):
        return 1.5 # 最高信仰！法人用真金白銀砸出來的訊號，資金開到 1.5 倍

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        # 籌碼策略核心：跟著主力吃大波段，容忍洗盤
        sl = max(sys_params['SL_MIN_PCT'] * 2.0, sys_params['SL_MAX_PCT']) # 停損最寬，防洗盤
        tp = sys_params['TP_TREND_PCT']
        ignore_tp = (adx_is_strong) # 如果動能強就跟著主力抱到底
        return sl, tp, ignore_tp

# ==========================================
# 策略派發中心 (Router)
# 根據大腦傳來的 tag，自動分發對應的策略模組
# ==========================================
def get_active_strategy(setup_tag):
    tag = setup_tag.upper()
    
    # 1. 判斷是否為「突破」
    if any(k in tag for k in ["BREAKOUT", "突破", "創高", "點火"]):
        return BreakoutStrategy()
        
    # 2. 判斷是否為「籌碼/基本面」
    elif any(k in tag for k in ["CHIP", "籌碼", "法人", "主力", "FACTOR"]):
        return ChipFactorStrategy()
        
    # 3. 判斷是否為「均值回歸/逆勢」
    elif any(k in tag for k in ["REVERSAL", "均值", "布林", "RSI", "抄底", "乖離"]):
        return MeanReversionStrategy()
        
    # 4. 判斷是否為「趨勢跟隨」
    elif any(k in tag for k in ["TREND", "趨勢", "MA", "均線"]):
        return TrendStrategy()
        
    # 5. 預設傳統策略
    else:
        return BaseStrategy()