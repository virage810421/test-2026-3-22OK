# ==========================================
# 策略模組化工廠 (Strategy Factory) - AI 賦能版 🧠
# 實裝 5 大核心邏輯 (含：🎯黃金狙擊武器) + 統一 AI 火控引擎
# ==========================================
import joblib
import pandas as pd
import os

# ==========================================
# 1. 系統全域變數與模型掛載區
# ==========================================
AI_MODELS = {}
SELECTED_FEATURES = []
try:
    if os.path.exists("models/selected_features.pkl"):
        SELECTED_FEATURES = joblib.load("models/selected_features.pkl")
        for regime in ['趨勢多頭', '區間盤整', '趨勢空頭']:
            model_path = f"models/model_{regime}.pkl"
            if os.path.exists(model_path):
                AI_MODELS[regime] = joblib.load(model_path)
        print("🤖 [戰術背包] 已成功掛載 AI 機器學習預測模型！")
    else:
        print("⚠️ [戰術背包] 找不到特徵檔，將使用傳統固定資金倍數。")
except Exception as e:
    print(f"⚠️ [戰術背包] AI 模型載入失敗 ({e})，降級為傳統模式。")


# ==========================================
# 2. 中央 AI 火控引擎 (唯一運算核心)
# ==========================================
def evaluate_ai_signal(latest_row, regime, strategy_config):
    """
    🎯 統一的 AI 生死門審核中心 (不再與 BaseStrategy 重複運算)
    """
    if not AI_MODELS or regime not in AI_MODELS or AI_MODELS[regime] is None or latest_row is None:
        print(f"⚠️ 找不到 {regime} 專屬大腦，降級使用物理預設倍數。")
        return strategy_config.get('Multiplier', 1.0)

    try:
        # 🌟 局部引入統一晶片，完美避開循環引用死結
        from screening import extract_ai_features
        features_dict = extract_ai_features(latest_row)
        
        # 精準對齊特徵矩陣 (防止實戰欄位跟訓練時不一致)
        if SELECTED_FEATURES:
            X_input = pd.DataFrame([{f: features_dict.get(f, 0) for f in SELECTED_FEATURES}])
        else:
            X_input = pd.DataFrame([features_dict])
            
        # 預測勝率
        proba = AI_MODELS[regime].predict_proba(X_input)[0][1]
        
        min_threshold = strategy_config.get('Min_Proba', 0.5)
        base_mult = strategy_config.get('Multiplier', 1.0)
        strat_name = strategy_config.get('Name', '策略')
        
        print(f"🤖 [{strat_name}] 狙擊鏡測距 | 預測勝率: {proba:.1%} | 武器門檻: {min_threshold:.1%}")
        
        # 生死門邏輯審判
        if proba < min_threshold:
            print(f"🛑 [物理阻斷] 勝率未達標 ({proba:.1%} < {min_threshold:.1%}) ➔ 取消進場！")
            return 0.0
            
        final_multiplier = proba * (base_mult * 2) 
        print(f"✅ [核准開火] 勝率合格！授權資金放大倍數: {final_multiplier:.2f}x")
        return final_multiplier

    except Exception as e:
        print(f"❌ AI 測距儀故障: {e}，強制拉下保險不開火。")
        return 0.0


# ==========================================
# 3. 武器庫藍圖 (Class 只負責裝載「參數」與「出場規則」)
# ==========================================
class BaseStrategy:
    def __init__(self):
        self.strategy_name = "傳統波段策略"
        self.base_mult = 1.0
        self.min_proba = 0.50

    def get_conviction_multiplier(self, latest_row=None, regime="未知"):
        # 🌟 所有武器都統一呼叫外部的中央火控引擎，不再自己算！
        config = {
            'Name': self.strategy_name,
            'Multiplier': self.base_mult,
            'Min_Proba': self.min_proba
        }
        return evaluate_ai_signal(latest_row, regime, config)

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        # 💡 這裡有未使用的變數 (反灰) 是正常的「多型 (Polymorphism)」設計，保留以維持系統介面一致性
        sl = max(sys_params['SL_MIN_PCT'], min(volatility_pct, sys_params['SL_MAX_PCT']))
        tp = sys_params['TP_TREND_PCT'] if (trend_is_with_me and adx_is_strong) else sys_params['TP_BASE_PCT']
        ignore_tp = entry_score >= 3
        return sl, tp, ignore_tp

# 🟢 1. 趨勢跟隨 (Trend)
class TrendStrategy(BaseStrategy):
    def __init__(self): 
        super().__init__()
        self.strategy_name = "📈 趨勢跟隨策略"
        
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        return max(sys_params['SL_MIN_PCT'] * 1.5, min(volatility_pct, sys_params['SL_MAX_PCT'])), sys_params['TP_TREND_PCT'], True 

# 🔴 2. 均值回歸 (Reversion)
class MeanReversionStrategy(BaseStrategy):
    def __init__(self): 
        super().__init__()
        self.strategy_name = "🏓 均值回歸策略"
        self.base_mult = 0.8  # 逆勢接刀風險高，基礎資金降為 0.8 倍
        
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        return sys_params['SL_MIN_PCT'], sys_params['TP_BASE_PCT'], False 

# 🟡 3. 動能突破 (Breakout)
class BreakoutStrategy(BaseStrategy):
    def __init__(self): 
        super().__init__()
        self.strategy_name = "🚀 動能突破策略"
        self.base_mult = 1.2
        
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        return sys_params['SL_MIN_PCT'], sys_params['TP_TREND_PCT'], True 

# 🔵 4. 籌碼因子 (Chip)
class ChipFactorStrategy(BaseStrategy):
    def __init__(self): 
        super().__init__()
        self.strategy_name = "🐋 主力籌碼策略"
        self.base_mult = 1.5
        
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        # 🛠️ BUG 修復：將 max 改為 min，確保停損不會超過系統定義的最大承受極限
        sl = min(sys_params['SL_MIN_PCT'] * 2.0, sys_params['SL_MAX_PCT'])
        return sl, sys_params['TP_TREND_PCT'], adx_is_strong 

# 🏴‍☠️ 5. 黃金狙擊武器 (Sniper) 
class SniperStrategy(BaseStrategy):
    def __init__(self): 
        super().__init__()
        self.strategy_name = "🎯 黃金狙擊特種部隊"
        self.base_mult = 2.0   # 開槍就是重壓 2 倍
        self.min_proba = 0.60  # 門檻極高，勝率 < 60% 絕不開槍
        
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = sys_params['SL_MIN_PCT'] * 0.8 
        tp = sys_params['TP_TREND_PCT'] * 1.5
        return sl, tp, True


# ==========================================
# 4. 策略派發中心 (Router)
# ==========================================
def get_active_strategy(setup_tag):
    """根據偵測到的陣型標籤，自動派發對應口徑的武器"""
    tag = str(setup_tag).upper()
    if any(k in tag for k in ["SNIPER", "狙擊"]):
        return SniperStrategy()
    elif any(k in tag for k in ["BREAKOUT", "突破", "創高", "點火"]):
        return BreakoutStrategy()
    elif any(k in tag for k in ["CHIP", "籌碼", "法人", "主力"]):
        return ChipFactorStrategy()
    elif any(k in tag for k in ["REVERSAL", "均值", "布林", "RSI", "抄底"]):
        return MeanReversionStrategy()
    elif any(k in tag for k in ["TREND", "趨勢", "MA", "均線"]):
        return TrendStrategy()
    else:
        return BaseStrategy()