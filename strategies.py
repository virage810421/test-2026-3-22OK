# ==========================================
# 策略模組化工廠 (Strategy Factory) - AI 賦能版 🧠
# 實裝 5 大核心邏輯 (含全新：🎯黃金狙擊武器) + 動態資金控管
# ==========================================
import joblib
import pandas as pd
import os

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

class BaseStrategy:
    def __init__(self):
        self.strategy_name = "傳統波段策略"

    # 🌟 優化：加入 min_proba 參數，讓不同武器能設定自己的 AI 門檻
    def get_dynamic_multiplier(self, base_mult, latest_row, regime, min_proba=0.50):
        if not AI_MODELS or regime not in AI_MODELS or not SELECTED_FEATURES or latest_row is None:
            return base_mult

        try:
            features = {
                'RSI': latest_row.get('RSI', 50),
                'MACD_Hist': latest_row.get('MACD_Hist', 0),
                'ADX': latest_row.get('ADX14', 0),
                'Foreign_Net': latest_row.get('Foreign_Net', 0),
                'Trust_Net': latest_row.get('Trust_Net', 0)
            }
            ma20 = latest_row.get('MA20', 0)
            features['BB_Width'] = (latest_row['BB_Upper'] - latest_row['BB_Lower']) / ma20 if ma20 > 0 else 0
            vol_ma20 = latest_row.get('Vol_MA20', 0)
            features['Volume_Ratio'] = latest_row['Volume'] / (vol_ma20 + 1) if vol_ma20 > 0 else 1

            X_input = pd.DataFrame([{f: features.get(f, 0) for f in SELECTED_FEATURES}])
            proba = AI_MODELS[regime].predict_proba(X_input)[0][1]

            # 嚴格控管：勝率低於武器專屬門檻，強制取消進場
            if proba < min_proba:
                print(f"🛑 [{self.strategy_name} 阻斷] 勝率未達標 ({proba*100:.1f}% < {min_proba*100:.0f}%) ➔ 取消進場！")
                return 0.0

            ai_mult = proba * (base_mult * 2) 
            print(f"🧠 [{self.strategy_name} 授權] 預測勝率: {proba*100:.1f}% ➔ 核准倍數: {ai_mult:.2f}x")
            return ai_mult
        except Exception as e:
            return base_mult

    def get_conviction_multiplier(self, latest_row=None, regime="未知"):
        return self.get_dynamic_multiplier(1.0, latest_row, regime)

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params['SL_MIN_PCT'], min(volatility_pct, sys_params['SL_MAX_PCT']))
        tp = sys_params['TP_TREND_PCT'] if (trend_is_with_me and adx_is_strong) else sys_params['TP_BASE_PCT']
        ignore_tp = entry_score >= 3
        return sl, tp, ignore_tp

# 🟢 1. 趨勢跟隨 (Trend)
class TrendStrategy(BaseStrategy):
    def __init__(self): self.strategy_name = "📈 趨勢跟隨策略"
    def get_conviction_multiplier(self, latest_row=None, regime="未知"): return self.get_dynamic_multiplier(1.0, latest_row, regime)
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        return max(sys_params['SL_MIN_PCT'] * 1.5, min(volatility_pct, sys_params['SL_MAX_PCT'])), sys_params['TP_TREND_PCT'], True 

# 🔴 2. 均值回歸 (Reversion)
class MeanReversionStrategy(BaseStrategy):
    def __init__(self): self.strategy_name = "🏓 均值回歸策略"
    def get_conviction_multiplier(self, latest_row=None, regime="未知"): return self.get_dynamic_multiplier(0.8, latest_row, regime)
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        return sys_params['SL_MIN_PCT'], sys_params['TP_BASE_PCT'], False 

# 🟡 3. 動能突破 (Breakout)
class BreakoutStrategy(BaseStrategy):
    def __init__(self): self.strategy_name = "🚀 動能突破策略"
    def get_conviction_multiplier(self, latest_row=None, regime="未知"): return self.get_dynamic_multiplier(1.2, latest_row, regime)
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        return sys_params['SL_MIN_PCT'], sys_params['TP_TREND_PCT'], True 

# 🔵 4. 籌碼因子 (Chip)
class ChipFactorStrategy(BaseStrategy):
    def __init__(self): self.strategy_name = "🐋 主力籌碼策略"
    def get_conviction_multiplier(self, latest_row=None, regime="未知"): return self.get_dynamic_multiplier(1.5, latest_row, regime)
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        return max(sys_params['SL_MIN_PCT'] * 2.0, sys_params['SL_MAX_PCT']), sys_params['TP_TREND_PCT'], adx_is_strong 

# 🏴‍☠️ 5. 新增：黃金狙擊武器 (Sniper) 
class SniperStrategy(BaseStrategy):
    def __init__(self): self.strategy_name = "🎯 黃金狙擊特種部隊"
    
    def get_conviction_multiplier(self, latest_row=None, regime="未知"):
        # 🌟 狙擊手極度嚴格：勝率不到 60% 絕對不開槍！一旦開槍，基礎資金放大 2 倍！
        return self.get_dynamic_multiplier(base_mult=2.0, latest_row=latest_row, regime=regime, min_proba=0.60)
        
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        # 🌟 狙擊手出場紀律：停損極緊 (防禦力極高)，獲利目標看極遠
        sl = sys_params['SL_MIN_PCT'] * 0.8 
        tp = sys_params['TP_TREND_PCT'] * 1.5
        ignore_tp = True 
        return sl, tp, ignore_tp

# ==========================================
# 策略派發中心 (Router)
# ==========================================
def get_active_strategy(setup_tag):
    tag = setup_tag.upper()
    if any(k in tag for k in ["SNIPER", "狙擊"]):  # 🌟 攔截狙擊訊號
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