# ==========================================
# 策略模組化工廠 (Strategy Factory) - AI 賦能版 🧠
# 實裝 4 大核心邏輯 + 機器學習動態資金部位控制
# ==========================================
import joblib
import pandas as pd
import os

# 🌟 啟動時自動載入 AI 大腦與特徵名單
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
    """【基礎傳統策略】(父類別) 中規中矩的預設邏輯"""
    def __init__(self):
        self.strategy_name = "傳統波段策略"

    def get_dynamic_multiplier(self, base_mult, latest_row, regime):
        """🧠 AI 勝率預測引擎：將機率轉換為資金倍數"""
        if not AI_MODELS or regime not in AI_MODELS or not SELECTED_FEATURES or latest_row is None:
            return base_mult

        try:
            # 1. 準備 AI 測驗卷 (現場計算特徵)
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

            # 2. 只挑選 AI 認識的黃金特徵組合
            X_input = pd.DataFrame([{f: features.get(f, 0) for f in SELECTED_FEATURES}])

            # 3. 呼叫 AI 預測勝率機率 (proba)
            model = AI_MODELS[regime]
            proba = model.predict_proba(X_input)[0][1] # 取出獲利機率

            # 4. 嚴格控管：勝率低於 50% 強制取消進場
            if proba < 0.50:
                print(f"🛑 [AI 阻斷] 偵測到高風險騙線 (勝率: {proba*100:.1f}%) ➔ 取消進場！")
                return 0.0

            # 5. 勝率轉換資金 (Dynamic Position Sizing)
            ai_mult = proba * 2.0
            print(f"🧠 [AI 授權] 預測勝率: {proba*100:.1f}% ➔ 核准資金倍數: {ai_mult:.2f}x")
            return ai_mult

        except Exception as e:
            # 回測時或出錯時，返回預設倍數
            return base_mult

    def get_conviction_multiplier(self, latest_row=None, regime="未知"):
        """預設倍數為 1.0"""
        return self.get_dynamic_multiplier(1.0, latest_row, regime)

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params['SL_MIN_PCT'], min(volatility_pct, sys_params['SL_MAX_PCT']))
        tp = sys_params['TP_TREND_PCT'] if (trend_is_with_me and adx_is_strong) else sys_params['TP_BASE_PCT']
        ignore_tp = entry_score >= 3
        return sl, tp, ignore_tp

# 🟢 1. 趨勢跟隨 (Trend Following)
class TrendStrategy(BaseStrategy):
    def __init__(self):
        self.strategy_name = "📈 趨勢跟隨策略 (Trend)"
    def get_conviction_multiplier(self, latest_row=None, regime="未知"):
        return self.get_dynamic_multiplier(1.0, latest_row, regime)
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params['SL_MIN_PCT'] * 1.5, min(volatility_pct, sys_params['SL_MAX_PCT'])) 
        tp = sys_params['TP_TREND_PCT']
        ignore_tp = True 
        return sl, tp, ignore_tp

# 🔴 2. 均值回歸 (Mean Reversion)
class MeanReversionStrategy(BaseStrategy):
    def __init__(self):
        self.strategy_name = "🏓 均值回歸策略 (Reversion)"
    def get_conviction_multiplier(self, latest_row=None, regime="未知"):
        return self.get_dynamic_multiplier(0.8, latest_row, regime)
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = sys_params['SL_MIN_PCT'] 
        tp = sys_params['TP_BASE_PCT'] 
        ignore_tp = False 
        return sl, tp, ignore_tp

# 🟡 3. 動能突破 (Breakout)
class BreakoutStrategy(BaseStrategy):
    def __init__(self):
        self.strategy_name = "🚀 動能突破策略 (Breakout)"
    def get_conviction_multiplier(self, latest_row=None, regime="未知"):
        return self.get_dynamic_multiplier(1.2, latest_row, regime)
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = sys_params['SL_MIN_PCT'] 
        tp = sys_params['TP_TREND_PCT']
        ignore_tp = True 
        return sl, tp, ignore_tp

# 🔵 4. 籌碼因子 (Chip / Factor)
class ChipFactorStrategy(BaseStrategy):
    def __init__(self):
        self.strategy_name = "🐋 主力籌碼策略 (Chip/Factor)"
    def get_conviction_multiplier(self, latest_row=None, regime="未知"):
        return self.get_dynamic_multiplier(1.5, latest_row, regime)
    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params['SL_MIN_PCT'] * 2.0, sys_params['SL_MAX_PCT']) 
        tp = sys_params['TP_TREND_PCT']
        ignore_tp = (adx_is_strong) 
        return sl, tp, ignore_tp

# ==========================================
# 策略派發中心 (Router)
# ==========================================
def get_active_strategy(setup_tag):
    tag = setup_tag.upper()
    if any(k in tag for k in ["BREAKOUT", "突破", "創高", "點火"]):
        return BreakoutStrategy()
    elif any(k in tag for k in ["CHIP", "籌碼", "法人", "主力", "FACTOR"]):
        return ChipFactorStrategy()
    elif any(k in tag for k in ["REVERSAL", "均值", "布林", "RSI", "抄底", "乖離"]):
        return MeanReversionStrategy()
    elif any(k in tag for k in ["TREND", "趨勢", "MA", "均線"]):
        return TrendStrategy()
    else:
        return BaseStrategy()