# ==========================================
# 策略模組化工廠（升級整合版）
# ==========================================
import os
import joblib
import pandas as pd

AI_MODELS = {}
SELECTED_FEATURES = []

try:
    if os.path.exists("models/selected_features.pkl"):
        SELECTED_FEATURES = joblib.load("models/selected_features.pkl")
        for regime in ["趨勢多頭", "區間盤整", "趨勢空頭"]:
            model_path = f"models/model_{regime}.pkl"
            if os.path.exists(model_path):
                AI_MODELS[regime] = joblib.load(model_path)
        print("🤖 [戰術背包] 已成功掛載 AI 機器學習預測模型！")
    else:
        print("⚠️ [戰術背包] 找不到特徵檔，將使用傳統固定資金倍數。")
except Exception as e:
    print(f"⚠️ [戰術背包] AI 模型載入失敗 ({e})，降級為傳統模式。")


def _extract_row_value(latest_row, key, default=0.0):
    try:
        if latest_row is None:
            return default
        val = latest_row.get(key, default)
        if pd.isna(val):
            return default
        return float(val)
    except Exception:
        return default


def evaluate_ai_signal(latest_row, regime, strategy_config):
    """
    統一的 AI 火控審核中心。
    除了模型勝率，還把樣本數 / 訊號信心 / 真實EV 一起納入。
    """
    if latest_row is None:
        return 0.0

    base_mult = float(strategy_config.get("Multiplier", 1.0))
    min_threshold = float(strategy_config.get("Min_Proba", 0.5))
    strat_name = strategy_config.get("Name", "策略")

    # 先從 latest_row 取得新版 screening / pipeline 相關欄位
    realized_ev = _extract_row_value(latest_row, "Realized_EV", 0.0)
    signal_conf = _extract_row_value(latest_row, "訊號信心分數(%)", 50.0) / 100.0
    sample_size = _extract_row_value(latest_row, "歷史訊號樣本數", 0.0)

    # 預設以 signal_conf 當弱備援
    proba = max(0.01, min(0.99, signal_conf))

    if AI_MODELS and regime in AI_MODELS and AI_MODELS[regime] is not None:
        try:
            from .screening import extract_ai_features
            features_dict = extract_ai_features(latest_row)

            if SELECTED_FEATURES:
                X_input = pd.DataFrame([{f: features_dict.get(f, 0) for f in SELECTED_FEATURES}])
            else:
                X_input = pd.DataFrame([features_dict])

            proba = float(AI_MODELS[regime].predict_proba(X_input)[0][1])
        except Exception as e:
            print(f"⚠️ [{strat_name}] AI 測距失敗，改用訊號信心備援 ({e})")
            proba = max(0.01, min(0.99, signal_conf))
    else:
        print(f"⚠️ 找不到 {regime} 專屬大腦，降級使用訊號信心 / 傳統倍數。")

    # 樣本數調整：樣本太少，自動保守
    if sample_size < 8:
        proba = 0.5 + (proba - 0.5) * 0.4
    elif sample_size < 15:
        proba = 0.5 + (proba - 0.5) * 0.7

    print(
        f"🤖 [{strat_name}] 狙擊鏡測距 | 預測勝率: {proba:.1%} | "
        f"門檻: {min_threshold:.1%} | EV: {realized_ev:.3f} | 樣本數: {int(sample_size)}"
    )

    # 生死門審判
    if proba < min_threshold:
        print(f"🛑 [物理阻斷] 勝率未達標 ({proba:.1%} < {min_threshold:.1%}) ➔ 取消進場！")
        return 0.0

    if realized_ev <= 0:
        print(f"🛑 [物理阻斷] 真實 EV 不為正 ({realized_ev:.3f}) ➔ 取消進場！")
        return 0.0

    # 綜合倍率：模型勝率 * 基礎倍率，再乘上 EV 與樣本數保守加權
    ev_boost = 1.0
    if realized_ev > 1.5:
        ev_boost = 1.15
    elif realized_ev > 0.5:
        ev_boost = 1.05

    sample_boost = 1.0
    if sample_size >= 20:
        sample_boost = 1.10
    elif sample_size >= 10:
        sample_boost = 1.03

    final_multiplier = proba * (base_mult * 2) * ev_boost * sample_boost
    final_multiplier = max(0.0, min(final_multiplier, 2.5))

    print(f"✅ [核准開火] 勝率合格，授權資金放大倍數: {final_multiplier:.2f}x")
    return final_multiplier


class BaseStrategy:
    def __init__(self):
        self.strategy_name = "傳統波段策略"
        self.base_mult = 1.0
        self.min_proba = 0.50

    def get_conviction_multiplier(self, latest_row=None, regime="未知"):
        config = {
            "Name": self.strategy_name,
            "Multiplier": self.base_mult,
            "Min_Proba": self.min_proba,
        }
        return evaluate_ai_signal(latest_row, regime, config)

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params["SL_MIN_PCT"], min(volatility_pct, sys_params["SL_MAX_PCT"]))
        tp = sys_params["TP_TREND_PCT"] if (trend_is_with_me and adx_is_strong) else sys_params["TP_BASE_PCT"]
        ignore_tp = entry_score >= 8
        return sl, tp, ignore_tp


class TrendBreakoutStrategy(BaseStrategy):
    def __init__(self):
        super().__init__()
        self.strategy_name = "趨勢突破"
        self.base_mult = 1.2
        self.min_proba = 0.55


class MeanReversionStrategy(BaseStrategy):
    def __init__(self):
        super().__init__()
        self.strategy_name = "均值回歸"
        self.base_mult = 0.9
        self.min_proba = 0.53

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params["SL_MIN_PCT"], min(volatility_pct * 0.8, sys_params["SL_MAX_PCT"]))
        tp = max(sys_params["TP_BASE_PCT"] * 0.7, 0.05)
        ignore_tp = False
        return sl, tp, ignore_tp


class DefensiveStrategy(BaseStrategy):
    def __init__(self):
        super().__init__()
        self.strategy_name = "防禦反擊"
        self.base_mult = 0.7
        self.min_proba = 0.58

    def get_exit_rules(self, sys_params, volatility_pct, trend_is_with_me, adx_is_strong, entry_score):
        sl = max(sys_params["SL_MIN_PCT"], min(volatility_pct * 0.7, sys_params["SL_MAX_PCT"]))
        tp = max(sys_params["TP_BASE_PCT"] * 0.6, 0.04)
        ignore_tp = False
        return sl, tp, ignore_tp


def get_active_strategy(setup_tag):
    tag = str(setup_tag)

    if "空" in tag:
        return DefensiveStrategy()

    if "多" in tag:
        return TrendBreakoutStrategy()

    return MeanReversionStrategy()