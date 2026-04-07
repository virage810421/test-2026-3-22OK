import itertools
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit


COST_PER_TRADE = 0.004



def evaluate_alpha_full(signal, future_return):
    """檢驗單一把武器的期望值與穩定性"""
    df = pd.DataFrame({"signal": signal, "ret": future_return}).dropna()

    if len(df) < 30:
        return None

    direction = np.sign(df["signal"])
    actual = np.sign(df["ret"])
    direction = np.where(direction == 0, 1, direction)

    hit_rate = (direction == actual).mean()
    returns = direction * df["ret"]
    avg_return = returns.mean()

    wins = returns[returns > 0]
    losses = returns[returns <= 0]

    if len(wins) == 0 or len(losses) == 0:
        expectancy = 0
    else:
        expectancy = wins.mean() * len(wins) / len(df) + losses.mean() * len(losses) / len(df)

    consistency = (returns > 0).mean()
    score = (expectancy * 0.5) + (avg_return * 0.3) + (hit_rate * 0.1) + (consistency * 0.1)

    return {
        "expectancy": expectancy,
        "avg_return": avg_return,
        "hit_rate": hit_rate,
        "consistency": consistency,
        "score": score,
    }



def walk_forward_analysis(X, y, target_return, cost_per_trade=COST_PER_TRADE):
    tscv = TimeSeriesSplit(n_splits=5)
    results = []

    for train_idx, test_idx in tscv.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        ret_test = target_return.iloc[test_idx]

        if len(y_train.unique()) < 2 or len(y_test.unique()) < 2:
            continue

        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=5,
            random_state=42,
            class_weight="balanced",
        )
        model.fit(X_train, y_train)
        pred = model.predict(X_test)

        strategy_returns = np.where(pred == 1, ret_test.values - cost_per_trade, 0.0)
        hit_rate = float(np.mean(strategy_returns > 0)) if len(strategy_returns) > 0 else 0.0
        avg_return = float(np.mean(strategy_returns)) if len(strategy_returns) > 0 else 0.0

        gains = strategy_returns[strategy_returns > 0].sum()
        losses = abs(strategy_returns[strategy_returns < 0].sum())
        profit_factor = float(gains / losses) if losses > 0 else (99.9 if gains > 0 else 0.0)

        results.append({
            "hit_rate": hit_rate,
            "return": avg_return,
            "profit_factor": profit_factor,
        })

    return results



def evaluate_stability(results):
    if not results:
        return {"ret_mean": 0, "consistency": 0, "pf_mean": 0}

    returns = [r["return"] for r in results]
    pfs = [r.get("profit_factor", 0) for r in results]
    return {
        "ret_mean": float(np.mean(returns)),
        "consistency": float(np.mean([r > 0 for r in returns])),
        "pf_mean": float(np.mean(pfs)),
    }



def train_models():
    print("🧠 [精神時光屋] 啟動 AI 兵工廠 (搭載 Alpha 特徵海關)...")

    dataset_path = "data/ml_training_data.csv"
    if not os.path.exists(dataset_path):
        print(f"❌ 找不到訓練教材 ({dataset_path})！")
        return

    df = pd.read_csv(dataset_path)
    os.makedirs("models", exist_ok=True)

    print("\n🔍 [系統安檢] 啟動 Regime 漂移檢測 (Distribution Drift Check)...")
    try:
        half_idx = len(df) // 2
        past_dist = df["Regime"].iloc[:half_idx].value_counts(normalize=True)
        recent_dist = df["Regime"].iloc[half_idx:].value_counts(normalize=True)

        all_regimes = list(set(past_dist.index.tolist() + recent_dist.index.tolist()))
        past_dist = past_dist.reindex(all_regimes).fillna(0)
        recent_dist = recent_dist.reindex(all_regimes).fillna(0)

        drift_score = (past_dist - recent_dist).abs().sum()

        print(
            f"   ► 歷史生態分布: 多頭({past_dist.get('趨勢多頭', 0):.1%}) | "
            f"空頭({past_dist.get('趨勢空頭', 0):.1%}) | 盤整({past_dist.get('區間盤整', 0):.1%})"
        )
        print(
            f"   ► 近期生態分布: 多頭({recent_dist.get('趨勢多頭', 0):.1%}) | "
            f"空頭({recent_dist.get('趨勢空頭', 0):.1%}) | 盤整({recent_dist.get('區間盤整', 0):.1%})"
        )
        print(f"   ► Regime 分布漂移指數 (Drift Score): {drift_score:.3f}")

        if drift_score < 0.2:
            print("   ✅ 判定：市場結構極度穩定，完美適合 AI 深度學習！")
        elif drift_score < 0.4:
            print("   ⚠️ 判定：市場結構輕微偏移，尚在容許範圍內。")
        else:
            print("   🛑 警告：偵測到嚴重 Regime 漂移 (Drift > 0.4)！近期市場生態已發生劇變！")
            print("   💡 系統建議：請密切關注本次訓練之大腦『獲利一致性』，若過低系統將自動啟動銷毀機制。")

    except Exception as e:
        print(f"   ⚠️ 漂移檢測模組異常，跳過檢驗: {e}")

    if "Date" in df.columns:
        df = df.sort_values("Date").reset_index(drop=True)

    drop_cols = ["Ticker", "Date", "Setup", "Regime", "Label_Y", "Target_Return"]
    all_features = [c for c in df.columns if c not in drop_cols]

    old_features = []
    if os.path.exists("models/selected_features.pkl"):
        try:
            old_features = joblib.load("models/selected_features.pkl")
            print(f"📦 [記憶讀取] 成功尋獲前人遺留的戰術背包，發現 {len(old_features)} 把歷史精銳武器。")
        except Exception as e:
            print(f"⚠️ [記憶讀取] 無法解析舊有背包，將以全新狀態啟動: {e}")

    all_features = list(dict.fromkeys(all_features + old_features))

    for feature in all_features:
        if "_X_" in feature and feature not in df.columns:
            parts = feature.split("_X_")
            if all(p in df.columns for p in parts):
                temp_signal = df[parts[0]].copy()
                for part in parts[1:]:
                    temp_signal = temp_signal * df[part]
                df[feature] = temp_signal

    all_features = [f for f in all_features if f in df.columns]
    print(f"⚔️ [軍火庫整編] 新舊武器庫合併與重鑄完畢，今日送審武器總數：{len(all_features)} 把。")

    if "Target_Return" in df.columns:
        future_return = pd.to_numeric(df["Target_Return"], errors="coerce").fillna(0)
    else:
        future_return = pd.Series(np.where(df["Label_Y"] == 1, 0.05, -0.05), index=df.index)

    print(f"\n🕵️‍♂️ [海關審查] 正在單獨檢驗 {len(all_features)} 把候選武器的期望值...")
    qualified_features = []

    for col in all_features:
        signal = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if signal.nunique() <= 1:
            continue

        result = evaluate_alpha_full(signal, future_return)
        if result is None:
            continue

        if result["expectancy"] > 0 and result["consistency"] >= 0.50:
            qualified_features.append(col)
            print(f"   ✅ [保留] {col}: EV={result['expectancy']:.2%} | 穩定度={result['consistency']:.0%}")
        else:
            print(f"   🗑️ [銷毀] {col}: EV為負或不穩定 (EV={result['expectancy']:.2%})")

    if not qualified_features:
        print("⚠️ 警告：沒有任何武器通過嚴格審查！系統強制保留所有武器以維持運作。")
        qualified_features = all_features.copy()

    print(f"\n⚔️ 啟動連擊武器研發：正在測試 {len(qualified_features)} 把及格武器的交叉組合...")
    combo_features = []

    for r in [2, 3]:
        for combo_tuple in itertools.combinations(qualified_features, r):
            combo_name = "_X_".join(combo_tuple)

            if combo_name in df.columns:
                combo_signal = pd.to_numeric(df[combo_name], errors="coerce").fillna(0)
            else:
                combo_signal = df[list(combo_tuple)].apply(pd.to_numeric, errors="coerce").fillna(0).prod(axis=1)

            active_count = int((combo_signal != 0).sum())
            if active_count < 20:
                continue

            result = evaluate_alpha_full(combo_signal, future_return)
            if result is None:
                continue

            if result["expectancy"] > 0 and result["consistency"] >= 0.60:
                df[combo_name] = combo_signal
                combo_features.append(combo_name)
                print(f"   🔥 [最強連擊] {combo_name}: EV={result['expectancy']:.2%} | 穩定={result['consistency']:.0%}")

    qualified_features = list(dict.fromkeys(qualified_features + combo_features))
    print(f"📦 連擊研發完畢！額外新增了 {len(combo_features)} 把組合技武器。")

    joblib.dump(qualified_features, "models/selected_features.pkl")
    print(f"\n📦 特徵審查完畢！已將 {len(qualified_features)} 把「正期望值精銳武器」裝載至戰術背包！")

    regimes = ["趨勢多頭", "區間盤整", "趨勢空頭"]

    for regime in regimes:
        print("\n" + "=" * 50)
        print(f"⏳ 正在使用精銳武器，訓練【{regime}】專屬大腦...")

        regime_df = df[df["Regime"] == regime].copy()
        if len(regime_df) < 50:
            print("⚠️ 數據過少，跳過。")
            continue

        safe_features = [f for f in qualified_features if f in regime_df.columns]
        if not safe_features:
            print("⚠️ 當前 Regime 沒有可用特徵，跳過。")
            continue

        X = regime_df[safe_features].copy()
        y = regime_df["Label_Y"]
        target_return_regime = pd.to_numeric(regime_df.get("Target_Return", 0), errors="coerce").fillna(0)

        X = X.replace([np.inf, -np.inf], np.nan).fillna(0)
        if len(pd.Series(y).unique()) < 2:
            print("⚠️ 標籤只有單一類別，跳過。")
            continue

        wf_results = walk_forward_analysis(X, y, target_return_regime)
        stability = evaluate_stability(wf_results)
        print(
            f"   ► [大腦品管] 平均期望報酬: {stability['ret_mean']:.4f} | "
            f"獲利一致性: {stability['consistency']:.1%} | 平均 PF: {stability['pf_mean']:.2f}"
        )

        model_path = f"models/model_{regime}.pkl"
        if stability["ret_mean"] > 0 and stability["consistency"] >= 0.60:
            print("   ✅ 檢驗合格！進行最終鍛造...")
            model = RandomForestClassifier(
                n_estimators=200,
                max_depth=7,
                random_state=42,
                class_weight="balanced",
            )
            model.fit(X, y)
            joblib.dump(model, model_path)
        else:
            print("   🛑 檢驗失敗！大腦不穩定，銷毀！")
            if os.path.exists(model_path):
                os.remove(model_path)

    print("\n🎉 精神時光屋結訓！精英武器與合格大腦均已就位！")


if __name__ == "__main__":
    import warnings

    warnings.filterwarnings("ignore")
    train_models()
