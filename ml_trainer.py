import itertools
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit


# ==========================================
# 🧩 模組 1：Alpha 武器海關審查
# ==========================================
def evaluate_alpha_full(signal, future_return):
    """檢驗單一把武器的期望值與穩定性"""
    df = pd.DataFrame({
        "signal": pd.to_numeric(signal, errors="coerce"),
        "ret": pd.to_numeric(future_return, errors="coerce"),
    }).dropna()

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
        expectancy = 0.0
    else:
        expectancy = wins.mean() * len(wins) / len(df) + losses.mean() * len(losses) / len(df)

    consistency = (returns > 0).mean()
    score = (expectancy * 0.5) + (avg_return * 0.3) + (hit_rate * 0.1) + (consistency * 0.1)

    return {
        "expectancy": float(expectancy),
        "avg_return": float(avg_return),
        "hit_rate": float(hit_rate),
        "consistency": float(consistency),
        "score": float(score),
    }


# ==========================================
# 🧩 模組 2：Walk-Forward 壓力測試模組（報酬導向）
# ==========================================
def walk_forward_analysis(X, y, target_return):
    tscv = TimeSeriesSplit(n_splits=5)
    results = []

    for train_idx, test_idx in tscv.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        ret_test = target_return.iloc[test_idx]

        if len(pd.Series(y_train).unique()) < 2 or len(pd.Series(y_test).unique()) < 2:
            continue

        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=5,
            random_state=42,
            class_weight="balanced",
        )
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        pred_proba = model.predict_proba(X_test)[:, 1]

        # 只在 pred=1 時視為進場，否則報酬=0
        strategy_returns = np.where(pred == 1, ret_test.values, 0.0)

        hit_rate = float(np.mean(strategy_returns > 0))
        avg_return = float(np.mean(strategy_returns))
        pred_accuracy = float(np.mean(pred == y_test))

        gross_profit = strategy_returns[strategy_returns > 0].sum() if np.any(strategy_returns > 0) else 0.0
        gross_loss = abs(strategy_returns[strategy_returns < 0].sum()) if np.any(strategy_returns < 0) else 0.0
        profit_factor = float(gross_profit / gross_loss) if gross_loss > 0 else 99.9

        if np.std(strategy_returns) > 0:
            sharpe_like = float(np.mean(strategy_returns) / np.std(strategy_returns))
        else:
            sharpe_like = 0.0

        coverage = float(np.mean(pred == 1))

        results.append({
            "hit_rate": hit_rate,
            "return": avg_return,
            "profit_factor": profit_factor,
            "pred_accuracy": pred_accuracy,
            "sharpe_like": sharpe_like,
            "coverage": coverage,
            "mean_proba": float(np.mean(pred_proba)),
        })

    return results


def evaluate_stability(results):
    if not results:
        return {
            "ret_mean": 0.0,
            "consistency": 0.0,
            "pf_mean": 0.0,
            "sharpe_mean": 0.0,
            "coverage_mean": 0.0,
        }

    returns = [r["return"] for r in results]
    pfs = [r["profit_factor"] for r in results]
    sharpes = [r["sharpe_like"] for r in results]
    coverages = [r["coverage"] for r in results]

    return {
        "ret_mean": float(np.mean(returns)),
        "consistency": float(np.mean([r > 0 for r in returns])),
        "pf_mean": float(np.mean(pfs)),
        "sharpe_mean": float(np.mean(sharpes)),
        "coverage_mean": float(np.mean(coverages)),
    }


# ==========================================
# 🧠 主幹：AI 大腦鍛造程序
# ==========================================
def train_models():
    print("🧠 [精神時光屋] 啟動 AI 兵工廠（報酬導向強化版）...")

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
    except Exception as e:
        print(f"   ⚠️ 漂移檢測模組異常，跳過檢驗: {e}")

    if "Date" in df.columns:
        df = df.sort_values("Date").reset_index(drop=True)

    # 清理關鍵欄位
    if "Target_Return" in df.columns:
        df["Target_Return"] = pd.to_numeric(df["Target_Return"], errors="coerce").fillna(0.0)
    else:
        df["Target_Return"] = np.where(df["Label_Y"] == 1, 0.05, -0.05)

    drop_cols = ["Ticker", "Date", "Setup", "Regime", "Label_Y", "Target_Return", "Stop_Hit", "Hold_Days"]
    all_features = [c for c in df.columns if c not in drop_cols]

    old_features = []
    if os.path.exists("models/selected_features.pkl"):
        try:
            old_features = joblib.load("models/selected_features.pkl")
            print(f"📦 [記憶讀取] 找到舊背包，發現 {len(old_features)} 把歷史精銳武器。")
        except Exception as e:
            print(f"⚠️ [記憶讀取] 無法解析舊背包，將以全新狀態啟動: {e}")

    all_features = list(dict.fromkeys(all_features + old_features))

    # 自動重鑄歷史組合武器
    for feature in all_features:
        if "_X_" in feature and feature not in df.columns:
            parts = feature.split("_X_")
            if all(p in df.columns for p in parts):
                temp_signal = pd.to_numeric(df[parts[0]], errors="coerce").fillna(0)
                for part in parts[1:]:
                    temp_signal = temp_signal * pd.to_numeric(df[part], errors="coerce").fillna(0)
                df[feature] = temp_signal

    all_features = [f for f in all_features if f in df.columns]
    print(f"⚔️ [軍火庫整編] 新舊武器庫整編完畢，今日送審武器總數：{len(all_features)} 把。")

    future_return = pd.to_numeric(df["Target_Return"], errors="coerce").fillna(0)
    print(f"\n🕵️‍♂️ [海關審查] 正在檢驗 {len(all_features)} 把候選武器的期望值...")
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
            print(f"   ✅ [保留] {col}: EV={result['expectancy']:.3f} | 穩定度={result['consistency']:.0%}")
        else:
            print(f"   🗑️ [銷毀] {col}: EV為負或不穩定 (EV={result['expectancy']:.3f})")

    if not qualified_features:
        print("⚠️ 沒有任何武器通過嚴格審查！系統強制保留所有武器以維持運作。")
        qualified_features = all_features.copy()

    # ==========================================
    # 🔥 連擊武器研發中心
    # ==========================================
    print(f"\n⚔️ 啟動連擊武器研發：正在測試 {len(qualified_features)} 把及格武器的交叉組合...")
    combo_features = []

    # 限制候選數量，避免組合爆炸
    seed_features = qualified_features[:12]

    for r in [2, 3]:
        for combo_tuple in itertools.combinations(seed_features, r):
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
                print(f"   🔥 [最強連擊] {combo_name}: EV={result['expectancy']:.3f} | 穩定={result['consistency']:.0%}")

    qualified_features = list(dict.fromkeys(qualified_features + combo_features))
    print(f"📦 連擊研發完畢！額外新增了 {len(combo_features)} 把組合技武器。")

    joblib.dump(qualified_features, "models/selected_features.pkl")
    print(f"\n📦 特徵審查完畢！已將 {len(qualified_features)} 把精銳武器裝載至戰術背包！")

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
        y = pd.to_numeric(regime_df["Label_Y"], errors="coerce").fillna(0).astype(int)
        target_return = pd.to_numeric(regime_df["Target_Return"], errors="coerce").fillna(0.0)

        X = X.replace([np.inf, -np.inf], np.nan).fillna(0)
        if len(pd.Series(y).unique()) < 2:
            print("⚠️ 標籤只有單一類別，跳過。")
            continue

        wf_results = walk_forward_analysis(X, y, target_return)
        stability = evaluate_stability(wf_results)
        print(
            f"   ► [大腦品管] 平均期望報酬: {stability['ret_mean']:.4f} | "
            f"獲利一致性: {stability['consistency']:.1%} | "
            f"PF: {stability['pf_mean']:.2f} | "
            f"SharpeLike: {stability['sharpe_mean']:.2f}"
        )

        model_path = f"models/model_{regime}.pkl"
        if (
            stability["ret_mean"] > 0
            and stability["consistency"] >= 0.60
            and stability["pf_mean"] >= 1.05
        ):
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
