import json
import os
import shutil
from datetime import datetime
from pathlib import Path


MODELS_DIR = Path("models")
VERSIONS_DIR = MODELS_DIR / "versions"
CURRENT_DIR = MODELS_DIR / "current"
BEST_DIR = MODELS_DIR / "best"
REGISTRY_PATH = MODELS_DIR / "model_registry.json"


def ensure_dirs():
    MODELS_DIR.mkdir(exist_ok=True)
    VERSIONS_DIR.mkdir(exist_ok=True)
    CURRENT_DIR.mkdir(exist_ok=True)
    BEST_DIR.mkdir(exist_ok=True)


def load_registry():
    ensure_dirs()
    if not REGISTRY_PATH.exists():
        return {"versions": [], "current_version": None, "best_version": None}
    try:
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"versions": [], "current_version": None, "best_version": None}
        data.setdefault("versions", [])
        data.setdefault("current_version", None)
        data.setdefault("best_version", None)
        return data
    except Exception:
        return {"versions": [], "current_version": None, "best_version": None}


def save_registry(registry):
    ensure_dirs()
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)


def create_version_tag(prefix="model"):
    return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _copy_if_exists(src: Path, dst: Path):
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def snapshot_current_models(version_tag, metrics=None, note=""):
    """
    將 models 目錄下的現役模型快照成一個版本。
    預設快照：
      - selected_features.pkl
      - model_趨勢多頭.pkl
      - model_區間盤整.pkl
      - model_趨勢空頭.pkl
    """
    ensure_dirs()
    version_dir = VERSIONS_DIR / version_tag
    version_dir.mkdir(parents=True, exist_ok=True)

    tracked_files = [
        "selected_features.pkl",
        "model_趨勢多頭.pkl",
        "model_區間盤整.pkl",
        "model_趨勢空頭.pkl",
    ]

    copied = []
    for filename in tracked_files:
        src = MODELS_DIR / filename
        dst = version_dir / filename
        if src.exists():
            shutil.copy2(src, dst)
            copied.append(filename)

    registry = load_registry()
    entry = {
        "version": version_tag,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "files": copied,
        "metrics": metrics or {},
        "note": note,
    }
    registry["versions"].append(entry)
    registry["current_version"] = version_tag
    save_registry(registry)

    # 同步 current/
    CURRENT_DIR.mkdir(exist_ok=True)
    for filename in tracked_files:
        src = version_dir / filename
        dst = CURRENT_DIR / filename
        _copy_if_exists(src, dst)

    return entry


def promote_best_version(version_tag):
    registry = load_registry()
    registry["best_version"] = version_tag
    save_registry(registry)

    version_dir = VERSIONS_DIR / version_tag
    BEST_DIR.mkdir(exist_ok=True)
    for filename in [
        "selected_features.pkl",
        "model_趨勢多頭.pkl",
        "model_區間盤整.pkl",
        "model_趨勢空頭.pkl",
    ]:
        _copy_if_exists(version_dir / filename, BEST_DIR / filename)


def restore_version(version_tag, target="models"):
    """
    回滾指定版本到 models/ 根目錄
    """
    ensure_dirs()
    version_dir = VERSIONS_DIR / version_tag
    if not version_dir.exists():
        raise FileNotFoundError(f"找不到版本：{version_tag}")

    target_dir = Path(target)
    target_dir.mkdir(exist_ok=True)

    restored = []
    for filename in [
        "selected_features.pkl",
        "model_趨勢多頭.pkl",
        "model_區間盤整.pkl",
        "model_趨勢空頭.pkl",
    ]:
        src = version_dir / filename
        dst = target_dir / filename
        if src.exists():
            shutil.copy2(src, dst)
            restored.append(filename)

    registry = load_registry()
    registry["current_version"] = version_tag
    save_registry(registry)

    return restored


def get_best_version_entry():
    registry = load_registry()
    best_version = registry.get("best_version")
    if not best_version:
        return None
    for item in registry.get("versions", []):
        if item.get("version") == best_version:
            return item
    return None


def get_current_version_entry():
    registry = load_registry()
    cur_version = registry.get("current_version")
    if not cur_version:
        return None
    for item in registry.get("versions", []):
        if item.get("version") == cur_version:
            return item
    return None


if __name__ == "__main__":
    ensure_dirs()
    print("✅ model_governance.py 就緒")
    print(load_registry())
