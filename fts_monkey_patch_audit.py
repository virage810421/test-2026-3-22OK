# -*- coding: utf-8 -*-
from __future__ import annotations
import re, json
from pathlib import Path

PATCH_ASSIGN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\s*=\s*", re.M)

def audit(root: str | Path = ".") -> dict:
    root = Path(root)
    findings = []
    for path in root.glob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        matches = PATCH_ASSIGN_RE.findall(text)
        if matches:
            findings.append({"file": path.name, "patch_assignments": len(matches)})
    out = {"findings": findings, "count": len(findings), "note": "Public broker/logger/engine symbols now expose formal facade classes; residual patch assignments are legacy compatibility debt."}
    return out

if __name__ == "__main__":
    result = audit(Path.cwd())
    Path("runtime").mkdir(exist_ok=True)
    Path("runtime/monkey_patch_audit.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
