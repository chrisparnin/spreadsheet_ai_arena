from __future__ import annotations
import shutil
from pathlib import Path

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def safe_rmtree(p: Path):
    if p.exists():
        shutil.rmtree(p)
