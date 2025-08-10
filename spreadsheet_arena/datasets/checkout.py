from __future__ import annotations

import json, shutil, subprocess, tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from spreadsheet_arena.datasets.adapters.transforms import apply_transforms
from spreadsheet_arena.datasets.utils import ensure_dir, safe_rmtree
from spreadsheet_arena.datasets.adapters.git import fetch_git

REGISTRY_PATH = Path("configs/registry.json")

def _fetch_source(source: Dict[str, Any], tmp: Path) -> Path:
    stype = source["type"]
    if stype == "git":
        return fetch_git(source["url"], source.get("ref"), source.get("subdir"), tmp)
    raise ValueError(f"Unsupported source type: {stype}")

@dataclass(frozen=True)
class DatasetConfig:
    id: str
    version: str
    source: Dict[str, Any]
    transforms: List[Dict[str, Any]] = field(default_factory=list)
    # NEW:
    manifest: Optional[str] = None
    # (optional for future use)
    tasks: Optional[List[Dict[str, Any]]] = None

def load_dataset_config(ref: str | Path | Dict[str, Any]) -> DatasetConfig:
    obj = json.loads(Path(ref).read_text(encoding="utf-8")) if not isinstance(ref, dict) else ref
    return DatasetConfig(
        id=obj["id"],
        version=obj.get("version", "0.0.0"),
        source=obj["source"],
        transforms=obj.get("transforms", []),
        manifest=obj.get("manifest"),
        tasks=obj.get("tasks"),
    )

def load_registry(registry_path: Path = REGISTRY_PATH) -> Dict[str, Any]:
    return json.loads(registry_path.read_text(encoding="utf-8"))

def checkout_one(dataset_id: str, output_dir: Path, update: bool = False) -> Path:
    registry = load_registry()
    ref = registry.get(dataset_id)
    if not ref:
        raise ValueError(f"Dataset '{dataset_id}' not found in registry {REGISTRY_PATH}")
    cfg = load_dataset_config(ref)

    dst = (output_dir / dataset_id).resolve()
    ensure_dir(output_dir)

    if dst.exists() and not update:
        print(f"✔ Already present: {dataset_id}")
        return dst

    # stage: fetch into tmp, then transform into dst atomically
    with tempfile.TemporaryDirectory(prefix="arena_") as tmpdir:
        tmp = Path(tmpdir)
        src_root = _fetch_source(cfg.source, tmp)
        # copy fetched content to a working dir so transforms don’t mutate the clone directly
        workdir = tmp / "work"
        shutil.copytree(src_root, workdir)
        apply_transforms(workdir, cfg.transforms)

        # replace destination atomically
        if dst.exists():
            safe_rmtree(dst)
        shutil.move(str(workdir), str(dst))

    print(f"⬇ Downloaded: {dataset_id} → {dst}")
    return dst

def checkout_many(ids: List[str], output_dir: str = "datasets", update: bool = False) -> None:
    base = Path(output_dir).resolve()
    ensure_dir(base)
    for i in ids:
        checkout_one(i if "/" in i else f"benchmark-tasks/{i}", base, update=update)

def list_available(output_dir: str = "datasets") -> None:
    base = Path(output_dir).resolve()
    registry = load_registry()
    print("Available benchmarks:")
    for did, _ref in registry.items():
        mark = "✔" if (base / did).exists() else "—"
        print(f"  {mark} {did}")
