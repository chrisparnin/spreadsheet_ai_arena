from __future__ import annotations
import fnmatch, os, shutil, zipfile
import tarfile
from pathlib import Path
from typing import Dict, List

from spreadsheet_arena.datasets.utils import ensure_dir

def apply_transforms(root: Path, transforms: List[Dict]) -> None:
    for step in transforms:
        t = step["type"]
        if t == "unzip":
            _t_unzip(root, step)
        elif t == "keep_only":
            _t_keep_only(root, step)
        elif t == "strip_prefix":
            _t_strip_prefix(root, step)
        elif t == "rename":
            _t_rename(root, step)
        elif t == "delete":
            _t_delete(root, step)
        elif t == "copy":
            _t_copy(root, step)
        elif t == "move":
            _t_move(root, step)
        elif t == "untar":
            _t_untar(root, step)
        else:
            raise ValueError(f"Unknown transform type: {t}")


def _t_untar(root: Path, step: dict):
    for tgz in _glob_paths(root, step["patterns"]):
        if not tgz.suffixes or not (tgz.name.endswith(".tar.gz") or tgz.name.endswith(".tgz")):
            continue
        _safe_untar(tgz, tgz.parent)
        if step.get("delete_archives"):
            tgz.unlink()

def _safe_untar(tar_path: Path, dest: Path):
    with tarfile.open(tar_path, "r:gz") as tf:
        # Prevent path traversal
        for m in tf.getmembers():
            mpath = (dest / m.name).resolve()
            if not str(mpath).startswith(str(dest.resolve())):
                raise RuntimeError(f"Unsafe tar member path: {m.name}")
        tf.extractall(dest)

def _glob_paths(root: Path, patterns: List[str]) -> List[Path]:
    out: List[Path] = []
    for p in patterns:
        out.extend(root.rglob(p))
    return list(dict.fromkeys(out))  # dedupe

def _t_unzip(root: Path, step: Dict):
    for z in _glob_paths(root, step["patterns"]):
        if z.suffix.lower() != ".zip":
            continue
        _safe_unzip(z, z.parent)
        if step.get("delete_archives"):
            z.unlink()

def _safe_unzip(zip_path: Path, dest: Path):
    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            # prevent Zip Slip
            extracted = (dest / member.filename).resolve()
            if not str(extracted).startswith(str(dest.resolve())):
                raise RuntimeError(f"Unsafe zip member path: {member.filename}")
        zf.extractall(dest)

def _t_keep_only(root: Path, step: Dict):
    keep = set(_glob_paths(root, step["patterns"]))
    for p in list(root.rglob("*")):
        if p == root: 
            continue
        if any(parent in keep for parent in p.parents):
            continue
        if p.is_dir():
            # delete empty dirs after pass
            continue
        if p not in keep:
            p.unlink(missing_ok=True)
    # second pass to prune empty dirs
    for d in sorted([d for d in root.rglob("*") if d.is_dir()], reverse=True):
        try: d.rmdir()
        except OSError: pass

def _t_strip_prefix(root: Path, step: Dict):
    prefix = step["prefix"].rstrip("/\\")
    src = root / prefix
    if not src.exists():
        return
    for item in src.iterdir():
        shutil.move(str(item), str(root / item.name))
    # remove emptied prefix dirs
    for d in sorted([d for d in (root / prefix).rglob("*") if d.is_dir()], reverse=True):
        d.rmdir()
    (root / prefix).rmdir()

def _t_rename(root: Path, step: Dict):
    (root / step["from"]).rename(root / step["to"])

def _t_delete(root: Path, step: Dict):
    for p in _glob_paths(root, step["patterns"]):
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink(missing_ok=True)

def _t_copy(root: Path, step: Dict):
    src = root / step["from"]
    dst = root / step["to"]
    if src.is_dir():
        ensure_dir(dst)
        for item in src.iterdir():
            _copy_any(item, dst / item.name)
    else:
        ensure_dir(dst.parent)
        _copy_any(src, dst)

def _t_move(root: Path, step: Dict):
    src = root / step["from"]
    dst = root / step["to"]
    ensure_dir(dst.parent)
    shutil.move(str(src), str(dst))

def _copy_any(src: Path, dst: Path):
    if src.is_dir():
        shutil.copytree(src, dst, dirs_exist_ok=True)
    else:
        shutil.copy2(src, dst)
