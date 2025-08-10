from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .checkout import checkout_one, load_dataset_config, load_registry  # reuse your helpers

@dataclass
class TaskSpec:
    id: str
    instruction: str
    input_path: Optional[Path] = None
    answer_path: Optional[Path] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_prompt(self) -> str:
        parts = [self.instruction]
        if self.input_path:
            parts.append(f"Input: {self.input_path}")
        if self.answer_path:
            parts.append(f"Expected answer: {self.answer_path}")
        return " ".join(parts)


def _normalize_dataset_id(dataset_id: str) -> str:
    return dataset_id if "/" in dataset_id else f"benchmark-tasks/{dataset_id}"


def _maybe_rel(base: Path, s: Optional[str]) -> Optional[Path]:
    if not s:
        return None
    p = Path(s)
    return p if p.is_absolute() else (base / p).resolve()


def _parse_tasks(obj: Any, dataset_root: Path) -> List[TaskSpec]:
    # Accept array or {"tasks":[...]} for manifest contents
    raw: List[Dict[str, Any]]
    if isinstance(obj, list):
        raw = obj
    elif isinstance(obj, dict) and isinstance(obj.get("tasks"), list):
        raw = obj["tasks"]
    else:
        raise ValueError("Manifest must be a JSON array or an object with a 'tasks' array")

    out: List[TaskSpec] = []
    for t in raw:
        tid = str(t.get("id") or t.get("name") or f"task-{len(out)+1}")
        instr = t.get("instruction") or f"Run benchmark task '{tid}'."
        inp = _maybe_rel(dataset_root, t.get("input"))
        ans = _maybe_rel(dataset_root, t.get("answer"))
        meta = {k: v for k, v in t.items() if k not in {"id", "name", "instruction", "input", "answer"}}
        out.append(TaskSpec(tid, instr, inp, ans, meta))
    return out


def build_tasks_from_dataset(
    dataset_id: str,
    *,
    datasets_dir: str = "datasets",
    update: bool = False,
    limit: Optional[int] = None,
    shuffle: bool = False,
) -> List[TaskSpec]:
    """
    Materialize the dataset and load tasks from the manifest specified in the dataset config.
    Requires 'manifest' in the dataset config; no guessing or pairing heuristics.
    """
    dsid = _normalize_dataset_id(dataset_id)

    registry = load_registry()
    cfg_ref = registry.get(dsid)
    if not cfg_ref:
        raise ValueError(f"Dataset '{dsid}' not found in registry")

    cfg = load_dataset_config(cfg_ref)  # dict or dataclass-like

    # Enforce manifest presence
    manifest_rel = None
    if isinstance(cfg, dict):
        manifest_rel = cfg.get("manifest")
    else:
        manifest_rel = getattr(cfg, "manifest", None)

    if not manifest_rel or Path(manifest_rel).is_absolute():
        raise ValueError(
            f"Dataset config for '{dsid}' must include a relative 'manifest' path (e.g., 'tasks.json')."
        )

    # Ensure dataset contents exist (clones + transforms run here)
    root = checkout_one(dsid, Path(datasets_dir), update=update)

    manifest_path = (root / manifest_rel).resolve()
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    obj = json.loads(manifest_path.read_text(encoding="utf-8"))
    tasks = _parse_tasks(obj, root)

    if shuffle:
        random.shuffle(tasks)
    if limit is not None:
        tasks = tasks[: max(0, int(limit))]

    return tasks
