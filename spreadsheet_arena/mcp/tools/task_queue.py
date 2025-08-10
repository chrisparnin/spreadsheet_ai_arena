from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Callable, Dict, List, Optional, Any
import threading
import time

from fastmcp import FastMCP
from spreadsheet_arena.datasets.tasks import TaskSpec


@dataclass
class QueueItem:
    id: int
    spec: TaskSpec
    status: str = "pending"          # pending | in_progress | completed
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    result: Optional[Dict[str, Any]] = None


class TaskQueue:
    """
    Stream tasks one-by-one to clients. Thread-safe.
    """
    def __init__(
        self,
        specs: List[TaskSpec],
        *,
        results_dir: Path | str = "results",
        grader: Optional[Callable[[TaskSpec, Path], Dict[str, Any]]] = None,
        log_progress: bool = False,
    ):
        self._items = [QueueItem(id=i, spec=s) for i, s in enumerate(specs)]
        self._lock = threading.Lock()
        self._in_progress: set[int] = set()
        self._completed = 0
        self._results_dir = Path(results_dir)
        self._results_dir.mkdir(parents=True, exist_ok=True)
        self._grader = grader
        self.log_progress = log_progress

    # ---------- public API for server/CLI ----------
    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for it in self._items if it.status == "pending")

    def completed_count(self) -> int:
        with self._lock:
            return self._completed

    async def wait_for_all_completed(self, timeout_seconds: int | None = None):
        import asyncio
        start = time.time()
        while True:
            with self._lock:
                done = self._completed == len(self._items)
            if done:
                return
            if timeout_seconds is not None and (time.time() - start) > timeout_seconds:
                raise TimeoutError("wait_for_all_completed timed out")
            await asyncio.sleep(0.1)

    # ---------- exposed to MCP tools (via wrapper below) ----------
    def next_task(self) -> Optional[Dict[str, Any]]:
        """
        Lease the next pending task. Returns a JSON-serializable dict or None when exhausted.
        """
        with self._lock:
            for it in self._items:
                if it.status == "pending":
                    it.status = "in_progress"
                    it.started_at = time.time()
                    self._in_progress.add(it.id)
                    if self.log_progress:
                        print(f"[queue] leasing task {it.id}: {it.spec.id or ''}")
                    # Don't leak answer_path to the agent; keep that server-side
                    return {
                        "task_id": it.id,
                        "id": it.spec.id,
                        "instruction": it.spec.instruction,
                        "input": str(it.spec.input_path) if it.spec.input_path else None,
                        "meta": it.spec.meta,
                    }
        return None

    def submit_answer(self, task_id: int, output_path: str | None = None) -> Dict[str, Any]:
        """
        Submit/grade an answer for a leased task.
        """
        p_out = Path(output_path) if output_path else None
        with self._lock:
            if task_id < 0 or task_id >= len(self._items):
                raise ValueError(f"Unknown task_id {task_id}")
            it = self._items[task_id]
            if it.status != "in_progress":
                raise RuntimeError(f"Task {task_id} not in progress (status={it.status})")

            # Optional grading
            grade: Dict[str, Any] = {}
            if self._grader and p_out:
                try:
                    grade = self._grader(it.spec, p_out)
                except Exception as e:
                    grade = {"status": "grading_error", "error": str(e)}

            # Persist a small record
            rec = {
                "task_id": it.id,
                "spec_id": it.spec.id,
                "output_path": str(p_out) if p_out else None,
                "grade": grade or None,
                "submitted_at": time.time(),
            }
            (self._results_dir / f"task_{it.id:05d}.json").write_text(
                __import__("json").dumps(rec, indent=2), encoding="utf-8"
            )

            # Mark completed
            it.status = "completed"
            it.finished_at = time.time()
            it.result = rec
            self._in_progress.discard(it.id)
            self._completed += 1

            if self.log_progress:
                print(f"[queue] completed task {it.id}")

            return {"ok": True, "grade": grade or None, "completed": self._completed, "total": len(self._items)}


def get_task_queue_mcp(queue: TaskQueue) -> FastMCP:
    """
    Two tools:
      - next_task() -> { task_id, id, instruction, input, meta } | null
      - submit_answer(task_id, output_path) -> { ok, grade?, completed, total }
    """
    mcp = FastMCP("SpreadsheetArena")

    @mcp.tool()
    def next_task() -> Optional[dict]:
        """Lease the next task to work on. Returns null when no tasks remain."""
        return queue.next_task()

    @mcp.tool()
    def submit_answer(task_id: int, output_path: str | None = None) -> dict:
        """
        Submit/grade results for a task leased via next_task().
        'output_path' should point to a produced workbook or artifact.
        """
        return queue.submit_answer(task_id, output_path)

    @mcp.prompt()
    def complete_tasks_prompt():
        """Generate a prompt for completing the tasks."""
        return "Please complete the tasks."

    return mcp

TASK_PROMPT_NAME = "complete_tasks_prompt"