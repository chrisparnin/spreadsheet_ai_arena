from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .tools.task_queue import get_task_queue_mcp, TaskQueue

@dataclass
class ServerHandle:
    """
    Lightweight handle to control the MCP server.

    Usage:
        handle = start_in_thread(port=8080, path="/mcp", dataset_id="basic")
        try:
            ok = handle.wait(timeout=60)        # synchronous wait (True if completed)
            # or: await handle.wait_async(60)   # if you're already in async context
        finally:
            handle.stop()
    """
    mcp: object
    thread: threading.Thread
    tasks: TaskQueue

    async def wait_async(self, timeout: Optional[float] = None) -> None:
        await asyncio.wait_for(self.tasks.wait_for_all_completed(), timeout=timeout)

    def wait(self, timeout: Optional[float] = None) -> bool:
        """
        Synchronous wrapper around wait_async.

        Returns:
            True  -> all tasks completed before timeout
            False -> timed out

        If an event loop is already running in this thread, raise a RuntimeError and
        use `await handle.wait_async(...)` instead.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            try:
                asyncio.run(self.wait_async(timeout))
                return True
            except asyncio.TimeoutError:
                return False
        else:
            raise RuntimeError(
                "An event loop is already running; use `await handle.wait_async(...)` instead."
            )

    def stop(self, join_timeout: float = 5) -> None:
        """Stop the MCP server and join its thread."""
        try:
            self.mcp.stop()
        except Exception:
            pass
        self.thread.join(timeout=join_timeout)


def start_in_thread(
    *,
    transport: str = "http",
    port: int = 8080,
    path: str = "/mcp",
    tasks: Optional[TaskQueue] = None,
) -> ServerHandle:
    """
    Start the (blocking) MCP server on a daemon thread and return a ServerHandle.
    """
    #mcp = get_task_list_mcp(tasks)
    mcp = get_task_queue_mcp(tasks)

    t = threading.Thread(
        target=mcp.run,
        kwargs=dict(transport=transport, port=port, path=path),
        daemon=True,
    )
    t.start()
    return ServerHandle(mcp=mcp, thread=t, tasks=tasks)
