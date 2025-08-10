# spreadsheet_arena/cli.py
from __future__ import annotations

import argparse
from typing import Optional, List

from spreadsheet_arena.datasets.checkout import list_available, checkout_many
from spreadsheet_arena.mcp.server import start_in_thread
from spreadsheet_arena.datasets.tasks import build_tasks_from_dataset
from spreadsheet_arena.mcp.tools.task_queue import TaskQueue

# -----------------
# Subcommand impls
# -----------------
def _cmd_checkout(args: argparse.Namespace) -> int:
    if args.list:
        list_available(output_dir=args.output_dir)
        return 0

    if not args.datasets:
        print("No datasets provided. Try: arena checkout --list")
        return 1

    checkout_many(args.datasets, output_dir=args.output_dir, update=args.update)
    return 0


def _cmd_start(args):
    # Preflight materialize + load
    prebuilt_specs = None
    if args.dataset:
        prebuilt_specs = build_tasks_from_dataset(
            args.dataset,
            datasets_dir=args.datasets_dir,
            update=args.update,
            limit=args.limit,
            shuffle=args.shuffle,
        )
        print(f"Loaded {len(prebuilt_specs)} tasks from manifest for '{args.dataset}'.")

    # Optional: drop in your real grader here
    def _noop_grader(spec, output_path):
        return {"status": "ungraded"}

    queue = TaskQueue(prebuilt_specs, results_dir="output_dir", grader=_noop_grader, log_progress=True)


    handle = start_in_thread(
        transport=args.transport,
        port=args.port,
        path=args.path,
        tasks=queue,
    )
    try:
        completed = handle.wait(timeout=args.timeout)
        print("All tasks completed." if completed else "Timed out waiting for tasks.")
        return 0 if completed else 2
    finally:
        handle.stop()

# -------------
# Argparse CLI
# -------------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="arena",
        description="Spreadsheet Arena CLI — dataset checkout and MCP runtime.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # checkout
    pc = sub.add_parser(
        "checkout",
        help="Fetch benchmark datasets into the local datasets directory.",
        description="Download benchmark materials as defined in configs/registry.json.",
    )
    pc.add_argument(
        "datasets",
        nargs="*",
        help="Dataset IDs (e.g., 'benchmark-tasks/basic' or shorthand 'basic'). Multiple allowed.",
    )
    pc.add_argument("--list", action="store_true", help="List available datasets and local cache status.")
    pc.add_argument("--update", action="store_true", help="Force re-download/update of datasets.")
    pc.add_argument("--output-dir", default="datasets", help="Directory to store datasets (default: ./datasets)")
    pc.set_defaults(func=_cmd_checkout)

    # start
    ps = sub.add_parser(
        "start",
        help="Start MCP server and enqueue tasks from a dataset manifest.",
        description="Starts the MCP server and seeds tasks from a dataset's declared manifest.",
    )
    ps.add_argument("--transport", default="http")
    ps.add_argument("--port", type=int, default=8080)
    ps.add_argument("--path", default="/mcp")
    ps.add_argument("--dataset", help="Dataset ID (e.g., 'benchmark-tasks/basic' or 'basic').")
    ps.add_argument("--datasets-dir", default="datasets", help="Local datasets root (default: ./datasets)")
    ps.add_argument("--update", action="store_true", help="Re-checkout/update the dataset before starting.")
    ps.add_argument("--limit", type=int, help="Limit number of tasks enqueued.")
    ps.add_argument("--shuffle", action="store_true", help="Shuffle tasks before limiting.")
    ps.add_argument("--timeout", type=int, default=60, help="Seconds to wait for tasks to complete.")
    ps.set_defaults(func=_cmd_start)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
