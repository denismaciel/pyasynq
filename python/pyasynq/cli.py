"""Command line: run workers/schedulers for an app, inspect queues.

    pyasynq worker myproj.tasks:app -q critical=6 -q default=3 -c 20
    pyasynq worker myproj.tasks:app --processes 4
    pyasynq scheduler myproj.tasks:app
    pyasynq inspect queues
    pyasynq inspect tasks default retry
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Sequence

from pyasynq import _core
from pyasynq.worker import DEFAULT_REDIS_URL


def load_app(ref: str) -> Any:
    from pyasynq.app import Asynq

    module_name, _, attr = ref.partition(":")
    if not module_name or not attr:
        raise SystemExit(f"expected 'module:attribute', got {ref!r}")
    sys.path.insert(0, os.getcwd())
    obj: Any = importlib.import_module(module_name)
    for part in attr.split("."):
        obj = getattr(obj, part)
    if callable(obj) and not isinstance(obj, Asynq):
        obj = obj()
    if not isinstance(obj, Asynq):
        raise SystemExit(f"{ref!r} is not an Asynq app")
    return obj


def parse_queues(values: list[str] | None) -> dict[str, int] | None:
    if not values:
        return None
    queues: dict[str, int] = {}
    for v in values:
        name, _, weight = v.partition("=")
        queues[name] = int(weight) if weight else 1
    return queues


def strip_option(argv: Sequence[str], name: str) -> list[str]:
    """Removes `name VALUE` and `name=VALUE` from argv."""
    out: list[str] = []
    skip = False
    for a in argv:
        if skip:
            skip = False
        elif a == name:
            skip = True
        elif not a.startswith(name + "="):
            out.append(a)
    return out


def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime,)):
        return v.isoformat()
    if isinstance(v, timedelta):
        return v.total_seconds()
    if isinstance(v, bytes):
        try:
            return v.decode()
        except UnicodeDecodeError:
            return v.hex()
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if hasattr(v, "__class__") and v.__class__.__module__ == "pyasynq._core":
        return {
            k: _jsonable(getattr(v, k))
            for k in dir(v)
            if not k.startswith("_") and not callable(getattr(v, k))
        }
    return v


def _print(v: Any) -> None:
    print(json.dumps(_jsonable(v), indent=2, sort_keys=True))


def cmd_worker(args: argparse.Namespace, argv: Sequence[str]) -> int:
    if args.processes > 1:
        from pyasynq.prefork import Supervisor

        return Supervisor([*strip_option(argv, "--processes"), "--processes", "1"], args.processes).run()
    app = load_app(args.app)
    if args.redis:
        app.redis_url = args.redis
    options: dict[str, Any] = {"shutdown_timeout": args.shutdown_timeout}
    if args.concurrency:
        options["concurrency"] = args.concurrency
    if (queues := parse_queues(args.queue)) is not None:
        options["queues"] = queues
    if args.strict:
        options["strict_priority"] = True
    app.run_worker(**options)
    return 0


def cmd_scheduler(args: argparse.Namespace, argv: Sequence[str]) -> int:
    app = load_app(args.app)
    if args.redis:
        app.redis_url = args.redis
    app.run_scheduler(location=args.location)
    return 0


def cmd_inspect(args: argparse.Namespace, argv: Sequence[str]) -> int:
    from pyasynq.inspector import Inspector

    i = Inspector(args.redis or DEFAULT_REDIS_URL)
    a = args.args
    match args.what:
        case "queues":
            _print([i.get_queue_info(q) for q in i.queues()])
        case "queue":
            _print(i.get_queue_info(a[0]))
        case "history":
            _print(i.history(a[0], days=int(a[1]) if len(a) > 1 else 7))
        case "tasks":
            _print(i.list_tasks(a[0], a[1], page=args.page, size=args.size))
        case "task":
            _print(i.get_task_info(a[0], a[1]))
        case "servers":
            _print(i.servers())
        case "schedulers":
            _print(i.scheduler_entries())
        case "groups":
            _print(i.groups(a[0]))
        case "pause":
            i.pause_queue(a[0])
        case "unpause":
            i.unpause_queue(a[0])
        case "run":
            i.run_task(a[0], a[1])
        case "archive":
            i.archive_task(a[0], a[1])
        case "delete":
            i.delete_task(a[0], a[1])
        case "cancel":
            i.cancel_processing(a[0])
        case "delete-queue":
            i.delete_queue(a[0], force=args.force)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pyasynq", description=__doc__.split("\n")[0])
    p.add_argument("--log-level", default=os.environ.get("PYASYNQ_LOG_LEVEL", "info"))
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser("worker", help="process tasks")
    w.add_argument("app", help="module:attribute of the Asynq app")
    w.add_argument("--redis", help="redis URL (overrides the app's)")
    w.add_argument("-c", "--concurrency", type=int)
    w.add_argument("-q", "--queue", action="append", help="name[=weight], repeatable")
    w.add_argument("--strict", action="store_true", help="strict queue priority")
    w.add_argument("--shutdown-timeout", type=float, default=8)
    w.add_argument("--processes", type=int, default=1, help="worker processes")
    w.set_defaults(func=cmd_worker)

    s = sub.add_parser("scheduler", help="enqueue @app.periodic tasks")
    s.add_argument("app")
    s.add_argument("--redis")
    s.add_argument("--location", default="UTC", help="time zone for cron specs")
    s.set_defaults(func=cmd_scheduler)

    i = sub.add_parser("inspect", help="inspect queues and tasks")
    i.add_argument(
        "what",
        choices=[
            "queues", "queue", "history", "tasks", "task", "servers", "schedulers", "groups",
            "pause", "unpause", "run", "archive", "delete", "cancel", "delete-queue",
        ],
    )
    i.add_argument("args", nargs="*")
    i.add_argument("--redis")
    i.add_argument("--page", type=int, default=1)
    i.add_argument("--size", type=int, default=30)
    i.add_argument("--force", action="store_true")
    i.set_defaults(func=cmd_inspect)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    _core.reset_log_cache()
    try:
        return args.func(args, argv)
    except _core.AsynqError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
