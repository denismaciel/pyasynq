"""Multi-process workers.

Forking a process that already started the tokio runtime is unsafe
(threads don't survive fork), so instead of `os.fork` the supervisor
starts fresh interpreters running `pyasynq worker ... --processes 1`.
Each child is an independent asynq server with its own heartbeat.
"""

from __future__ import annotations

import logging
import signal
import subprocess
import sys
import time
from types import FrameType

log = logging.getLogger("pyasynq.prefork")


class Supervisor:
    """Keeps `processes` children running; restarts crashed ones and
    forwards SIGTERM/SIGINT (graceful shutdown) and SIGTSTP (stop)."""

    def __init__(self, child_args: list[str], processes: int, restart_backoff: float = 1.0):
        self.child_args = child_args
        self.processes = processes
        self.restart_backoff = restart_backoff
        self.children: list[subprocess.Popen[bytes]] = []
        self._stopping = False

    def _spawn(self) -> subprocess.Popen[bytes]:
        cmd = [sys.executable, "-m", "pyasynq", *self.child_args]
        proc = subprocess.Popen(cmd)
        log.info("started worker process pid=%d", proc.pid)
        return proc

    def _forward(self, signum: int, frame: FrameType | None) -> None:
        if signum in (signal.SIGTERM, signal.SIGINT):
            self._stopping = True
        for c in self.children:
            if c.poll() is None:
                c.send_signal(signal.SIGTERM if signum == signal.SIGINT else signum)

    def run(self) -> int:
        sigs = [signal.SIGTERM, signal.SIGINT] + ([signal.SIGTSTP] if hasattr(signal, "SIGTSTP") else [])
        previous = {s: signal.signal(s, self._forward) for s in sigs}
        try:
            self.children = [self._spawn() for _ in range(self.processes)]
            while True:
                alive = False
                for i, c in enumerate(self.children):
                    code = c.poll()
                    if code is None:
                        alive = True
                    elif not self._stopping:
                        log.warning("worker pid=%d exited with %d; restarting", c.pid, code)
                        time.sleep(self.restart_backoff)
                        self.children[i] = self._spawn()
                        alive = True
                if not alive:
                    return 0
                time.sleep(0.2)
        finally:
            for s, h in previous.items():
                signal.signal(s, h)
