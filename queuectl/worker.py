import os
import signal
import sys
import time
import uuid
from datetime import datetime
from multiprocessing import Process, Event
from rich.console import Console  # type: ignore
from rich.table import Table  # type: ignore
from rich.progress import Progress  # type: ignore

from .models import JobStore, run_command
from .utils import to_iso, utcnow
from .config import Config
from .db import DEFAULT_DB_PATH

PIDFILE = ".queuectl-workers.pid"
console = Console()


# ============================================================
# 🧠 Worker Loop Function (enhanced)
# ============================================================
def _worker_loop(stop_evt: Event, worker_id: str, db_path: str): # type: ignore
    """
    Single worker loop — fetch, execute, retry or DLQ a job.
    Handles timeout, logs, metrics and graceful exit.
    """
    store = JobStore(db_path)
    cfg = Config(db_path)
    lease_sec = max(30, cfg.job_timeout_sec + 5)  # lease = timeout + buffer

    console.print(f"[cyan][{datetime.now().strftime('%H:%M:%S')}] [Worker {worker_id}] Started.[/cyan]")

    while not stop_evt.is_set():
        now_iso = to_iso(utcnow())
        job = store.fetch_and_lease_one(worker_id, now_iso, lease_sec=lease_sec)

        # 💤 No jobs available → idle
        if not job:
            time.sleep(0.5)
            continue

        console.print(f"[yellow][{datetime.now().strftime('%H:%M:%S')}] "
                      f"[Worker {worker_id}] Picked job [bold]{job['id']}[/bold] "
                      f"(Priority={job['priority']}, Attempt={job['attempts']+1}/{job['max_retries']})[/yellow]")

        timeout = job["timeout_sec"] or cfg.job_timeout_sec
        start_time = utcnow()

        # 🧾 Execute job and log output
        rc = run_command(job["command"], timeout_sec=timeout, job_id=job["id"])
        duration = round((utcnow() - start_time).total_seconds(), 2)

        # ✅ Success path
        if rc == 0:
            store.complete(job["id"], worker_id, start_time)
            console.print(f"[green][{datetime.now().strftime('%H:%M:%S')}] "
                          f"[Worker {worker_id}]  Completed job {job['id']} "
                          f"in {duration}s[/green]")
        else:
            # ❌ Failure path — handle retry or DLQ
            reason = f"exit_code={rc}"
            console.print(f"[red][{datetime.now().strftime('%H:%M:%S')}] "
                          f"[Worker {worker_id}] Job {job['id']} failed ({reason}), "
                          f"scheduling retry or DLQ[/red]")
            store.fail_or_retry(job, reason=reason)

        # Small cooldown to avoid CPU spin
        time.sleep(0.2)

    console.print(f"[cyan][{datetime.now().strftime('%H:%M:%S')}] "
                  f"[Worker {worker_id}] Exiting gracefully.[/cyan]")


# ============================================================
# 🧩 Helper: PID File Handling
# ============================================================
def _write_pidfile(pids: list[int]):
    with open(PIDFILE, "w") as f:
        for p in pids:
            f.write(str(p) + "\n")


def _read_pidfile():
    if not os.path.exists(PIDFILE):
        return []
    with open(PIDFILE) as f:
        return [int(x.strip()) for x in f if x.strip()]


# ============================================================
# 🚀 Start Workers (multi-process)
# ============================================================
def start_workers(count: int, db_path: str = DEFAULT_DB_PATH):
    """
    Launch N worker processes for parallel job execution.
    Supports graceful shutdown via SIGINT/SIGTERM.
    """
    stop_evt = Event()
    procs: list[Process] = []

    console.print(f"[bold green] Launching {count} worker(s)...[/bold green]")

    for i in range(count):
        wid = f"w-{uuid.uuid4().hex[:8]}"
        p = Process(target=_worker_loop, args=(stop_evt, wid, db_path), daemon=False)
        p.start()
        procs.append(p)

    _write_pidfile([p.pid for p in procs])
    console.print(f"[green]Started {len(procs)} worker(s).[/green] "
                  f"PIDs: {', '.join(str(p.pid) for p in procs)}")

    def handle_signal(signum, frame):
        console.print("\n[yellow]Graceful shutdown requested...[/yellow]")
        stop_evt.set()

    # 🧩 Handle SIGTERM / Ctrl+C
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        handle_signal(None, None)
    finally:
        if os.path.exists(PIDFILE):
            os.remove(PIDFILE)
        console.print("[bold green] Workers stopped gracefully.[/bold green]")


# ============================================================
# 🛑 Stop Workers
# ============================================================
def stop_workers():
    """
    Stops all running workers by sending SIGTERM.
    """
    pids = _read_pidfile()
    if not pids:
        console.print("[yellow] No running workers found.[/yellow]")
        return

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            console.print(f"[cyan]Sent stop signal to worker PID {pid}[/cyan]")
        except ProcessLookupError:
            console.print(f"[red]Worker PID {pid} not found.[/red]")

    if os.path.exists(PIDFILE):
        os.remove(PIDFILE)

    console.print("[green]All workers stopped successfully.[/green]")
