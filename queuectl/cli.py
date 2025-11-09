import json
import os
import sys
from typing import Optional

import typer  # type: ignore
from rich import print  # type: ignore
from rich.table import Table  # type: ignore
from rich.console import Console  # type: ignore

from .db import DEFAULT_DB_PATH, init_db, connect
from .models import JobStore
from .worker import start_workers, stop_workers
from .config import Config
from .utils import to_iso, utcnow

app = typer.Typer(help="queuectl: Background job queue with workers, retries, logs, metrics, and DLQ")

# -------------------- SUBCOMMAND GROUPS --------------------
worker_app = typer.Typer(help="Manage workers (start/stop)")
dlq_app = typer.Typer(help="Manage the Dead Letter Queue")
config_app = typer.Typer(help="Configuration commands")

app.add_typer(worker_app, name="worker")
app.add_typer(dlq_app, name="dlq")
app.add_typer(config_app, name="config")

DB_OPTION = typer.Option(DEFAULT_DB_PATH, "--db", help="Path to SQLite database file", show_default=True)
console = Console()


# ============================================================
# Ensure DB initialization
# ============================================================
@app.callback()
def _ensure_db(db: str = DB_OPTION):
    init_db(db)


# ============================================================
# ENQUEUE JOB
# ============================================================
@app.command()
def enqueue(job_json: str = typer.Argument(..., help="Job JSON (example: '{\"id\":\"job1\",\"command\":\"echo hi\"}')"),
            db: str = DB_OPTION):
    """
    Enqueue a new job into the queue.
    """
    try:
        job = json.loads(job_json)
        if "id" not in job or "command" not in job:
            raise ValueError("Job must include 'id' and 'command'")
    except Exception as e:
        console.print(f"[red]Invalid job JSON: {e}[/red]")
        raise typer.Exit(code=1)

    store = JobStore(db)
    store.enqueue(job)
    console.print(f"[green] Enqueued job:[/green] {job['id']}")


# ============================================================
# WORKER COMMANDS
# ===============================================================
@worker_app.command("start")
def worker_start(count: int = typer.Option(1, "--count", "-c", help="Number of workers"),
                 db: str = DB_OPTION):
    """
    Start one or more worker processes to process jobs.
    """
    start_workers(count, db_path=db)


@worker_app.command("stop")
def worker_stop():
    """
    Gracefully stop all running workers.
    """
    stop_workers()


# ============================================================
# STATUS COMMAND
# =============================================================

@app.command()
def status(db: str = DB_OPTION):
    """
    Show the number of jobs in each state and DLQ size.
    """
    store = JobStore(db)
    st = store.stats()

    table = Table(title="QueueCTL System Status")
    table.add_column("State", justify="left")
    table.add_column("Count", justify="right")

    for state in ("pending", "processing", "completed", "failed", "dead", "dlq"):
        table.add_row(state, str(st.get(state, 0)))

    table.add_row("avg_duration_sec", str(st.get("avg_duration", 0)))
    console.print(table)


# ============================================================
# LIST JOBS
# =============================================================
@app.command("list")
def list_jobs(state: Optional[str] = typer.Option(None, "--state", "-s", help="Filter by state"),
              db: str = DB_OPTION):
    """
    List all jobs (optionally filtered by state).
    """
    store = JobStore(db)
    jobs = store.list_jobs(state)

    if not jobs:
        console.print("[yellow]No jobs found.[/yellow]")
        raise typer.Exit(0)

    table = Table(title=f"Jobs {'(' + state + ')' if state else ''}")
    table.add_column("id")
    table.add_column("command")
    table.add_column("state")
    table.add_column("attempts")
    table.add_column("max_retries")
    table.add_column("priority")
    table.add_column("duration(s)")
    table.add_column("exit_code")
    table.add_column("run_at")
    table.add_column("updated_at")

    for j in jobs:
        table.add_row(
            j["id"],
            j["command"],
            j["state"],
            str(j["attempts"]),
            str(j["max_retries"]),
            str(j.get("priority", 100)),
            str(j.get("duration_sec", "")),
            str(j.get("exit_code", "")),
            str(j["run_at"]),
            j["updated_at"],
        )

    console.print(table)


# ============================================================
# DEAD LETTER QUEUE COMMANDS
# ==============================================================
@dlq_app.command("list")
def dlq_list(db: str = DB_OPTION):
    """
    View jobs currently in the DLQ.
    """
    store = JobStore(db)
    rows = store.list_dlq()

    if not rows:
        console.print("[yellow]No DLQ entries found.[/yellow]")
        return

    table = Table(title="Dead Letter Queue")
    table.add_column("id")
    table.add_column("reason")
    table.add_column("created_at")

    for r in rows:
        table.add_row(r["id"], r.get("reason", ""), r["created_at"])

    console.print(table)


@dlq_app.command("retry")
def dlq_retry(job_id: str = typer.Argument(..., help="Job ID to retry"),
              db: str = DB_OPTION):
    """
    Retry a failed job from the DLQ (moves it back to pending).
    """
    store = JobStore(db)
    ok = store.retry_from_dlq(job_id)
    if not ok:
        console.print(f"[red]No DLQ job found with ID {job_id}[/red]")
        raise typer.Exit(1)
    console.print(f"[green]Re-enqueued {job_id} from DLQ[/green]")


# ============================================================
# CONFIGURATION COMMANDS
# ============================================================
@config_app.command("get")
def cfg_get(key: str = typer.Argument(...), db: str = DB_OPTION):
    """
    Get a specific configuration value.
    """
    cfg = Config(db)
    val = cfg.get(key)
    if val is None:
        console.print(f"[yellow]{key} not set.[/yellow]")
    else:
        console.print(f"[cyan]{key}[/cyan] = [green]{val}[/green]")


@config_app.command("set")
def cfg_set(key: str = typer.Argument(...), value: str = typer.Argument(...), db: str = DB_OPTION):
    """
    Set a configuration value.

    Example keys:
      - base_backoff
      - default_max_retries
      - job_timeout_sec
    """
    cfg = Config(db)
    cfg.set(key, value)
    console.print(f"[green]Set {key} = {value}[/green]")


@config_app.command("show")
def cfg_show(db: str = DB_OPTION):
    """
    Display all configuration key-value pairs.
    """
    con = connect(db)
    rows = con.execute("SELECT key, value FROM config ORDER BY key;").fetchall()

    if not rows:
        console.print("[yellow]No configuration values found.[/yellow]")
        return

    table = Table(title="Configuration Settings")
    table.add_column("Key")
    table.add_column("Value")
    for r in rows:
        table.add_row(r["key"], r["value"])
    console.print(table)


# ============================================================
# JOB LOGS VIEWER
# ============================================================
@app.command("logs")
def view_logs(job_id: str = typer.Argument(..., help="Job ID to view logs for")):
    """
    View logs for a specific completed or failed job.
    """
    log_path = os.path.join("logs", f"{job_id}.log")
    if not os.path.exists(log_path):
        console.print(f"[red]No logs found for job {job_id}[/red]")
        raise typer.Exit(1)

    console.print(f"[cyan]=== Logs for job {job_id} ===[/cyan]\n")
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        print(f.read())


# ============================================================
# METRICS SUMMARY
# ============================================================
@app.command("metrics")
def metrics(db: str = DB_OPTION):
    """
    Show execution metrics such as success rate and average duration.
    """
    con = connect(db)
    total = con.execute("SELECT COUNT(*) AS c FROM jobs;").fetchone()["c"]
    completed = con.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='completed';").fetchone()["c"]
    failed = con.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='failed';").fetchone()["c"]
    avg_duration = con.execute("SELECT AVG(duration_sec) AS a FROM jobs WHERE duration_sec > 0;").fetchone()["a"]
    avg_duration = round(avg_duration or 0, 2)

    success_rate = 0 if total == 0 else round((completed / total) * 100, 2)

    table = Table(title="Job Execution Metrics")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("Total Jobs", str(total))
    table.add_row("Completed", str(completed))
    table.add_row("Failed", str(failed))
    table.add_row("Avg Duration (s)", str(avg_duration))
    table.add_row("Success Rate (%)", f"{success_rate}%")

    console.print(table)


# ============================================================
# DASHBOARD COMMAND
# ==============================================================
@app.command("dashboard")
def dashboard(db: str = DB_OPTION,
              port: int = typer.Option(8080, "--port", "-p", help="Port for the dashboard")):
    """
    Launch the live monitoring web dashboard (bonus feature).
    """
    try:
        from .dashboard import start_dashboard
    except ImportError:
        console.print("[red]Dashboard module not found. Ensure dashboard.py exists.[/red]")
        raise typer.Exit(1)

    console.print(f"[green]Launching dashboard at http://localhost:{port}[/green]")
    start_dashboard(db, port)
