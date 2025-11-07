import json
import os
import platform
import subprocess
import signal
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .db import connect, init_db, tx, DEFAULT_DB_PATH
from .utils import utcnow, to_iso, add_seconds
from .config import Config

VALID_STATES = ("pending", "processing", "completed", "failed", "dead")


@dataclass
class Job:
    id: str
    command: str
    state: str = "pending"
    attempts: int = 0
    max_retries: int = 3
    created_at: str = ""
    updated_at: str = ""
    run_at: Optional[str] = None
    lease_until: Optional[str] = None
    worker_id: Optional[str] = None
    priority: int = 100
    timeout_sec: Optional[int] = None
    duration_sec: Optional[float] = 0.0
    exit_code: Optional[int] = None


class JobStore:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        init_db(db_path)
        self.cfg = Config(db_path)

    # ---------------- Enqueue ----------------
    def enqueue(self, job: dict):
        con = connect(self.db_path)
        now = to_iso(utcnow())
        job.setdefault("state", "pending")
        job.setdefault("attempts", 0)
        job.setdefault("max_retries", self.cfg.default_max_retries)
        job.setdefault("created_at", now)
        job.setdefault("updated_at", now)
        job.setdefault("priority", 100)
        job.setdefault("run_at", job.get("run_at", job["created_at"]))
        job.setdefault("timeout_sec", 60)
        job.setdefault("duration_sec", 0)
        job.setdefault("exit_code", None)

        with con:
            con.execute("""
                INSERT INTO jobs(id, command, state, attempts, max_retries,
                                 created_at, updated_at, run_at, priority, timeout_sec,
                                 duration_sec, exit_code)
                VALUES(:id, :command, :state, :attempts, :max_retries,
                       :created_at, :updated_at, :run_at, :priority, :timeout_sec,
                       :duration_sec, :exit_code)
            """, job)

    # ---------------- List Jobs ----------------
    def list_jobs(self, state: Optional[str] = None):
        con = connect(self.db_path)
        if state:
            cur = con.execute("SELECT * FROM jobs WHERE state=? ORDER BY created_at;", (state,))
        else:
            cur = con.execute("SELECT * FROM jobs ORDER BY created_at;")
        return [dict(r) for r in cur.fetchall()]

    # ---------------- Stats ----------------
    def stats(self):
        con = connect(self.db_path)
        out = {}
        for s in VALID_STATES:
            cur = con.execute("SELECT COUNT(*) AS c FROM jobs WHERE state=?;", (s,))
            out[s] = cur.fetchone()["c"]
        cur = con.execute("SELECT COUNT(*) AS c FROM dlq;")
        out["dlq"] = cur.fetchone()["c"]
        cur = con.execute("SELECT AVG(duration_sec) AS avg_duration FROM jobs WHERE duration_sec > 0;")
        out["avg_duration"] = round(cur.fetchone()["avg_duration"] or 0, 2)
        return out

    # ---------------- Fetch Job for Worker ----------------
    def fetch_and_lease_one(self, worker_id: str, now_iso: str, lease_sec: int = 60):
        """
        Atomically pick one pending job whose run_at <= now.
        """
        con = connect(self.db_path)
        with tx(con):
            row = con.execute("""
                WITH picked AS (
                  SELECT id FROM jobs
                  WHERE state='pending' AND (run_at IS NULL OR run_at <= ?)
                  ORDER BY priority ASC, created_at ASC
                  LIMIT 1
                )
                UPDATE jobs
                SET state='processing',
                    worker_id=?,
                    lease_until=datetime(?, '+' || ? || ' seconds'),
                    updated_at=?,
                    exit_code=NULL
                WHERE id IN picked
                RETURNING *;
            """, (now_iso, worker_id, now_iso, lease_sec, now_iso)).fetchone()
            if row:
                return dict(row)
            return None

    # ---------------- Complete Job ----------------
    def complete(self, job_id: str, worker_id: str, start_time: datetime):
        con = connect(self.db_path)
        now = to_iso(utcnow())
        duration = round((utcnow() - start_time).total_seconds(), 3)
        with con:
            con.execute("""
                UPDATE jobs
                SET state='completed', worker_id=?, updated_at=?, lease_until=NULL,
                    duration_sec=?, exit_code=0
                WHERE id=? AND worker_id=?;
            """, (worker_id, now, duration, job_id, worker_id))

    # ---------------- Fail or Retry ----------------
    def fail_or_retry(self, job: dict, reason: str):
        import traceback
        con = connect(self.db_path)
        now = to_iso(utcnow())
        attempts = job["attempts"] + 1
        base = int(Config(self.db_path).base_backoff)
        delay = base ** attempts

        print(f"[DEBUG] fail_or_retry for job={job['id']} (attempt {attempts}/{job['max_retries']}) reason={reason}")

        if attempts >= job["max_retries"]:
            # --- Move to DLQ ---
            print("=" * 60)
            print(f"[DLQ DEBUG] Moving job {job['id']} to DLQ now!")
            print("=" * 60)
            try:
                con.execute("""
                    UPDATE jobs
                    SET state='dead', attempts=?, updated_at=?, lease_until=NULL, exit_code=1
                    WHERE id=?;
                """, (attempts, now, job["id"]))

                con.execute("""
                    INSERT OR REPLACE INTO dlq(id, job_json, reason, created_at)
                    VALUES(?,?,?,?);
                """, (job["id"], json.dumps(job), reason, now))
                con.commit()
                print(f"[DLQ DEBUG] ✅ Job {job['id']} inserted into DLQ")
            except Exception as e:
                print(f"[DLQ ERROR] {e}")
                traceback.print_exc()
        else:
            # --- Retry job with exponential backoff ---
            run_at = to_iso(add_seconds(utcnow(), delay))
            print(f"[Retry DEBUG] Retrying job {job['id']} in {delay}s (attempt {attempts})")
            con.execute("""
                UPDATE jobs
                SET state='pending', attempts=?, run_at=?, updated_at=?, lease_until=NULL,
                    worker_id=NULL, exit_code=1
                WHERE id=?;
            """, (attempts, run_at, now, job["id"]))
            con.commit()

    # ---------------- Retry from DLQ ----------------
    def retry_from_dlq(self, job_id: str):
        con = connect(self.db_path)
        row = con.execute("SELECT job_json FROM dlq WHERE id=?;", (job_id,)).fetchone()
        if not row:
            return False
        job = json.loads(row["job_json"])
        job["state"] = "pending"
        job["attempts"] = 0
        job["run_at"] = None
        job["updated_at"] = to_iso(utcnow())
        with tx(con):
            con.execute("DELETE FROM dlq WHERE id=?;", (job_id,))
            con.execute("""
                INSERT INTO jobs(id, command, state, attempts, max_retries,
                                 created_at, updated_at, run_at, priority, timeout_sec)
                VALUES(:id, :command, :state, :attempts, :max_retries,
                       :created_at, :updated_at, :run_at, :priority, :timeout_sec)
                ON CONFLICT(id) DO UPDATE SET
                  command=excluded.command,
                  state=excluded.state,
                  attempts=excluded.attempts,
                  max_retries=excluded.max_retries,
                  updated_at=excluded.updated_at,
                  run_at=excluded.run_at,
                  priority=excluded.priority,
                  timeout_sec=excluded.timeout_sec;
            """, job)
        return True

    # ---------------- List DLQ ----------------
    def list_dlq(self):
        con = connect(self.db_path)
        cur = con.execute("SELECT id, reason, created_at FROM dlq ORDER BY created_at;")
        return [dict(r) for r in cur.fetchall()]


# ---------------- Command Runner with Logging ----------------
def run_command(cmd: str, timeout_sec: int, job_id: Optional[str] = None) -> int:
    """
    Execute a shell command and save its stdout/stderr to logs/<job_id>.log.
    """
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{job_id or 'unknown'}.log")

    try:
        if platform.system() == "Windows":
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            proc = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        start_time = time.time()
        try:
            out, err = proc.communicate(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            if platform.system() == "Windows":
                proc.kill()
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            out, err = b"", b"[TIMEOUT] Job exceeded timeout limit."
            rc = 124
        else:
            rc = proc.returncode

        duration = round(time.time() - start_time, 2)
        with open(log_path, "wb") as f:
            f.write(b"[COMMAND]: " + cmd.encode() + b"\n\n")
            f.write(b"[DURATION]: " + str(duration).encode() + b" seconds\n\n")
            f.write(b"[EXIT CODE]: " + str(rc).encode() + b"\n\n")
            f.write(b"[STDOUT]\n" + out + b"\n\n")
            f.write(b"[STDERR]\n" + err + b"\n")

        return rc

    except FileNotFoundError:
        with open(log_path, "a") as f:
            f.write("[ERROR] Command not found.\n")
        return 127
    except Exception as e:
        with open(log_path, "a") as f:
            f.write(f"[ERROR] {e}\n")
        return 1
