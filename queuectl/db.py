import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

# ============================================================
# 🗂️ Default Database Path
# ============================================================
DEFAULT_DB_PATH = os.environ.get("QUEUECTL_DB", "queue.db")

# ============================================================
# 🧱 Database Schema with Bonus Features
# ============================================================
SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ============================================================
-- JOBS TABLE
-- Tracks all queued jobs, including metrics and execution data
-- ============================================================
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  command TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN ('pending','processing','completed','failed','dead')),
  attempts INTEGER NOT NULL DEFAULT 0,
  max_retries INTEGER NOT NULL DEFAULT 3,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  run_at TEXT,                        -- for backoff/scheduled jobs
  lease_until TEXT,                   -- job lease timeout
  worker_id TEXT,                     -- worker that processed it
  priority INTEGER NOT NULL DEFAULT 100, -- lower number = higher priority
  timeout_sec INTEGER,                -- per-job timeout override
  duration_sec REAL DEFAULT 0.0,      -- total execution time
  exit_code INTEGER DEFAULT NULL      -- command return code
);

-- ============================================================
-- DEAD LETTER QUEUE (DLQ)
-- Stores failed jobs after max retries
-- ============================================================
CREATE TABLE IF NOT EXISTS dlq (
  id TEXT PRIMARY KEY,
  job_json TEXT NOT NULL,
  reason TEXT,
  created_at TEXT NOT NULL
);

-- ============================================================
-- CONFIG TABLE
-- Stores runtime configurations (backoff, retries, etc.)
-- ============================================================
CREATE TABLE IF NOT EXISTS config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- ============================================================
-- METRICS TABLE
-- Optional: Tracks historical job statistics for monitoring
-- ============================================================
CREATE TABLE IF NOT EXISTS metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  total_jobs INTEGER DEFAULT 0,
  completed_jobs INTEGER DEFAULT 0,
  failed_jobs INTEGER DEFAULT 0,
  avg_duration REAL DEFAULT 0,
  last_updated TEXT NOT NULL
);

-- ============================================================
-- DEFAULT CONFIG VALUES
-- ============================================================
INSERT OR IGNORE INTO config(key, value) VALUES
 ('base_backoff','2'),
 ('default_max_retries','3'),
 ('job_timeout_sec','60');
"""

# ============================================================
# 🧰 Utility Functions
# ============================================================
def ensure_dir(path: str):
    """Ensure that the directory for the database exists."""
    p = Path(path).resolve().parent
    p.mkdir(parents=True, exist_ok=True)


def connect(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """
    Connect to SQLite database with WAL and busy timeout.
    """
    ensure_dir(db_path)
    con = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=5000;")
    return con


def init_db(db_path: str = DEFAULT_DB_PATH):
    """
    Initialize database with schema and default configuration.
    """
    con = connect(db_path)
    with con:
        con.executescript(SCHEMA)

        # Initialize metrics if not present
        cur = con.execute("SELECT COUNT(*) AS c FROM metrics;").fetchone()
        if cur["c"] == 0:
            con.execute("""
                INSERT INTO metrics(total_jobs, completed_jobs, failed_jobs, avg_duration, last_updated)
                VALUES(0, 0, 0, 0, datetime('now'));
            """)
    return con


# ============================================================
# 🔒 Transaction Context Manager
# ============================================================
@contextmanager
def tx(con: sqlite3.Connection):
    """
    Execute statements in a transaction with automatic rollback on error.
    """
    con.execute("BEGIN IMMEDIATE;")
    try:
        yield
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise


# ============================================================
# 📊 Metrics Updater (Bonus Utility)
# ============================================================
def update_metrics(con: sqlite3.Connection):
    """
    Recalculate and update job execution metrics.
    """
    total = con.execute("SELECT COUNT(*) AS c FROM jobs;").fetchone()["c"]
    completed = con.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='completed';").fetchone()["c"]
    failed = con.execute("SELECT COUNT(*) AS c FROM jobs WHERE state='failed';").fetchone()["c"]
    avg_duration = con.execute("SELECT AVG(duration_sec) AS a FROM jobs WHERE duration_sec > 0;").fetchone()["a"]
    avg_duration = round(avg_duration or 0, 2)

    with con:
        con.execute("""
            UPDATE metrics
            SET total_jobs=?, completed_jobs=?, failed_jobs=?, avg_duration=?, last_updated=datetime('now')
            WHERE id=1;
        """, (total, completed, failed, avg_duration))
