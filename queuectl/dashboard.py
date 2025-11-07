import os
import sqlite3
from flask import Flask, render_template, jsonify
from .db import DEFAULT_DB_PATH

DB_PATH = r"C:\Users\HP\OneDrive\Desktop\queuectl\queuectl\queue.db"

app = Flask(__name__, template_folder="templates")

# =============================
# Helper: Connect to database
# =============================
def get_conn(db_path=DEFAULT_DB_PATH):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


# =============================
# Routes
# =============================
@app.route("/")
def home():
    return render_template("dashboard.html")


@app.route("/api/stats")
def stats():
    con = get_conn()
    cur = con.cursor()

    def count(state):
        cur.execute("SELECT COUNT(*) AS c FROM jobs WHERE state=?;", (state,))
        row = cur.fetchone()
        return row["c"] if row else 0

    # ✅ Fixed DLQ count query (only one fetch)
    cur.execute("SELECT COUNT(*) AS c FROM dlq;")
    row = cur.fetchone()
    dlq_count = row["c"] if row else 0

    # ✅ Handle no durations safely
    cur.execute("SELECT AVG(duration_sec) FROM jobs WHERE duration_sec > 0;")
    avg_row = cur.fetchone()
    avg_duration = avg_row[0] if avg_row and avg_row[0] else 0.0

    data = {
        "pending": count("pending"),
        "processing": count("processing"),
        "completed": count("completed"),
        "failed": count("failed"),
        "dead": count("dead"),
        "dlq": dlq_count,
        "avg_duration": round(avg_duration, 2),
    }

    con.close()
    return jsonify(data)


@app.route("/api/jobs")
def jobs():
    con = get_conn()
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, command, state, attempts, max_retries, priority, 
               exit_code, duration_sec, run_at, updated_at 
        FROM jobs ORDER BY updated_at DESC LIMIT 20;
        """
    )
    jobs = [dict(row) for row in cur.fetchall()]
    con.close()
    return jsonify(jobs)


@app.route("/api/dlq")
def dlq():
    con = get_conn()
    cur = con.cursor()
    cur.execute("SELECT id, reason, created_at FROM dlq ORDER BY created_at DESC LIMIT 20;")
    dlq = [dict(row) for row in cur.fetchall()]
    con.close()
    return jsonify(dlq)


# =============================
# Launch dashboard programmatically
# =============================
def start_dashboard(db_path=DEFAULT_DB_PATH, port=8080):
    app.config["DB_PATH"] = db_path
    print(f"[INFO] Dashboard running at http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=True)


# =============================
# Direct launch (python -m queuectl.dashboard)
# =============================
if __name__ == "__main__":
    db_path = os.environ.get("QUEUECTL_DB", DEFAULT_DB_PATH)
    start_dashboard(db_path)
