from .db import connect, init_db, DEFAULT_DB_PATH

class Config:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        init_db(db_path)

    def get(self, key: str) -> str | None:
        con = connect(self.db_path)
        cur = con.execute("SELECT value FROM config WHERE key=?;", (key,))
        row = cur.fetchone()
        return row["value"] if row else None

    def set(self, key: str, value: str):
        con = connect(self.db_path)
        with con:
            con.execute(
                "INSERT INTO config(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
                (key, value),
            )

    @property
    def base_backoff(self) -> int:
        return int(self.get("base_backoff") or "2")

    @property
    def default_max_retries(self) -> int:
        return int(self.get("default_max_retries") or "3")

    @property
    def job_timeout_sec(self) -> int:
        return int(self.get("job_timeout_sec") or "60")
