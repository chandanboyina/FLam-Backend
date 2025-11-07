from datetime import datetime, timezone, timedelta

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"

def utcnow():
    return datetime.now(timezone.utc)

def to_iso(dt: datetime):
    return dt.astimezone(timezone.utc).strftime(ISO_FMT)

def from_iso(s: str):
    return datetime.strptime(s, ISO_FMT).replace(tzinfo=timezone.utc)

def add_seconds(dt: datetime, sec: int):
    return dt + timedelta(seconds=sec)
