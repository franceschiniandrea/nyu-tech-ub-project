# time_utils.py
import ciso8601
from time import strftime, gmtime, time_ns, time as time_sec

def time_iso8601() -> str:
    millis = str((time_ns() % 1_000_000_000) // 1_000_000).zfill(3)
    return f"{strftime('%Y-%m-%dT%H:%M:%S', gmtime())}.{millis}Z"

def iso8601_to_unix(timestamp: str) -> float:
    return ciso8601.parse_datetime(timestamp).timestamp()