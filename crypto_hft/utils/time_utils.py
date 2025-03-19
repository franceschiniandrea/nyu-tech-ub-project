import datetime
import ciso8601

def iso8601_to_unix(timestamp: str) -> float:
    """Convert ISO 8601 formatted timestamp to Unix timestamp."""
    dt = ciso8601.parse_datetime(timestamp)
    return dt.timestamp()

def unix_to_mysql_datetime(unix_time: float) -> str:
    """Convert Unix timestamp to MySQL DATETIME(6) format."""
    dt = datetime.datetime.utcfromtimestamp(unix_time)
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{dt.microsecond:06d}"
