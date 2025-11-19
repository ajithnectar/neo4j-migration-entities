from datetime import datetime, timezone

def convert_epoch_to_timestamp(value):
    """
    Convert long epoch millis/seconds to timezone-aware timestamp.
    If value is already a datetime, return as is.
    If value is None or empty, return None.
    """
    if not value:
        return None

    try:
        value = int(value)
    except ValueError:
        return None

    # Detect milliseconds vs seconds
    if value > 1_000_000_000_000:  # > 2001 in millis
        dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    else:
        dt = datetime.fromtimestamp(value, tz=timezone.utc)

    return dt
