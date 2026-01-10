import hashlib

def sha1_hash(key: str, m: int = 64) -> int:
    """Hashes a string key to an m-bit integer."""
    sha1 = hashlib.sha1(key.encode('utf-8')).digest()
    # Convert bytes to int
    val = int.from_bytes(sha1, byteorder='big')
    # Truncate to m-bits
    return val % (2**m)

def in_interval(id: int, start: int, end: int, m: int = 64, right_inclusive=False) -> bool:
    """
    Checks if id is in the interval (start, end).
    Handles wrap-around logic of the ring.
    """
    start = start % (2**m)
    end = end % (2**m)
    id = id % (2**m)

    if start == end:
        return True # Full circle implied if not distinguished, usually logic handles this elsewhere

    if start < end:
        if right_inclusive:
            return start < id <= end
        return start < id < end
    else:
        # Wrap around case
        if right_inclusive:
            return start < id or id <= end
        return start < id or id < end