import hashlib


def hash_key(key: str, m: int) -> int:
    h = hashlib.sha1(key.encode()).hexdigest()
    return int(h, 16) % (2 ** m)