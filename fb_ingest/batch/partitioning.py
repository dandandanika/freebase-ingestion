from __future__ import annotations

import hashlib


def stable_partition(key: str, partition_count: int) -> int:
    """
    Stable deterministic partition assignment.
    """
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % partition_count
    