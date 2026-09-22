"""Versioned report exports and byte integrity, independent of the database."""

import hashlib

ALGORITHM_VERSION = 4


def export_matches(path, expected_digest):
    if not expected_digest:
        return False
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() == expected_digest
    except OSError:
        return False
