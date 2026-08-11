"""Schnelle Kopie-Verifikation via stdlib.

Phase A: Größe + gesampelter blake2b-Fingerprint (head/mid/tail). Reicht fürs
Board und den Ingest-Schutz; volle Byte-Identität (SHA-256 über alles) folgt in
Phase B. Kein neuer Dependency (kein xxhash).
"""

import hashlib
from pathlib import Path

_SAMPLE = 4 * 1024 * 1024  # 4 MiB pro Region


def fingerprint(path: Path, sample_bytes: int = _SAMPLE) -> str:
    """blake2b über Dateigröße + head/mid/tail-Sample.

    Kleine Dateien (<= 3*sample) werden vollständig gehasht.
    """
    path = Path(path)
    size = path.stat().st_size
    h = hashlib.blake2b(digest_size=16)
    h.update(str(size).encode())

    with path.open("rb") as f:
        if size <= 3 * sample_bytes:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        else:
            # head
            h.update(f.read(sample_bytes))
            # mid (zentriert)
            f.seek(size // 2 - sample_bytes // 2)
            h.update(f.read(sample_bytes))
            # tail
            f.seek(size - sample_bytes)
            h.update(f.read(sample_bytes))
    return h.hexdigest()


def verify_copy(src: Path, dst: Path) -> bool:
    """True, wenn dst dieselbe Größe UND denselben Fingerprint wie src hat."""
    src, dst = Path(src), Path(dst)
    if not dst.exists():
        return False
    if src.stat().st_size != dst.stat().st_size:
        return False
    return fingerprint(src) == fingerprint(dst)
