"""Chunked Copy zu zwei Zielen, resumable und atomar.

Kopiert nach `dst.tmp` und benennt erst nach vollständiger Übertragung um
(atomarer rename) — nie eine halbe Datei am Zielnamen. Existiert bereits eine
verifizierte Kopie, wird sie übersprungen (resumable). Quelle wird nur gelesen.
"""

import os
import shutil
from pathlib import Path

from .verify import verify_copy

_CHUNK = 8 * 1024 * 1024  # 8 MiB


def copy_file(src: Path, dst: Path, chunk: int = _CHUNK, progress=None) -> bool:
    """Kopiert src->dst chunked+atomar. Gibt True bei tatsächlicher Kopie,
    False wenn übersprungen (bereits verifiziert vorhanden)."""
    src, dst = Path(src), Path(dst)
    if verify_copy(src, dst):
        return False  # resumable: identische Kopie existiert schon

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    with src.open("rb") as fin, tmp.open("wb") as fout:
        while True:
            buf = fin.read(chunk)
            if not buf:
                break
            fout.write(buf)
            if progress:
                progress(len(buf))
    shutil.copystat(src, tmp)
    os.replace(tmp, dst)  # atomar
    return True


def copy_to_targets(pairs_a: list[tuple[Path, Path]],
                    pairs_b: list[tuple[Path, Path]],
                    progress=None) -> dict:
    """Kopiert dieselben Quellen nach Ziel A und Ziel B und verifiziert beide.

    pairs_a/pairs_b: Listen (src, dst) — gleiche Quellen, verschiedene Zielbäume.
    """
    copied = 0
    verified_a = True
    verified_b = True
    for src, dst in pairs_a:
        if copy_file(src, dst, progress=progress):
            copied += 1
        if not verify_copy(src, dst):
            verified_a = False
    for src, dst in pairs_b:
        if copy_file(src, dst, progress=progress):
            copied += 1
        if not verify_copy(src, dst):
            verified_b = False
    return {"copied": copied, "verified_a": verified_a, "verified_b": verified_b}
