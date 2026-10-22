"""Kopie einer Karte auf zwei Ziele über das sichere Transfer-Modul.

Delegiert an storage_scanner.transfer.safe_copy: eine Quelle wird einmal
gelesen und parallel auf beide Ziele geschrieben, jedes Ziel landet erst nach
F_FULLFSYNC + vollständigem Hash-Abgleich unter seinem Namen. Vorhandene
identische Kopien werden übersprungen (resumable), fremde Dateien am Ziel nie
überschrieben. Quelle wird nur gelesen.
"""

from pathlib import Path

from ..transfer import safe_copy
from .verify import verify_copy


def copy_file(src: Path, dst: Path, progress=None) -> bool:
    """Kopiert src->dst sicher. True bei tatsächlicher Kopie, False wenn
    übersprungen (identische Kopie vorhanden)."""
    return bool(safe_copy(Path(src), [Path(dst)], progress=progress)["copied"])


def copy_to_targets(pairs_a: list[tuple[Path, Path]],
                    pairs_b: list[tuple[Path, Path]],
                    progress=None) -> dict:
    """Kopiert dieselben Quellen nach Ziel A und Ziel B (eine Lesung pro Quelle)
    und prüft danach beide Ziele.

    pairs_a/pairs_b: Listen (src, dst) — gleiche Quellen, verschiedene Zielbäume.
    """
    targets: dict[Path, list[Path]] = {}
    for src, dst in list(pairs_a) + list(pairs_b):
        targets.setdefault(Path(src), []).append(Path(dst))

    copied = 0
    for src, dsts in targets.items():
        copied += len(safe_copy(src, dsts, progress=progress)["copied"])

    verified_a = all(verify_copy(s, d) for s, d in pairs_a)
    verified_b = all(verify_copy(s, d) for s, d in pairs_b)
    return {"copied": copied, "verified_a": verified_a, "verified_b": verified_b}
