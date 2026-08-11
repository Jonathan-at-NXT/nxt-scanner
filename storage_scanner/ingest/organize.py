"""Zielpfad-Bau + kollisions-sicheres Flatten in die NXT-Konvention.

Struktur wird plattgemacht: Medien landen flach in
`YYYY-MM-DD_Projekt_TYP / DATE / CAMERA /`, Video->FOOTAGE, Foto->PHOTOS.
Beim Flatten können gleiche Dateinamen aus verschiedenen Kartenordnern
kollidieren (DCIM-Rollover, Spanned Clips). Harte Regel: NIE überschreiben —
bei gleichem Namen + anderem Inhalt eindeutig suffixen; gleicher Inhalt = Dedup.
"""

import os
from pathlib import Path

from .verify import fingerprint

_TYP = {"video": "FOOTAGE", "photo": "PHOTOS"}


def target_dir(dest_root: Path, project_slug: str, date: str,
               media_type: str, camera: str) -> Path:
    """`dest_root / {date}_{slug}_{TYP} / {date} / {camera}`."""
    typ = _TYP[media_type]
    return Path(dest_root) / f"{date}_{project_slug}_{typ}" / date / camera


def _candidate_names(filename: str):
    """Erzeugt Namensvarianten: name.ext, name__2.ext, name__3.ext, ..."""
    stem, ext = os.path.splitext(filename)
    i = 1
    while True:
        yield filename if i == 1 else f"{stem}__{i}{ext}"
        i += 1


def _resolve(dest_dir: Path, src: Path, seen: dict) -> Path:
    """Bestimmt einen kollisions-sicheren Zielpfad für src in dest_dir.

    seen: dict {Path -> fingerprint} bereits verplanter Ziele (in dieser Runde).
    """
    fp = fingerprint(src)
    for name in _candidate_names(src.name):
        cand = dest_dir / name
        if cand in seen:
            if seen[cand] == fp:
                return cand          # gleicher Inhalt -> Dedup auf dasselbe Ziel
            continue                 # anderer Inhalt -> nächster Name
        if cand.exists():
            if fingerprint(cand) == fp:
                seen[cand] = fp
                return cand          # bereits identisch vorhanden -> resumable
            continue                 # belegt mit anderem Inhalt -> nächster Name
        seen[cand] = fp
        return cand


def plan_flatten(media: list[Path], dest_dir: Path) -> list[tuple[Path, Path]]:
    """Plant (src, final_dst) für alle Medien in ein flaches dest_dir.

    Kopiert nichts — reine Zielauflösung mit Kollisions-Sicherheit.
    """
    dest_dir = Path(dest_dir)
    seen: dict = {}
    return [(src, _resolve(dest_dir, src, seen)) for src in media]
