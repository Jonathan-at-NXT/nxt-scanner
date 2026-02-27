"""Ordneranalyse – Größe und Dateianzahl rekursiv berechnen."""

import os
from pathlib import Path


def analyze_folder(path: Path) -> dict:
    """Berechnet rekursiv Ordnergröße (Bytes) und Dateianzahl.

    Nutzt os.scandir() für Performance.

    Returns:
        {"size_bytes": int, "file_count": int}
    """
    total_size = 0
    file_count = 0

    try:
        _scan_recursive(str(path), total_size_ref := [0], file_count_ref := [0])
    except PermissionError:
        pass

    return {
        "size_bytes": total_size_ref[0],
        "file_count": file_count_ref[0],
    }


def _scan_recursive(path: str, size_ref: list[int], count_ref: list[int]) -> None:
    """Rekursiver Scan mit os.scandir()."""
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                try:
                    if entry.is_file(follow_symlinks=False):
                        size_ref[0] += entry.stat(follow_symlinks=False).st_size
                        count_ref[0] += 1
                    elif entry.is_dir(follow_symlinks=False):
                        _scan_recursive(entry.path, size_ref, count_ref)
                except (PermissionError, OSError):
                    continue
    except (PermissionError, OSError):
        pass
