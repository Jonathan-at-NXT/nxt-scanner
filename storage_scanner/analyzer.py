"""Ordneranalyse – Größe und Dateianzahl rekursiv berechnen.

Nutzt native CLI-Tools (du, find) für deutlich bessere Performance auf
großen HDDs / Spinning Disks. Fallback auf Python os.scandir() falls
die CLI-Aufrufe fehlschlagen.
"""

import os
import subprocess
from pathlib import Path


def analyze_folder(path: Path) -> dict:
    """Berechnet rekursiv Ordnergröße (Bytes) und Dateianzahl.

    Nutzt 'du -sk' und 'find -type f' für schnelle Analyse auf großen HDDs.
    Fällt auf Python os.scandir() zurück falls CLI-Tools fehlschlagen.

    Returns:
        {"size_bytes": int, "file_count": int}
    """
    size_bytes = _du_size(str(path))
    file_count = _find_count(str(path))

    # Fallback auf Python wenn CLI fehlschlägt
    if size_bytes is None or file_count is None:
        size_ref, count_ref = [0], [0]
        try:
            _scan_recursive(str(path), size_ref, count_ref)
        except PermissionError:
            pass
        if size_bytes is None:
            size_bytes = size_ref[0]
        if file_count is None:
            file_count = count_ref[0]

    return {
        "size_bytes": size_bytes,
        "file_count": file_count,
    }


def _du_size(path: str) -> int | None:
    """Ordnergröße via 'du -sk' (nutzt Filesystem-Metadaten, viel schneller).

    Gibt None zurück bei Timeout, Fehlern oder exFAT-Problemen (Invalid argument).
    """
    try:
        result = subprocess.run(
            ["du", "-sk", path],
            capture_output=True, text=True, timeout=120,
        )
        # exFAT-Fehler auf stderr erkennen (du gibt trotzdem exit 0 zurück)
        if result.stderr and "Invalid argument" in result.stderr:
            return None
        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.split()[0]) * 1024
    except (subprocess.TimeoutExpired, ValueError, IndexError, OSError):
        pass
    return None


def _find_count(path: str) -> int | None:
    """Dateianzahl via 'find -type f | wc -l' (schneller als Python-Rekursion)."""
    find_proc = None
    wc_proc = None
    try:
        find_proc = subprocess.Popen(
            ["find", path, "-type", "f"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        wc_proc = subprocess.Popen(
            ["wc", "-l"],
            stdin=find_proc.stdout, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        find_proc.stdout.close()
        output, _ = wc_proc.communicate(timeout=120)
        find_proc.wait(timeout=5)
        return int(output.strip())
    except (subprocess.TimeoutExpired, ValueError, OSError):
        for proc in (find_proc, wc_proc):
            if proc is not None:
                try:
                    proc.kill()
                except OSError:
                    pass
    return None


def _scan_recursive(path: str, size_ref: list[int], count_ref: list[int]) -> None:
    """Fallback: Rekursiver Scan mit os.scandir()."""
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
