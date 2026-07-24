"""Ordneranalyse – Größe und Dateianzahl rekursiv berechnen.

Nutzt native CLI-Tools (du, find) für deutlich bessere Performance auf
großen HDDs / Spinning Disks. Fallback auf Python os.scandir() falls
die CLI-Aufrufe fehlschlagen.
"""

import os
import subprocess
from collections import defaultdict
from datetime import datetime
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


def is_network_volume(path: str) -> bool:
    """True wenn der Mount ein Netzlaufwerk ist (smbfs/nfs/afpfs/webdav)."""
    try:
        mr = subprocess.run(["mount"], capture_output=True, text=True, timeout=10)
        target = str(path).rstrip("/")
        for line in mr.stdout.splitlines():
            # Format: <src> on <mountpoint> (<fstype>, ...)
            if f" on {target} " in line or f" on {target}/" in line:
                return any(fs in line for fs in ("smbfs", "nfs", "afpfs", "webdav"))
    except (subprocess.TimeoutExpired, OSError):
        pass
    return False


def _safe_iso_mtime(p: str) -> str:
    try:
        return datetime.fromtimestamp(os.path.getmtime(p)).isoformat()
    except OSError:
        return datetime.now().isoformat()


def walk_tree(root, sub_depth: int = 2, timeout: int = 180) -> list[dict]:
    """Erfasst den Ordnerbaum ab root bis sub_depth Ebenen (kumulativ).

    depth 0 == root selbst (rel_path ""). Größen via 'du -d', Dateizahlen via
    'find'. Größen/Dateizahlen sind kumulativ (ganzer Teilbaum). Leere Liste bei
    Timeout/Fehler.
    """
    root = Path(root)
    root_str = os.path.normpath(str(root))

    # 1) Größen je Knoten bis sub_depth (BSD du: -d Tiefe, -k KiB)
    sizes = {}
    try:
        r = subprocess.run(
            ["du", "-d", str(sub_depth), "-k", root_str],
            capture_output=True, text=True, timeout=timeout,
        )
        if r.stderr and "Invalid argument" in r.stderr:
            return []
        for line in r.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            kb, p = parts
            try:
                sizes[os.path.normpath(p)] = int(kb) * 1024
            except ValueError:
                continue
    except (subprocess.TimeoutExpired, OSError):
        return []

    if root_str not in sizes:
        return []

    # 2) Dateizahlen kumulativ je Knoten (find -> Vorfahren bis sub_depth zählen)
    counts = defaultdict(int)
    find_proc = None
    try:
        find_proc = subprocess.Popen(
            ["find", root_str, "-type", "f"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        )
        for bline in find_proc.stdout:
            fpath = os.path.normpath(bline.decode("utf-8", "replace").rstrip("\n"))
            d = os.path.dirname(fpath)
            rel = os.path.relpath(d, root_str)
            parts = [] if rel == "." else rel.split(os.sep)
            counts[root_str] += 1
            acc = root_str
            for seg in parts[:sub_depth]:
                acc = os.path.join(acc, seg)
                counts[acc] += 1
        find_proc.wait(timeout=timeout)
    except (subprocess.TimeoutExpired, OSError):
        if find_proc is not None:
            try:
                find_proc.kill()
            except OSError:
                pass

    # 3) Knoten bauen
    nodes = []
    for abspath, size in sizes.items():
        rel = os.path.relpath(abspath, root_str)
        rel = "" if rel == "." else rel.replace(os.sep, "/")
        depth = 0 if rel == "" else rel.count("/") + 1
        parent = None if depth == 0 else "/".join(rel.split("/")[:-1])
        nodes.append({
            "rel_path": rel,
            "depth": depth,
            "parent_rel_path": parent,
            "size_bytes": size,
            "file_count": counts.get(abspath, 0),
            "mtime": _safe_iso_mtime(abspath),
        })
    return nodes
