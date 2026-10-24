"""Storage Scanner – CLI-Einstiegspunkt.

Scannt Datenträger, erkennt Projektordner anhand einer Naming-Convention,
berechnet Ordnergrößen/Dateianzahl und erzeugt einen JSON-Report.
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

from .rules import validate_folder
from .analyzer import analyze_folder, walk_tree, is_network_volume
from .report import generate_report, save_report
from .utils import format_size


class ScanIncomplete(Exception):
    """Scan hatte Lesefehler oder die Platte verschwand: Ergebnis wird nicht verwendet."""


def _build_folder_tree(subfolders, scan_path, incomplete: list | None = None) -> list[dict]:
    """Baut den flachen folder_tree (disk-relative rel_paths) aus den
    Top-Level-Ordnern. Netzlaufwerke: nur Top-Level (sub_depth=0).
    Ordner, deren Baum nicht ermittelt werden konnte, landen in `incomplete`."""
    sub_depth = 0 if is_network_volume(str(scan_path)) else 2
    tree = []
    for folder in subfolders:
        nodes = walk_tree(folder, sub_depth=sub_depth)
        if not nodes and incomplete is not None:
            incomplete.append(folder.name)
        for node in nodes:
            if node["rel_path"] == "":
                rel = folder.name
            else:
                rel = f"{folder.name}/{node['rel_path']}"
            if node["parent_rel_path"] is None:
                parent = None
            elif node["parent_rel_path"] == "":
                parent = folder.name
            else:
                parent = f"{folder.name}/{node['parent_rel_path']}"
            tree.append({
                "rel_path": rel,
                # realer Pfad: rel_path lässt Container wie "NXT STUDIOS/" weg
                "abs_path": str(folder / node["rel_path"]) if node["rel_path"] else str(folder),
                "depth": node["depth"] + 1,   # +1: Top-Level ist Ebene 1 unter Volume-Root
                "parent_rel_path": parent,
                "size_bytes": node["size_bytes"],
                "file_count": node["file_count"],
                "mtime": node["mtime"],
            })
    return tree


def _safe_mtime(path: Path) -> str:
    """stat().st_mtime mit Fallback – exFAT-Ordner mit Sonderzeichen können EINVAL werfen."""
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat()
    except OSError:
        return datetime.now().isoformat()


def _safe_listdir(path: Path) -> list[Path]:
    """Listet Unterordner auf und überspringt Einträge mit defekten Metadaten (exFAT EINVAL)."""
    result = []
    try:
        for entry in path.iterdir():
            if entry.name.startswith(".") or entry.name.startswith("@"):
                continue
            try:
                if entry.is_dir():
                    result.append(entry)
            except OSError:
                continue
    except OSError:
        pass
    return sorted(result, key=lambda p: p.name)


def main():
    parser = argparse.ArgumentParser(
        description="Scannt einen Datenträger und erkennt Projektordner anhand der Naming-Convention."
    )
    parser.add_argument("path", help="Pfad zum Datenträger oder Ordner")
    parser.add_argument(
        "-o", "--output",
        default="./scan_report.json",
        help="Pfad für den JSON-Report (default: ./scan_report.json)",
    )
    args = parser.parse_args()

    scan_path = Path(args.path).resolve()
    output_path = Path(args.output).resolve()

    # Pfad validieren
    if not scan_path.exists():
        print(f"Fehler: Pfad existiert nicht: {scan_path}", file=sys.stderr)
        sys.exit(1)
    if not scan_path.is_dir():
        print(f"Fehler: Pfad ist kein Verzeichnis: {scan_path}", file=sys.stderr)
        sys.exit(1)

    # Alle direkten Unterordner auflisten (nur 1. Ebene, ohne versteckte)
    # "NXT STUDIOS"- und "OLDER"-Ordner transparent auflösen → deren Inhalt stattdessen scannen
    raw_folders = _safe_listdir(scan_path)
    subfolders = []
    for folder in raw_folders:
        if folder.name.upper() in {"NXT STUDIOS", "OLDER"}:
            subfolders.extend(_safe_listdir(folder))
        else:
            subfolders.append(folder)

    if not subfolders:
        print("Keine Unterordner gefunden.")
        sys.exit(0)

    projects = []
    unassigned = []

    print(f"Scanne {len(subfolders)} Ordner in: {scan_path}\n")

    for folder in tqdm(subfolders, desc="Analysiere Ordner", unit="Ordner"):
        try:
            validation = validate_folder(folder.name)
            stats = analyze_folder(folder)

            entry = {
                "name": folder.name,
                "absolute_path": str(folder),
                "size_bytes": stats["size_bytes"],
                "size_human": format_size(stats["size_bytes"]),
                "file_count": stats["file_count"],
                "last_modified": _safe_mtime(folder),
            }

            if validation:
                entry["date"] = validation["date"]
                entry["project_name"] = validation["project_name"]
                entry["type"] = validation["type"]

                if validation["type"] == "PROJECT":
                    children = []
                    child_folders = _safe_listdir(folder)
                    for child_folder in child_folders:
                        try:
                            child_validation = validate_folder(child_folder.name)
                            child_stats = analyze_folder(child_folder)
                            child_entry = {
                                "name": child_folder.name,
                                "absolute_path": str(child_folder),
                                "size_bytes": child_stats["size_bytes"],
                                "size_human": format_size(child_stats["size_bytes"]),
                                "file_count": child_stats["file_count"],
                                "last_modified": _safe_mtime(child_folder),
                            }
                            if child_validation:
                                child_entry["date"] = child_validation["date"]
                                child_entry["project_name"] = child_validation["project_name"]
                                child_entry["type"] = child_validation["type"]
                            children.append(child_entry)
                        except OSError as e:
                            print(f"  Übersprungen (Fehler): {child_folder.name} – {e}")
                            continue
                    entry["children"] = children

                projects.append(entry)
            else:
                unassigned.append(entry)
        except OSError as e:
            print(f"  Übersprungen (Fehler): {folder.name} – {e}")
            continue

    # Report generieren und speichern
    report = generate_report(str(scan_path), projects, unassigned)
    report["folder_tree"] = _build_folder_tree(subfolders, scan_path)
    save_report(report, output_path)

    # Zusammenfassung im Terminal
    total_size = sum(p["size_bytes"] for p in projects) + sum(u["size_bytes"] for u in unassigned)
    print(f"\n{'=' * 50}")
    print(f"Scan abgeschlossen: {scan_path}")
    print(f"{'=' * 50}")
    print(f"  Ordner gesamt:     {len(subfolders)}")
    print(f"  Gültige Projekte:  {len(projects)}")
    print(f"  Unassigned:        {len(unassigned)}")
    print(f"  Gesamtgröße:       {format_size(total_size)}")
    print(f"  Report gespeichert: {output_path}")


def run_scan(volume_path: str, output_path: str) -> None:
    """Programmatischer Einstiegspunkt für Scans (ohne argparse/sys.exit).

    Args:
        volume_path: Pfad zum Datenträger (z.B. /Volumes/NXT 005).
        output_path: Pfad für den JSON-Report.

    Raises:
        FileNotFoundError: Wenn der Pfad nicht existiert.
        NotADirectoryError: Wenn der Pfad kein Verzeichnis ist.
        ScanIncomplete: Bei Lesefehlern oder wenn die Platte während des Scans
            verschwand. Der vorhandene (letzte gute) Report bleibt dann unangetastet;
            das Teilergebnis liegt zur Analyse unter <report>.incomplete.json.
    """
    scan_path = Path(volume_path).resolve()
    out = Path(output_path).resolve()

    if not scan_path.exists():
        raise FileNotFoundError(f"Pfad existiert nicht: {scan_path}")
    if not scan_path.is_dir():
        raise NotADirectoryError(f"Pfad ist kein Verzeichnis: {scan_path}")

    # Oberste Ebene muss lesbar sein; sonst ist es kein leerer Datenträger, sondern ein
    # Rechteproblem (z. B. macOS-Freigabe "Wechseldatenträger" fehlt) -> Fehler statt still nichts
    try:
        next(iter(os.scandir(scan_path)), None)
    except PermissionError as e:
        raise ScanIncomplete(f"Kein Lesezugriff auf {scan_path} – in den Systemeinstellungen unter "
                             f"Datenschutz & Sicherheit → Dateien und Ordner den Zugriff auf Wechseldatenträger erlauben") from e
    raw_folders = _safe_listdir(scan_path)
    subfolders = []
    for folder in raw_folders:
        if folder.name.upper() in {"NXT STUDIOS", "OLDER"}:
            subfolders.extend(_safe_listdir(folder))
        else:
            subfolders.append(folder)

    # Leerer Datenträger: trotzdem einen (leeren) Report schreiben, damit er als gescannt gilt

    projects = []
    unassigned = []
    incomplete: list[str] = []
    # Fortschritt ans Dashboard: Ordner X von Y (Scan-Phase vor der Datei-Erfassung)
    from .progress import Reporter
    from .utils import get_volume_uuid
    rep = Reporter("scan", get_volume_uuid(str(scan_path)) or f"name:{scan_path.name}", scan_path.name,
                   total_files=len(subfolders), interval=5)

    for i, folder in enumerate(subfolders):
        rep.update(i, 0, message=f"Ordner {i + 1}/{len(subfolders)}: {folder.name[:60]}")
        try:
            validation = validate_folder(folder.name)
            stats = analyze_folder(folder)
            if not stats.get("complete", True):
                incomplete.append(folder.name)

            entry = {
                "name": folder.name,
                "absolute_path": str(folder),
                "size_bytes": stats["size_bytes"],
                "size_human": format_size(stats["size_bytes"]),
                "file_count": stats["file_count"],
                "last_modified": _safe_mtime(folder),
            }

            if validation:
                entry["date"] = validation["date"]
                entry["project_name"] = validation["project_name"]
                entry["type"] = validation["type"]

                if validation["type"] == "PROJECT":
                    children = []
                    child_folders = _safe_listdir(folder)
                    for child_folder in child_folders:
                        try:
                            child_validation = validate_folder(child_folder.name)
                            child_stats = analyze_folder(child_folder)
                            if not child_stats.get("complete", True):
                                incomplete.append(f"{folder.name}/{child_folder.name}")
                            child_entry = {
                                "name": child_folder.name,
                                "absolute_path": str(child_folder),
                                "size_bytes": child_stats["size_bytes"],
                                "size_human": format_size(child_stats["size_bytes"]),
                                "file_count": child_stats["file_count"],
                                "last_modified": _safe_mtime(child_folder),
                            }
                            if child_validation:
                                child_entry["date"] = child_validation["date"]
                                child_entry["project_name"] = child_validation["project_name"]
                                child_entry["type"] = child_validation["type"]
                            children.append(child_entry)
                        except OSError as e:
                            print(f"  Übersprungen (Fehler): {child_folder.name} – {e}")
                            incomplete.append(f"{folder.name}/{child_folder.name}")
                            continue
                    entry["children"] = children

                projects.append(entry)
            else:
                unassigned.append(entry)
        except OSError as e:
            print(f"  Übersprungen (Fehler): {folder.name} – {e}")
            incomplete.append(folder.name)
            continue

    report = generate_report(str(scan_path), projects, unassigned)
    report["folder_tree"] = _build_folder_tree(subfolders, scan_path, incomplete)

    # Platte muss am Ende noch dieselbe sein (nicht abgezogen/neu gemountet)
    if not os.path.ismount(scan_path) and str(scan_path).startswith("/Volumes/"):
        incomplete.append("<Datenträger nicht mehr gemountet>")

    rep.finish(len(subfolders), 0, ok=not incomplete, message=None if not incomplete else f"{len(set(incomplete))} Ordner unvollständig")
    report["scan_info"]["complete"] = not incomplete
    report["scan_info"]["incomplete_folders"] = sorted(set(incomplete))
    if incomplete:
        save_report(report, out.with_name(out.stem + ".incomplete.json"))
        raise ScanIncomplete(
            f"Scan unvollständig ({len(set(incomplete))} Ordner nicht sicher lesbar, "
            f"z. B. {sorted(set(incomplete))[0]}) – nichts hochgeladen")
    save_report(report, out)


if __name__ == "__main__":
    main()
