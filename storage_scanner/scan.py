"""Storage Scanner – CLI-Einstiegspunkt.

Scannt Datenträger, erkennt Projektordner anhand einer Naming-Convention,
berechnet Ordnergrößen/Dateianzahl und erzeugt einen JSON-Report.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

from .rules import validate_folder
from .analyzer import analyze_folder
from .report import generate_report, save_report
from .utils import format_size


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
    # "NXT STUDIOS"-Ordner transparent auflösen → dessen Inhalt stattdessen scannen
    raw_folders = sorted(
        [entry for entry in scan_path.iterdir() if entry.is_dir() and not entry.name.startswith(".")],
        key=lambda p: p.name,
    )
    subfolders = []
    for folder in raw_folders:
        if folder.name.upper() == "NXT STUDIOS":
            inner = sorted(
                [e for e in folder.iterdir() if e.is_dir() and not e.name.startswith(".")],
                key=lambda p: p.name,
            )
            subfolders.extend(inner)
        else:
            subfolders.append(folder)

    if not subfolders:
        print("Keine Unterordner gefunden.")
        sys.exit(0)

    projects = []
    unassigned = []

    print(f"Scanne {len(subfolders)} Ordner in: {scan_path}\n")

    for folder in tqdm(subfolders, desc="Analysiere Ordner", unit="Ordner"):
        # Naming-Convention prüfen
        validation = validate_folder(folder.name)

        # Größe und Dateianzahl berechnen
        stats = analyze_folder(folder)

        entry = {
            "name": folder.name,
            "absolute_path": str(folder),
            "size_bytes": stats["size_bytes"],
            "size_human": format_size(stats["size_bytes"]),
            "file_count": stats["file_count"],
            "last_modified": datetime.fromtimestamp(folder.stat().st_mtime).isoformat(),
        }

        if validation:
            entry["date"] = validation["date"]
            entry["project_name"] = validation["project_name"]
            entry["type"] = validation["type"]

            # PROJECT-Ordner: Eine Ebene tiefer scannen
            if validation["type"] == "PROJECT":
                children = []
                child_folders = sorted(
                    [e for e in folder.iterdir() if e.is_dir() and not e.name.startswith(".")],
                    key=lambda p: p.name,
                )
                for child_folder in child_folders:
                    child_validation = validate_folder(child_folder.name)
                    child_stats = analyze_folder(child_folder)
                    child_entry = {
                        "name": child_folder.name,
                        "absolute_path": str(child_folder),
                        "size_bytes": child_stats["size_bytes"],
                        "size_human": format_size(child_stats["size_bytes"]),
                        "file_count": child_stats["file_count"],
                        "last_modified": datetime.fromtimestamp(child_folder.stat().st_mtime).isoformat(),
                    }
                    if child_validation:
                        child_entry["date"] = child_validation["date"]
                        child_entry["project_name"] = child_validation["project_name"]
                        child_entry["type"] = child_validation["type"]
                    children.append(child_entry)
                entry["children"] = children

            projects.append(entry)
        else:
            unassigned.append(entry)

    # Report generieren und speichern
    report = generate_report(str(scan_path), projects, unassigned)
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
    """
    scan_path = Path(volume_path).resolve()
    out = Path(output_path).resolve()

    if not scan_path.exists():
        raise FileNotFoundError(f"Pfad existiert nicht: {scan_path}")
    if not scan_path.is_dir():
        raise NotADirectoryError(f"Pfad ist kein Verzeichnis: {scan_path}")

    raw_folders = sorted(
        [entry for entry in scan_path.iterdir() if entry.is_dir() and not entry.name.startswith(".")],
        key=lambda p: p.name,
    )
    subfolders = []
    for folder in raw_folders:
        if folder.name.upper() == "NXT STUDIOS":
            inner = sorted(
                [e for e in folder.iterdir() if e.is_dir() and not e.name.startswith(".")],
                key=lambda p: p.name,
            )
            subfolders.extend(inner)
        else:
            subfolders.append(folder)

    if not subfolders:
        return

    projects = []
    unassigned = []

    for folder in subfolders:
        validation = validate_folder(folder.name)
        stats = analyze_folder(folder)

        entry = {
            "name": folder.name,
            "absolute_path": str(folder),
            "size_bytes": stats["size_bytes"],
            "size_human": format_size(stats["size_bytes"]),
            "file_count": stats["file_count"],
            "last_modified": datetime.fromtimestamp(folder.stat().st_mtime).isoformat(),
        }

        if validation:
            entry["date"] = validation["date"]
            entry["project_name"] = validation["project_name"]
            entry["type"] = validation["type"]

            if validation["type"] == "PROJECT":
                children = []
                child_folders = sorted(
                    [e for e in folder.iterdir() if e.is_dir() and not e.name.startswith(".")],
                    key=lambda p: p.name,
                )
                for child_folder in child_folders:
                    child_validation = validate_folder(child_folder.name)
                    child_stats = analyze_folder(child_folder)
                    child_entry = {
                        "name": child_folder.name,
                        "absolute_path": str(child_folder),
                        "size_bytes": child_stats["size_bytes"],
                        "size_human": format_size(child_stats["size_bytes"]),
                        "file_count": child_stats["file_count"],
                        "last_modified": datetime.fromtimestamp(child_folder.stat().st_mtime).isoformat(),
                    }
                    if child_validation:
                        child_entry["date"] = child_validation["date"]
                        child_entry["project_name"] = child_validation["project_name"]
                        child_entry["type"] = child_validation["type"]
                    children.append(child_entry)
                entry["children"] = children

            projects.append(entry)
        else:
            unassigned.append(entry)

    report = generate_report(str(scan_path), projects, unassigned)
    save_report(report, out)


if __name__ == "__main__":
    main()
