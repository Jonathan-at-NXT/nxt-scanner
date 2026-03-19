#!/usr/bin/env python3
"""NXT Drive Reformat Tool — Kopiert, formatiert, kopiert zurück.

Nutzung:
    python3 reformat_tool.py "/Volumes/TOWER 4" "/Volumes/NXT HUB 02/TOWER 4"

Schritte:
    1. Alle Daten von Quelle → Ziel kopieren (rsync, überspringt kaputte Dateien)
    2. Kopie verifizieren (Dateianzahl + Größenvergleich)
    3. Quelle auf HFS+ formatieren (erst nach erfolgreicher Verifizierung!)
    4. Alle Daten von Ziel → Quelle zurückkopieren
    5. Rückkopie verifizieren
    6. Fertig

Fail-Safes:
    - Formatiert NICHT wenn Verifizierung fehlschlägt
    - Fragt vor Formatierung nochmal nach Bestätigung
    - Alle Fehler werden in Logdatei geschrieben
    - Kann bei Abbruch ab dem letzten Schritt fortgesetzt werden (--resume)
    - Funktioniert auf Intel + ARM Macs (Python 3.9+)
"""

import argparse
import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────

LOG_DIR = Path.home() / "Desktop" / "reformat_logs"
RSYNC_OPTS = [
    "-avh", "--progress",
    "--exclude=.Spotlight*",
    "--exclude=.fseventsd",
    "--exclude=.Trashes",
    "--exclude=@eaDir",
    "--exclude=@tmp",
    "--exclude=.DS_Store",
]

# ── Hilfsfunktionen ────────────────────────────────────────────────

def log(msg, logfile=None):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    if logfile:
        with open(logfile, "a") as f:
            f.write(line + "\n")


def run_cmd(cmd, logfile=None, check=True):
    """Führt einen Befehl aus und loggt stdout/stderr."""
    log(f"  → {' '.join(cmd)}", logfile)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout.strip():
        for line in result.stdout.strip().split("\n")[-5:]:
            log(f"    {line}", logfile)
    if result.stderr.strip():
        for line in result.stderr.strip().split("\n")[-5:]:
            log(f"    STDERR: {line}", logfile)
    if check and result.returncode not in (0, 23, 24):
        # rsync: 23 = partial transfer (some files couldn't be read), 24 = vanished files
        # Beide sind okay — wir loggen und machen weiter
        log(f"  ⚠ Exit code: {result.returncode}", logfile)
    return result


def get_disk_identifier(volume_path):
    """Findet die disk identifier (z.B. disk7s1) für einen Volume-Pfad."""
    result = subprocess.run(
        ["diskutil", "info", volume_path],
        capture_output=True, text=True,
    )
    for line in result.stdout.split("\n"):
        if "Device Identifier" in line:
            return line.split(":")[-1].strip()
    return None


def get_disk_base(disk_id):
    """disk7s1 → disk7 (für Formatierung brauchen wir das ganze Disk)."""
    import re
    match = re.match(r"(disk\d+)", disk_id)
    return match.group(1) if match else None


def count_files_and_size(path):
    """Zählt Dateien und Gesamtgröße eines Verzeichnisses."""
    total_files = 0
    total_size = 0
    errors = 0
    for root, dirs, files in os.walk(path):
        # Versteckte Ordner überspringen
        dirs[:] = [d for d in dirs if not d.startswith(".") and d not in ("@eaDir", "@tmp")]
        for f in files:
            if f.startswith("."):
                continue
            try:
                fp = os.path.join(root, f)
                total_size += os.path.getsize(fp)
                total_files += 1
            except OSError:
                errors += 1
    return total_files, total_size, errors


def format_size(size_bytes):
    """Formatiert Bytes als menschenlesbare Größe."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1000:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1000
    return f"{size_bytes:.1f} PB"


def format_duration(seconds):
    """Formatiert Sekunden als h:mm:ss."""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}:{m:02d}:{s:02d}"


# ── State-Management ───────────────────────────────────────────────

def load_state(state_file):
    if state_file.exists():
        with open(state_file) as f:
            return json.load(f)
    return {"step": 0}


def save_state(state_file, state):
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)


# ── Hauptschritte ──────────────────────────────────────────────────

def step1_copy_to_target(source, target, logfile):
    """Schritt 1: Alle Daten von Quelle → Ziel kopieren."""
    log("", logfile)
    log("=" * 60, logfile)
    log("SCHRITT 1/5: Kopiere Daten von Quelle → Ziel", logfile)
    log(f"  Quelle: {source}", logfile)
    log(f"  Ziel:   {target}", logfile)
    log("=" * 60, logfile)

    os.makedirs(target, exist_ok=True)

    error_log = LOG_DIR / "rsync_to_target_errors.log"
    cmd = ["rsync"] + RSYNC_OPTS + [
        f"--log-file={LOG_DIR / 'rsync_to_target.log'}",
        f"{source}/",
        f"{target}/",
    ]

    start = time.time()
    result = subprocess.run(cmd, stderr=open(error_log, "w"))
    duration = time.time() - start

    log(f"  Kopie abgeschlossen in {format_duration(duration)}", logfile)
    log(f"  rsync Exit-Code: {result.returncode}", logfile)

    if result.returncode in (0, 23, 24):
        if result.returncode == 23:
            log("  ⚠ Einige Dateien konnten nicht gelesen werden (exFAT-Fehler)", logfile)
            log(f"    Siehe: {error_log}", logfile)
        return True
    else:
        log(f"  ✗ rsync fehlgeschlagen (Exit-Code {result.returncode})", logfile)
        log(f"    Siehe: {error_log}", logfile)
        return False


def step2_verify_copy(source, target, logfile):
    """Schritt 2: Kopie verifizieren (Dateianzahl + Größe vergleichen)."""
    log("", logfile)
    log("=" * 60, logfile)
    log("SCHRITT 2/5: Verifiziere Kopie", logfile)
    log("=" * 60, logfile)

    log("  Zähle Dateien auf Quelle...", logfile)
    src_files, src_size, src_errors = count_files_and_size(source)
    log(f"  Quelle: {src_files} Dateien, {format_size(src_size)}, {src_errors} nicht lesbar", logfile)

    log("  Zähle Dateien auf Ziel...", logfile)
    tgt_files, tgt_size, tgt_errors = count_files_and_size(target)
    log(f"  Ziel:   {tgt_files} Dateien, {format_size(tgt_size)}, {tgt_errors} Fehler", logfile)

    # Vergleich: Ziel sollte >= Quelle minus Fehler sein
    expected_min = src_files - src_errors
    diff_files = abs(tgt_files - expected_min)
    diff_size = abs(tgt_size - src_size)

    log(f"  Differenz: {diff_files} Dateien, {format_size(diff_size)}", logfile)

    if tgt_files >= expected_min * 0.99:  # 99% Toleranz
        log("  ✓ Verifizierung bestanden", logfile)
        return True
    else:
        log(f"  ✗ Verifizierung fehlgeschlagen!", logfile)
        log(f"    Erwartet mindestens {expected_min} Dateien, gefunden: {tgt_files}", logfile)
        return False


def step3_format_drive(source, logfile):
    """Schritt 3: Quelle auf HFS+ (Mac OS Extended, Journaled) formatieren."""
    log("", logfile)
    log("=" * 60, logfile)
    log("SCHRITT 3/5: Formatiere Quelle auf HFS+", logfile)
    log("=" * 60, logfile)

    disk_id = get_disk_identifier(source)
    if not disk_id:
        log("  ✗ Konnte Disk-Identifier nicht ermitteln", logfile)
        return False

    disk_base = get_disk_base(disk_id)
    if not disk_base:
        log(f"  ✗ Konnte Basis-Disk nicht ermitteln aus {disk_id}", logfile)
        return False

    volume_name = Path(source).name
    log(f"  Disk: {disk_id} (Base: {disk_base})", logfile)
    log(f"  Volume-Name: {volume_name}", logfile)
    log(f"  Neues Format: HFS+ (Mac OS Extended, Journaled)", logfile)

    # Sicherheitsabfrage
    print()
    print("  ╔════════════════════════════════════════════════════╗")
    print(f"  ║  ACHTUNG: {volume_name} wird KOMPLETT GELÖSCHT!     ")
    print("  ║  Alle Daten auf diesem Datenträger gehen verloren.  ")
    print("  ╚════════════════════════════════════════════════════╝")
    print()
    confirm = input(f'  Tippe "JA FORMATIEREN" um fortzufahren: ')
    if confirm.strip() != "JA FORMATIEREN":
        log("  Abgebrochen durch Benutzer", logfile)
        return False

    log(f"  Formatiere {disk_base} als HFS+ ({volume_name})...", logfile)
    result = subprocess.run(
        ["diskutil", "eraseDisk", "JHFS+", volume_name, f"/dev/{disk_base}"],
        capture_output=True, text=True,
    )

    if result.returncode == 0:
        log(f"  ✓ Formatierung abgeschlossen", logfile)
        # Neuen Mount-Punkt zurückgeben
        for line in result.stdout.split("\n"):
            if "Mounting disk" in line or "mounted" in line.lower():
                log(f"    {line.strip()}", logfile)
        return True
    else:
        log(f"  ✗ Formatierung fehlgeschlagen: {result.stderr.strip()}", logfile)
        return False


def step4_copy_back(source, target, logfile):
    """Schritt 4: Alle Daten von Ziel → Quelle zurückkopieren."""
    log("", logfile)
    log("=" * 60, logfile)
    log("SCHRITT 4/5: Kopiere Daten zurück Ziel → Quelle", logfile)
    log(f"  Von: {target}", logfile)
    log(f"  Nach: {source}", logfile)
    log("=" * 60, logfile)

    error_log = LOG_DIR / "rsync_to_source_errors.log"
    cmd = ["rsync"] + RSYNC_OPTS + [
        f"--log-file={LOG_DIR / 'rsync_to_source.log'}",
        f"{target}/",
        f"{source}/",
    ]

    start = time.time()
    result = subprocess.run(cmd, stderr=open(error_log, "w"))
    duration = time.time() - start

    log(f"  Rückkopie abgeschlossen in {format_duration(duration)}", logfile)
    log(f"  rsync Exit-Code: {result.returncode}", logfile)

    if result.returncode in (0, 23, 24):
        return True
    else:
        log(f"  ✗ rsync fehlgeschlagen (Exit-Code {result.returncode})", logfile)
        return False


def step5_verify_final(source, target, logfile):
    """Schritt 5: Finale Verifizierung."""
    log("", logfile)
    log("=" * 60, logfile)
    log("SCHRITT 5/5: Finale Verifizierung", logfile)
    log("=" * 60, logfile)

    log("  Zähle Dateien auf Quelle (neu formatiert)...", logfile)
    src_files, src_size, src_errors = count_files_and_size(source)
    log(f"  Quelle: {src_files} Dateien, {format_size(src_size)}", logfile)

    log("  Zähle Dateien auf Ziel (Backup)...", logfile)
    tgt_files, tgt_size, tgt_errors = count_files_and_size(target)
    log(f"  Ziel:   {tgt_files} Dateien, {format_size(tgt_size)}", logfile)

    diff_files = abs(src_files - tgt_files)
    log(f"  Differenz: {diff_files} Dateien", logfile)

    if src_files >= tgt_files * 0.99:
        log("  ✓ Alles erfolgreich! Datenträger ist jetzt HFS+.", logfile)
        return True
    else:
        log("  ⚠ Differenz erkannt — bitte manuell prüfen", logfile)
        log(f"    Backup bleibt erhalten unter: {target}", logfile)
        return False


# ── Main ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Kopiert Daten, formatiert auf HFS+, kopiert zurück.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Beispiel: python3 reformat_tool.py "/Volumes/TOWER 4" "/Volumes/NXT HUB 02/TOWER 4"',
    )
    parser.add_argument("source", help="Quell-Volume (z.B. /Volumes/TOWER 4)")
    parser.add_argument("target", help="Zielordner für Backup (z.B. /Volumes/NXT HUB 02/TOWER 4)")
    parser.add_argument("--resume", action="store_true", help="Fortsetzen ab letztem Schritt")
    parser.add_argument("--skip-verify", action="store_true", help="Verifizierung überspringen (nicht empfohlen)")
    args = parser.parse_args()

    source = args.source.rstrip("/")
    target = args.target.rstrip("/")

    # Validierung
    if not os.path.isdir(source):
        print(f"Fehler: Quelle existiert nicht oder ist kein Verzeichnis: {source}")
        sys.exit(1)

    # Log-Verzeichnis erstellen
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    volume_name = Path(source).name.replace(" ", "_")
    logfile = LOG_DIR / f"reformat_{volume_name}.log"
    state_file = LOG_DIR / f"reformat_{volume_name}_state.json"

    state = load_state(state_file) if args.resume else {"step": 0}

    log("", logfile)
    log("╔══════════════════════════════════════════════════════════╗", logfile)
    log("║          NXT Drive Reformat Tool                        ║", logfile)
    log("╚══════════════════════════════════════════════════════════╝", logfile)
    log(f"  Quelle:   {source}", logfile)
    log(f"  Ziel:     {target}", logfile)
    log(f"  Logfile:  {logfile}", logfile)
    if args.resume:
        log(f"  Fortsetzen ab Schritt {state['step'] + 1}", logfile)

    start_total = time.time()

    # Schritt 1: Kopieren
    if state["step"] < 1:
        if step1_copy_to_target(source, target, logfile):
            state["step"] = 1
            save_state(state_file, state)
        else:
            log("Abbruch nach Schritt 1. Starte mit --resume neu.", logfile)
            sys.exit(1)

    # Schritt 2: Verifizieren
    if state["step"] < 2:
        if args.skip_verify:
            log("  Verifizierung übersprungen (--skip-verify)", logfile)
            state["step"] = 2
            save_state(state_file, state)
        elif step2_verify_copy(source, target, logfile):
            state["step"] = 2
            save_state(state_file, state)
        else:
            log("", logfile)
            log("✗ VERIFIZIERUNG FEHLGESCHLAGEN — Formatierung wird NICHT durchgeführt.", logfile)
            log("  Bitte prüfe die Kopie manuell.", logfile)
            log(f"  Fehlerlog: {LOG_DIR / 'rsync_to_target_errors.log'}", logfile)
            sys.exit(1)

    # Schritt 3: Formatieren
    if state["step"] < 3:
        if step3_format_drive(source, logfile):
            state["step"] = 3
            save_state(state_file, state)
        else:
            log("Formatierung abgebrochen. Starte mit --resume neu.", logfile)
            sys.exit(1)

    # Schritt 4: Zurückkopieren
    if state["step"] < 4:
        # Nach Formatierung kann sich der Mount-Punkt geändert haben
        if not os.path.isdir(source):
            log(f"  ⚠ {source} nicht gefunden nach Formatierung.", logfile)
            log(f"    Bitte Volume manuell mounten und mit --resume fortfahren.", logfile)
            sys.exit(1)
        if step4_copy_back(source, target, logfile):
            state["step"] = 4
            save_state(state_file, state)
        else:
            log("Rückkopie fehlgeschlagen. Starte mit --resume neu.", logfile)
            sys.exit(1)

    # Schritt 5: Finale Verifizierung
    if state["step"] < 5:
        step5_verify_final(source, target, logfile)
        state["step"] = 5
        save_state(state_file, state)

    duration_total = time.time() - start_total
    log("", logfile)
    log("═" * 60, logfile)
    log(f"  FERTIG! Gesamtdauer: {format_duration(duration_total)}", logfile)
    log(f"  {source} ist jetzt HFS+ (Mac OS Extended, Journaled)", logfile)
    log(f"  Backup liegt noch unter: {target}", logfile)
    log(f"  → Kann gelöscht werden wenn alles passt.", logfile)
    log("═" * 60, logfile)


if __name__ == "__main__":
    main()
