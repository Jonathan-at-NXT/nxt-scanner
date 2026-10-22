"""Platten-Check: Zustand eines Datenträgers, ausschließlich lesend.

Drei Stufen:
1. SMART (smartctl, Sekunden): Verschleiß, Reserve, Medien-/Lesefehler, Temperatur.
   Über USB nur, wenn das Gehäuse die Daten durchreicht; sonst "unbekannt".
2. Dateisystem (diskutil verifyVolume, Minuten): nur lesend, hängt die Platte aber
   kurz um -> nur auf dem Sicherungs-Mac und nie während einer Kopie.
3. Lesetest (optional, lang): liest Dateien vollständig (am Cache vorbei) und
   sammelt Dateien mit Lesefehlern; nutzt den Datei-Katalog, resumable mit Zeitbudget.

Ergebnis: verdict ok | warnung | defekt | unbekannt, plus Details. Wird lokal im
Katalog gespeichert und (über die Upload-Queue) nach D1 gemeldet.
"""

import argparse
import json
import logging
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path

from .paths import DATA_DIR

_log = logging.getLogger("health")

HEALTH_DIR = DATA_DIR / "health"
# Bridge-Typen, die smartctl über USB probiert (NVMe-Gehäuse, SATA-Adapter)
_DEVICE_TYPES = [None, "sat", "sntasmedia", "sntjmicron", "sntrealtek", "nvme"]
WEAR_WARN = 80        # % Lebensdauer verbraucht -> Warnung
WEAR_BAD = 100
SPARE_MARGIN = 10     # Reserve weniger als 10 Punkte über Schwelle -> Warnung


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _smartctl() -> str | None:
    for p in ("/opt/homebrew/bin/smartctl", "/usr/local/bin/smartctl", shutil.which("smartctl")):
        if p and Path(p).exists():
            return p
    return None


def whole_disk(mount: str) -> str | None:
    """Physische Platte hinter einem Volume (APFS: über den Physical Store)."""
    from .catalog import _diskutil
    info = _diskutil(mount)
    stores = info.get("APFSPhysicalStores") or []
    if stores:
        phys = _diskutil(stores[0].get("APFSPhysicalStore", ""))
        if phys.get("ParentWholeDisk"):
            return phys["ParentWholeDisk"]
    return info.get("ParentWholeDisk")


# ── 1. SMART ──────────────────────────────────────────────────────

def read_smart(disk: str, runner=subprocess.run) -> dict:
    """Rohes smartctl-JSON (erster Gerätetyp, der echte Werte liefert) oder {}."""
    exe = _smartctl()
    if not exe or not disk:
        return {}
    for dtype in _DEVICE_TYPES:
        cmd = [exe, "-a", "-j", disk] + (["-d", dtype] if dtype else [])
        try:
            r = runner(cmd, capture_output=True, text=True, timeout=60)
            data = json.loads(r.stdout or "{}")
        except (subprocess.TimeoutExpired, OSError, ValueError):
            continue
        if data.get("smart_status") is not None or data.get("nvme_smart_health_information_log") or data.get("ata_smart_attributes"):
            data["_device_type"] = dtype or "auto"
            return data
    return {}


def _ata_raw(data: dict, *ids: int) -> int | None:
    for a in (data.get("ata_smart_attributes") or {}).get("table", []):
        if a.get("id") in ids:
            return (a.get("raw") or {}).get("value")
    return None


def evaluate_smart(data: dict) -> dict:
    """Kennzahlen + Urteil aus smartctl-JSON."""
    if not data:
        return {"status": "unbekannt", "reason": "SMART nicht verfügbar (Gehäuse reicht keine Daten durch)"}
    out = {"model": data.get("model_name"), "serial": data.get("serial_number"),
           "device_type": data.get("_device_type"),
           "passed": (data.get("smart_status") or {}).get("passed"),
           "power_on_hours": (data.get("power_on_time") or {}).get("hours"),
           "temperature": (data.get("temperature") or {}).get("current")}
    problems, warnings = [], []
    nv = data.get("nvme_smart_health_information_log")
    if nv:
        out.update(wear_pct=nv.get("percentage_used"), spare_pct=nv.get("available_spare"),
                   spare_threshold=nv.get("available_spare_threshold"), media_errors=nv.get("media_errors"),
                   critical_warning=nv.get("critical_warning"), unsafe_shutdowns=nv.get("unsafe_shutdowns"))
        out["power_on_hours"] = out["power_on_hours"] or nv.get("power_on_hours")
        out["temperature"] = out["temperature"] or nv.get("temperature")
        if nv.get("critical_warning"):
            problems.append(f"kritische Warnung der SSD (Code {nv['critical_warning']})")
        if (nv.get("media_errors") or 0) > 0:
            problems.append(f"{nv['media_errors']} Medienfehler")
        if nv.get("available_spare") is not None and nv.get("available_spare_threshold") is not None:
            if nv["available_spare"] <= nv["available_spare_threshold"]:
                problems.append(f"Reserve erschöpft ({nv['available_spare']} %)")
            elif nv["available_spare"] - nv["available_spare_threshold"] < SPARE_MARGIN and nv["available_spare_threshold"] < 90:
                warnings.append(f"Reserve knapp ({nv['available_spare']} %)")
    else:
        realloc = _ata_raw(data, 5)
        pending = _ata_raw(data, 197)
        uncorr = _ata_raw(data, 198, 187)
        wear_left = _ata_raw(data, 177, 231, 233)   # je nach Hersteller: verbleibende Lebensdauer
        out.update(reallocated=realloc, pending=pending, uncorrectable=uncorr)
        if pending:
            problems.append(f"{pending} instabile Sektoren")
        if uncorr:
            problems.append(f"{uncorr} nicht korrigierbare Fehler")
        if realloc:
            (problems if realloc > 50 else warnings).append(f"{realloc} ersetzte Sektoren")
        if wear_left is not None and 0 < wear_left <= 100:
            out["wear_pct"] = 100 - wear_left
    if out.get("passed") is False:
        problems.append("SMART-Selbsttest: FEHLGESCHLAGEN")
    wear = out.get("wear_pct")
    if wear is not None:
        if wear >= WEAR_BAD:
            problems.append(f"Lebensdauer verbraucht ({wear} %)")
        elif wear >= WEAR_WARN:
            warnings.append(f"Lebensdauer zu {wear} % verbraucht")
    out["status"] = "defekt" if problems else "warnung" if warnings else "ok"
    out["reason"] = "; ".join(problems + warnings) or "unauffällig"
    return out


# ── 2. Dateisystem ────────────────────────────────────────────────

def verify_fs(mount: str, runner=subprocess.run) -> dict:
    """diskutil verifyVolume (lesend). Ergebnis ok | defekt | unbekannt."""
    try:
        r = runner(["diskutil", "verifyVolume", mount], capture_output=True, text=True, timeout=6 * 3600)
    except (subprocess.TimeoutExpired, OSError) as e:
        return {"status": "unbekannt", "reason": f"Prüfung nicht möglich: {e}"}
    text = (r.stdout or "") + (r.stderr or "")
    tail = "\n".join(text.strip().splitlines()[-6:])
    if r.returncode == 0 and ("appears to be OK" in text or "exit code is 0" in text):
        return {"status": "ok", "reason": "Dateisystem in Ordnung", "output": tail}
    if any(k in text for k in ("Unable to unmount", "could not be unmounted", "not supported", "Live file system verification")):
        return {"status": "unbekannt", "reason": "Dateisystem ließ sich nicht prüfen (in Benutzung oder nicht unterstützt)", "output": tail}
    return {"status": "defekt", "reason": "Dateisystem meldet Fehler: Daten sofort sichern, nicht weiter beschreiben", "output": tail}


# ── 3. Lesetest ───────────────────────────────────────────────────

def read_test(mount: str, conn, uuid: str, budget_s: float | None = None, restart: bool = False) -> dict:
    """Liest Dateien vollständig. Merkt sich je Datei Erfolg/Fehler im Katalog (resumable).
    Dateien mit Lesefehler werden nicht erneut gelesen, außer bei restart=True (neuer Durchgang)."""
    from . import transfer
    file_hash = transfer.file_hash
    for col in ("read_ok_at TEXT", "read_error TEXT"):
        try:
            conn.execute(f"ALTER TABLE file ADD COLUMN {col}")
        except Exception:  # noqa: BLE001 - Spalte existiert schon
            pass
    if restart:
        with conn:
            conn.execute("UPDATE file SET read_ok_at=NULL, read_error=NULL WHERE volume_uuid=?", (uuid,))
    todo = conn.execute(
        """SELECT rel_path, size FROM file WHERE volume_uuid=? AND present=1 AND read_ok_at IS NULL
           AND read_error IS NULL ORDER BY rel_path""", (uuid,)).fetchall()  # defekte Stellen nicht wiederholt lesen
    t0 = time.monotonic()
    done = errors = 0
    nbytes = 0
    for rel, size in todo:
        if budget_s is not None and time.monotonic() - t0 > budget_s:
            break
        try:
            file_hash(Path(mount) / rel)
            with conn:
                conn.execute("UPDATE file SET read_ok_at=?, read_error=NULL WHERE volume_uuid=? AND rel_path=?",
                             (_now(), uuid, rel))
            nbytes += size or 0
        except OSError as e:
            errors += 1
            with conn:
                conn.execute("UPDATE file SET read_error=? WHERE volume_uuid=? AND rel_path=?", (str(e)[:300], uuid, rel))
        done += 1
    left = conn.execute("SELECT COUNT(*) FROM file WHERE volume_uuid=? AND present=1 AND read_ok_at IS NULL AND read_error IS NULL",
                        (uuid,)).fetchone()[0]
    total_err = conn.execute("SELECT COUNT(*) FROM file WHERE volume_uuid=? AND present=1 AND read_error IS NOT NULL",
                             (uuid,)).fetchone()[0]
    secs = max(0.001, time.monotonic() - t0)
    return {"status": "defekt" if total_err else ("ok" if left == 0 else "laeuft"),
            "read_files": done, "read_bytes": nbytes, "errors": total_err, "left": left,
            "mb_s": round(nbytes / secs / 1e6, 1),
            "reason": f"{total_err} Dateien nicht lesbar" if total_err else ("alle Dateien lesbar" if left == 0 else f"{left} Dateien noch offen")}


# ── Gesamt ────────────────────────────────────────────────────────

_RANK = {"defekt": 3, "warnung": 2, "ok": 1, "laeuft": 0, "unbekannt": 0, None: 0}


def verdict(*parts: dict | None) -> str:
    st = [p.get("status") for p in parts if p]
    if not st or all(s in ("unbekannt", "laeuft") for s in st):
        return "unbekannt"
    return max(st, key=lambda s: _RANK.get(s, 0))


def check(mount: str, fs: bool = False, read_minutes: float = 0, conn=None) -> dict:
    """Prüft einen gemounteten Datenträger. fs/read sind optional (Sicherungs-Mac)."""
    from .catalog import volume_info
    if not Path(mount).is_dir():
        raise FileNotFoundError(f"Nicht gemountet: {mount}")
    vol = volume_info(mount, serials={})
    disk = whole_disk(mount)
    smart = evaluate_smart(read_smart(f"/dev/{disk}" if disk else ""))
    fs_res = verify_fs(mount) if fs else None
    read_res = None
    if read_minutes and conn is not None:
        read_res = read_test(mount, conn, vol["uuid"], budget_s=read_minutes * 60)
    res = {"disk_uuid": vol["uuid"], "name": vol["name"], "checked_at": _now(), "device": disk,
           "smart": smart, "fs": fs_res, "read": read_res, "verdict": verdict(smart, fs_res, read_res)}
    HEALTH_DIR.mkdir(parents=True, exist_ok=True)
    path = HEALTH_DIR / f"{vol['uuid'].replace('/', '_').replace(':', '_')}.json"
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(res, ensure_ascii=False, indent=2))
    tmp.replace(path)
    res["_path"] = str(path)
    return res


def health_safe(mount: str, config: dict) -> None:
    """Best-effort nach jedem Scan. `health_check: true` aktiviert SMART;
    `health_fs`/`health_read_minutes` zusätzlich (nur Sicherungs-Mac)."""
    if not config.get("health_check"):
        return
    try:
        conn = None
        if config.get("health_read_minutes"):
            from .catalog import connect
            conn = connect()
        try:
            res = check(mount, fs=bool(config.get("health_fs")), read_minutes=float(config.get("health_read_minutes") or 0), conn=conn)
        finally:
            if conn:
                conn.close()
        _log.info("Platten-Check %s: %s (%s)", mount, res["verdict"], res["smart"].get("reason"))
        from . import outbox
        if config.get("d1_ingest_url") and config.get("d1_token"):
            outbox.enqueue(res["_path"], channels=["health"])
            outbox.flush_safe()
    except Exception as e:  # noqa: BLE001 - darf den Scan nie stören
        _log.error("Platten-Check fehlgeschlagen fuer %s: %s", mount, e)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m storage_scanner.health", description="Platten-Check (nur lesend)")
    ap.add_argument("mount")
    ap.add_argument("--fs", action="store_true", help="Dateisystem prüfen (hängt kurz um, nicht während Kopien)")
    ap.add_argument("--read-minutes", type=float, default=0, help="Lesetest mit Zeitbudget (braucht Katalog-Inventur)")
    args = ap.parse_args(argv)
    conn = None
    if args.read_minutes:
        from .catalog import connect
        conn = connect()
    res = check(args.mount, fs=args.fs, read_minutes=args.read_minutes, conn=conn)
    sm = res["smart"]
    print(f"{res['name']} ({res['device']}): {res['verdict'].upper()}")
    print(f"  SMART: {sm['status']} – {sm['reason']}" + (f" | Modell {sm.get('model')}, Verschleiß {sm.get('wear_pct')} %, {sm.get('power_on_hours')} h" if sm.get("model") else ""))
    if res["fs"]:
        print(f"  Dateisystem: {res['fs']['status']} – {res['fs']['reason']}")
    if res["read"]:
        r = res["read"]
        print(f"  Lesetest: {r['status']} – {r['reason']} ({r['read_files']} Dateien, {r['mb_s']} MB/s)")


if __name__ == "__main__":
    main()
