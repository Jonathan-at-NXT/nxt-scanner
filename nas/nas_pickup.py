"""NXT Upload-Briefkasten – Abholer für das NAS (läuft im DSM Container Manager, nur Python-Standardbibliothek).

Holt fertige Uploads aus dem Cloudflare-Zwischenspeicher nach TARGET/<Linkname>/<Pfad>, prüft jeden
Teil per SHA-256 gegen die Prüfsumme aus dem Browser des Absenders, gibt der Datei erst dann ihren
Namen und lässt sie danach im Zwischenspeicher löschen. Vorhandene andere Dateien werden nie
überschrieben ("__2" usw.).

Umgebung:
  INGEST_URL    https://nxt-scanner-ingest.nxtstudios.workers.dev
  PICKUP_TOKEN  eingeschränkter Schlüssel (nur Abholen), in DSM als Umgebungsvariable hinterlegt
  TARGET        z. B. /data/_EINGANG  (/data = gemountete Freigabe "NXT STUDIOS")
  INTERVAL      Sekunden zwischen zwei Durchläufen (Default 120)
"""
import hashlib
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

URL = os.environ.get("INGEST_URL", "https://nxt-scanner-ingest.nxtstudios.workers.dev").rstrip("/")
TOKEN = os.environ.get("PICKUP_TOKEN", "")
TARGET = Path(os.environ.get("TARGET", "/data/_EINGANG"))
INTERVAL = int(os.environ.get("INTERVAL", "120"))
TMP = ".nxt.tmp"


def log(*a):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)


def req(method, path, body=None, params=None, stream=False, timeout=60):
    url = URL + path + ("?" + urllib.parse.urlencode(params) if params else "")
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Authorization": f"Bearer {TOKEN}", "content-type": "application/json",
                                        "user-agent": "nxt-nas-pickup"})
    resp = urllib.request.urlopen(r, timeout=timeout)
    return resp if stream else json.loads(resp.read() or b"{}")


def safe(s):
    s = re.sub(r"[\x00-\x1f/:]", "_", s).strip().strip(".")
    return s[:150] or "Upload"


def free_path(p: Path) -> Path:
    if not p.exists():
        return p
    i = 2
    while (p.parent / f"{p.stem}__{i}{p.suffix}").exists():
        i += 1
    return p.parent / f"{p.stem}__{i}{p.suffix}"


def progress(done, total, files_done, state="running"):
    try:   # nur Anzeige im Dashboard; Fehler egal
        req("POST", "/briefkasten/progress", {"done_bytes": done, "total_bytes": total, "done_files": files_done, "state": state}, timeout=10)
    except Exception:  # noqa: BLE001
        pass


def pull_once():
    files = req("GET", "/briefkasten/pending").get("files", [])
    if not files:
        return 0, 0
    total, done, ok, bad = sum(f["size"] for f in files), 0, 0, 0
    last = 0.0
    for f in files:
        dest = TARGET / safe(f["link_name"]) / "/".join(safe(x) for x in f["rel_path"].split("/"))
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + TMP)
        ps, shas, size, h, in_part = f["part_size"], [], 0, hashlib.sha256(), 0
        try:
            with req("GET", "/briefkasten/file", params={"id": f["id"]}, stream=True, timeout=300) as r, open(tmp, "wb") as out:
                while True:
                    chunk = r.read(8 * 1024 * 1024)
                    if not chunk:
                        break
                    mv = memoryview(chunk)
                    while len(mv):
                        take = min(len(mv), ps - in_part)
                        h.update(mv[:take]); out.write(mv[:take])
                        in_part += take; size += take; mv = mv[take:]
                        if in_part == ps:
                            shas.append(h.hexdigest()); h = hashlib.sha256(); in_part = 0
                    done += len(chunk)
                    if time.time() - last > 10:
                        progress(done, total, ok); last = time.time()
                if in_part or size == 0:
                    shas.append(h.hexdigest())
                out.flush(); os.fsync(out.fileno())
            want = [p["sha256"] for p in f["parts"]]
            if size != f["size"] or shas != want:
                raise ValueError(f"Prüfung fehlgeschlagen (Größe {size}/{f['size']})")
            final = free_path(dest)
            os.rename(tmp, final)
            dfd = os.open(final.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            req("POST", "/briefkasten/done", {"id": f["id"], "nas_path": str(final), "part_sha256": shas})
            ok += 1
            log(f"abgeholt: {f['link_name']}/{f['rel_path']} ({size / 1e9:.2f} GB)")
        except Exception as e:  # noqa: BLE001 - bleibt im Zwischenspeicher, nächster Durchlauf versucht es erneut
            bad += 1
            try:
                tmp.unlink()
            except OSError:
                pass
            log(f"FEHLER {f['link_name']}/{f['rel_path']}: {e}")
    progress(done, total, ok, "done" if not bad else "failed")
    return ok, bad


def main():
    if not TOKEN:
        sys.exit("PICKUP_TOKEN fehlt")
    log(f"Abholer gestartet → {TARGET} (alle {INTERVAL} s)")
    while True:
        try:
            if not TARGET.parent.exists():
                log(f"Ziel nicht erreichbar: {TARGET.parent}")
            else:
                ok, bad = pull_once()
                if ok or bad:
                    log(f"Durchlauf: {ok} abgeholt, {bad} Fehler")
        except Exception as e:  # noqa: BLE001 - Netz weg o. ä.
            log("Fehler:", e)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
