"""Volle Prüfsummen (blake2b-256, wie storage_scanner.transfer.file_hash) für eine Liste von NAS-Dateien.

Läuft als einmaliger Container auf dem NAS (lokales Lesen, viel schneller als übers Netz).
Liste:    /data/_SYSTEM/pruefung/liste.txt     (eine Zeile je Datei, Pfad relativ zur Freigabe "NXT STUDIOS")
Ergebnis: /data/_SYSTEM/pruefung/hashes.jsonl  ({"path","size","mtime","hash"} je Zeile; fortsetzbar)
Fortschritt: /data/_SYSTEM/pruefung/status.json und Balken im Dashboard (über den Briefkasten-Endpunkt).
"""
import hashlib
import json
import os
import time
import urllib.request

ROOT = "/data"
DIR = os.path.join(ROOT, "_SYSTEM", "pruefung")
URL = os.environ.get("INGEST_URL", "https://nxt-scanner-ingest.nxtstudios.workers.dev").rstrip("/")
TOKEN = os.environ.get("PICKUP_TOKEN", "")


def fhash(p):
    h = hashlib.blake2b(digest_size=32)
    with open(p, "rb") as f:
        for buf in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(buf)
    return h.hexdigest()


def report(done_b, total_b, n, state="running"):
    try:
        req = urllib.request.Request(URL + "/briefkasten/progress", method="POST",
                                     data=json.dumps({"done_bytes": done_b, "total_bytes": total_b, "done_files": n, "state": state,
                                                      "name": "NAS-Prüfsummen (Freigabe)"}).encode(),
                                     headers={"Authorization": f"Bearer {TOKEN}", "content-type": "application/json",
                                              "user-agent": "nxt-nas-hash"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception:  # noqa: BLE001 - nur Anzeige
        pass


def main():
    paths = [l.rstrip("\n") for l in open(os.path.join(DIR, "liste.txt"), encoding="utf-8") if l.strip()]
    out = os.path.join(DIR, "hashes.jsonl")
    done = {}
    if os.path.exists(out):
        for l in open(out, encoding="utf-8"):
            try:
                r = json.loads(l); done[r["path"]] = r
            except Exception:  # noqa: BLE001
                pass
    sizes = {}
    for p in paths:
        try:
            sizes[p] = os.stat(os.path.join(ROOT, p)).st_size
        except OSError:
            sizes[p] = 0
    total = sum(sizes.values()); done_b = sum(sizes.get(p, 0) for p in done); n = len(done); last = 0
    with open(out, "a", encoding="utf-8") as f:
        for p in paths:
            if p in done:
                continue
            full = os.path.join(ROOT, p)
            try:
                st = os.stat(full)
                h = fhash(full) if st.st_size > 0 else "LEER"
                rec = {"path": p, "size": st.st_size, "mtime": st.st_mtime, "hash": h}
            except OSError as e:
                rec = {"path": p, "size": -1, "mtime": 0, "hash": None, "error": str(e)}
            f.write(json.dumps(rec, ensure_ascii=False) + "\n"); f.flush()
            n += 1; done_b += max(0, rec["size"])
            if time.time() - last > 15:
                report(done_b, total, n); last = time.time()
                json.dump({"done": n, "total": len(paths), "done_bytes": done_b, "total_bytes": total, "at": time.strftime("%Y-%m-%d %H:%M:%S")},
                          open(os.path.join(DIR, "status.json"), "w"))
    report(done_b, total, n, "done")
    json.dump({"done": n, "total": len(paths), "done_bytes": done_b, "total_bytes": total, "fertig": True,
               "at": time.strftime("%Y-%m-%d %H:%M:%S")}, open(os.path.join(DIR, "status.json"), "w"))
    print("fertig", n, "Dateien")


if __name__ == "__main__":
    main()
