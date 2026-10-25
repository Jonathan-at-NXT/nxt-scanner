"""Upload-Briefkasten abholen: fertige Uploads aus R2 aufs NAS schreiben, prüfen, dann in R2 löschen lassen.

Jede Datei landet unter <ziel>/<Linkname>/<relativer Pfad>. Geschrieben wird nach `.nxt.tmp`,
mit F_FULLFSYNC auf die Platte gebracht und Teil für Teil (SHA-256, gleiche Teilgröße wie beim
Hochladen) gegen die Prüfsummen aus dem Browser verglichen. Erst wenn alles stimmt, bekommt die
Datei ihren Namen und der Server löscht sie in R2. Eine vorhandene andere Datei wird nie
überschrieben (dann "__2" usw.).

  python -m storage_scanner.briefkasten pull --target "/Volumes/NXT STUDIOS/_EINGANG"            # einmal
  python -m storage_scanner.briefkasten pull --target "/Volumes/NXT STUDIOS/_EINGANG" --loop 120 # dauerhaft
"""
import argparse
import hashlib
import os
import re
import time
from pathlib import Path

from .transfer import _full_fsync, _fsync_dir, TMP_SUFFIX


def _cfg():
    from .notion_sync import load_config
    c = load_config()
    url, token = c.get("d1_ingest_url", ""), c.get("d1_token", "")
    if not url or not token:
        raise SystemExit("D1 nicht eingerichtet (d1_ingest_url/d1_token)")
    return url.rstrip("/"), {"Authorization": f"Bearer {token}"}


def _safe_name(s: str) -> str:
    s = re.sub(r"[\x00-\x1f/:]", "_", s).strip().strip(".")
    return s[:150] or "Upload"


def _free_path(p: Path) -> Path:
    if not p.exists():
        return p
    stem, suf, i = p.stem, p.suffix, 2
    while (p.parent / f"{stem}__{i}{suf}").exists():
        i += 1
    return p.parent / f"{stem}__{i}{suf}"


def pull_once(target: Path, log=print) -> dict:
    import httpx
    from .progress import Reporter
    base, hdr = _cfg()
    if not target.parent.exists():
        raise SystemExit(f"Ziel nicht erreichbar: {target.parent} (NAS eingehängt?)")
    files = httpx.get(f"{base}/briefkasten/pending", headers=hdr, timeout=60).json().get("files", [])
    stats = {"abgeholt": 0, "fehler": 0, "bytes": 0}
    if not files:
        return stats
    total = sum(f["size"] for f in files)
    rep = Reporter("kopie", "briefkasten", "Briefkasten → NAS", total_bytes=total)
    done_b = 0
    for f in files:
        rel = "/".join(_safe_name(x) for x in f["rel_path"].split("/"))
        dest = target / _safe_name(f["link_name"]) / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + TMP_SUFFIX)
        ps, shas, size = f["part_size"], [], 0
        try:
            with httpx.stream("GET", f"{base}/briefkasten/file", params={"id": f["id"]}, headers=hdr, timeout=None) as r:
                r.raise_for_status()
                h = hashlib.sha256(); in_part = 0
                with open(tmp, "wb") as out:
                    for chunk in r.iter_bytes(8 * 1024 * 1024):
                        mv = memoryview(chunk)
                        while len(mv):
                            take = min(len(mv), ps - in_part)
                            h.update(mv[:take]); out.write(mv[:take])
                            in_part += take; size += take; mv = mv[take:]
                            if in_part == ps:
                                shas.append(h.hexdigest()); h = hashlib.sha256(); in_part = 0
                        done_b += len(chunk)
                        rep.update(stats["abgeholt"], done_b)
                    if in_part or size == 0:
                        shas.append(h.hexdigest())
                    out.flush(); _full_fsync(out.fileno())
            want = [p["sha256"] for p in f["parts"]]
            if size != f["size"] or shas != want:
                raise ValueError(f"Prüfung fehlgeschlagen (Größe {size}/{f['size']}, Teile {sum(a == b for a, b in zip(shas, want))}/{len(want)})")
            final = _free_path(dest)
            os.rename(tmp, final); _fsync_dir(final.parent)
            r2 = httpx.post(f"{base}/briefkasten/done", headers=hdr, timeout=60,
                            json={"id": f["id"], "nas_path": str(final), "part_sha256": shas})
            r2.raise_for_status()
            stats["abgeholt"] += 1; stats["bytes"] += size
            log(f"abgeholt: {f['link_name']}/{f['rel_path']} ({size / 1e9:.2f} GB) -> {final}")
        except Exception as e:  # noqa: BLE001 - nächste Datei; diese bleibt in R2 und wird später erneut versucht
            stats["fehler"] += 1
            try:
                tmp.unlink()
            except OSError:
                pass
            log(f"FEHLER {f['link_name']}/{f['rel_path']}: {e}")
    rep.finish(stats["abgeholt"], done_b, ok=stats["fehler"] == 0)
    return stats


def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m storage_scanner.briefkasten")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("pull", help="fertige Uploads aufs Ziel holen")
    p.add_argument("--target", required=True, help='z. B. "/Volumes/NXT STUDIOS/_EINGANG"')
    p.add_argument("--loop", type=int, default=0, help="alle N Sekunden wiederholen (0 = einmal)")
    a = ap.parse_args(argv)
    target = Path(a.target)
    while True:
        try:
            s = pull_once(target)
            if s["abgeholt"] or s["fehler"]:
                print(time.strftime("%H:%M:%S"), s, flush=True)
        except SystemExit as e:
            print(time.strftime("%H:%M:%S"), e, flush=True)
            if not a.loop:
                raise
        except Exception as e:  # noqa: BLE001 - Netz weg o. ä.: beim nächsten Durchlauf erneut
            print(time.strftime("%H:%M:%S"), "Fehler:", e, flush=True)
        if not a.loop:
            return
        time.sleep(a.loop)


if __name__ == "__main__":
    main()
