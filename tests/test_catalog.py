import os

from storage_scanner import catalog


def _vol(uuid="U1", name="NXT 001"):
    return {"uuid": uuid, "name": name, "fs_type": "exfat", "capacity_bytes": 10, "free_bytes": 5,
            "device_id": None, "whole_disk": None, "physical_store": None, "media_name": None,
            "bus_protocol": None, "solid_state": None, "device_serial": None}


def _write(root, rel, data=b"x"):
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return p


def _present(conn, uuid="U1"):
    return {r[0] for r in conn.execute(
        "SELECT rel_path FROM file WHERE volume_uuid=? AND present=1", (uuid,))}


def test_classify():
    assert catalog.classify("C0001.MP4") == ("mp4", "video")
    assert catalog.classify("GX010001.LRV") == ("lrv", "proxy")
    assert catalog.classify("IMG_1.HEIC") == ("heic", "photo")
    assert catalog.classify("Projekt.drp") == ("drp", "project")
    assert catalog.classify("README") == ("", "other")


def test_inventory_skips_system_files(tmp_path):
    _write(tmp_path, "P/FX3/C0001.MP4")
    _write(tmp_path, "P/FX3/._C0001.MP4")
    _write(tmp_path, ".Spotlight-V100/store.db")
    _write(tmp_path, "P/half.mov.tmp")
    conn = catalog.connect(":memory:")
    r = catalog.inventory(str(tmp_path), conn, vol=_vol())
    assert r["files"] == 1 and r["added"] == 1 and r["complete"]
    assert _present(conn) == {"P/FX3/C0001.MP4"}
    kind = conn.execute("SELECT kind FROM file").fetchone()[0]
    assert kind == "video"


def test_inventory_incremental_change_and_gone(tmp_path):
    a = _write(tmp_path, "A.mp4", b"aaaa")
    _write(tmp_path, "B.mp4", b"bbbb")
    conn = catalog.connect(":memory:")
    catalog.inventory(str(tmp_path), conn, vol=_vol())
    catalog.hash_pass(str(tmp_path), conn, uuid="U1")
    assert conn.execute("SELECT COUNT(*) FROM file WHERE fingerprint IS NOT NULL").fetchone()[0] == 2

    # A ändert Inhalt+Größe, B verschwindet, C kommt dazu
    a.write_bytes(b"aaaaaaaa")
    os.utime(a, (1_700_000_000, 1_700_000_000))
    (tmp_path / "B.mp4").unlink()
    _write(tmp_path, "C.mp4")
    r = catalog.inventory(str(tmp_path), conn, vol=_vol())
    assert (r["added"], r["changed"], r["removed"]) == (1, 1, 1)
    assert _present(conn) == {"A.mp4", "C.mp4"}
    # geänderter Inhalt -> Fingerprint verworfen
    fp_a = conn.execute("SELECT fingerprint FROM file WHERE rel_path='A.mp4'").fetchone()[0]
    assert fp_a is None
    gone = conn.execute("SELECT present, gone_at FROM file WHERE rel_path='B.mp4'").fetchone()
    assert gone[0] == 0 and gone[1]

    # unveränderter Lauf -> nichts neu, nichts geändert
    r = catalog.inventory(str(tmp_path), conn, vol=_vol())
    assert (r["added"], r["changed"], r["removed"]) == (0, 0, 0)


def test_unreadable_dir_does_not_mark_files_gone(tmp_path):
    _write(tmp_path, "P/X/a.mp4")
    _write(tmp_path, "P/Y/b.mp4")
    conn = catalog.connect(":memory:")
    catalog.inventory(str(tmp_path), conn, vol=_vol())

    real = catalog.os.scandir
    blocked = str(tmp_path / "P" / "X")

    def flaky(path):
        if str(path) == blocked:
            raise PermissionError("nope")
        return real(path)

    catalog.os.scandir = flaky
    try:
        r = catalog.inventory(str(tmp_path), conn, vol=_vol())
    finally:
        catalog.os.scandir = real
    assert not r["complete"] and r["removed"] == 0
    assert _present(conn) == {"P/X/a.mp4", "P/Y/b.mp4"}


def test_hash_pass_budget_resumes(tmp_path):
    for i in range(3):
        _write(tmp_path, f"f{i}.mp4", bytes([i]) * (i + 1))
    conn = catalog.connect(":memory:")
    catalog.inventory(str(tmp_path), conn, vol=_vol())
    r = catalog.hash_pass(str(tmp_path), conn, uuid="U1", budget_s=-1)
    assert r["hashed"] == 0 and not r["complete"] and r["remaining"] == 3
    r = catalog.hash_pass(str(tmp_path), conn, uuid="U1")
    assert r["hashed"] == 3 and r["complete"]
    assert conn.execute("SELECT DISTINCT fp_algo FROM file").fetchall() == [(catalog.FP_ALGO,)]


def test_diff_finds_missing_moved_mismatch(tmp_path):
    ref, cpy = tmp_path / "ref", tmp_path / "cpy"
    _write(ref, "PROJ/FX3/C1.MP4", b"one")
    _write(ref, "PROJ/FX3/C2.MP4", b"two")
    _write(ref, "PROJ/FX3/C3.MP4", b"three")
    _write(ref, "PROJ/A7/D1.MP4", b"dddd")
    _write(cpy, "BACKUP/PROJ/FX3/C1.MP4", b"one")
    _write(cpy, "BACKUP/PROJ/FX3/C3.MP4", b"THREE")       # gleiche Größe, anderer Inhalt
    _write(cpy, "BACKUP/PROJ/LOSE/D1.MP4", b"dddd")       # verschoben
    _write(cpy, "BACKUP/PROJ/extra.txt", b"e")
    conn = catalog.connect(":memory:")
    catalog.inventory(str(ref), conn, vol=_vol("R", "NXT 030"))
    catalog.inventory(str(cpy), conn, vol=_vol("K", "NXT 034"))
    catalog.hash_pass(str(ref), conn, uuid="R")
    catalog.hash_pass(str(cpy), conn, uuid="K")

    d = catalog.diff(conn, "NXT 030", "PROJ", "NXT 034", "BACKUP/PROJ")
    s = d["summary"]
    assert (s["ok"], s["mismatch"], s["moved"], s["missing"], s["extra"]) == (1, 1, 1, 1, 1)
    assert d["missing"][0]["path"] == "FX3/C2.MP4" and s["missing_bytes"] == 3
    assert d["mismatch"][0]["path"] == "FX3/C3.MP4"
    assert d["moved"][0]["copy_path"] == "LOSE/D1.MP4"


def test_diff_prefix_does_not_match_sibling_with_like_wildcards(tmp_path):
    _write(tmp_path, "A_B/x.mp4")
    _write(tmp_path, "AxB/y.mp4")   # '_' darf nicht als LIKE-Wildcard greifen
    conn = catalog.connect(":memory:")
    catalog.inventory(str(tmp_path), conn, vol=_vol())
    d = catalog.diff(conn, "U1", "A_B", "U1", "A_B")
    assert d["summary"]["ref_files"] == 1


def test_catalog_safe_is_opt_in(tmp_path, monkeypatch):
    called = []
    monkeypatch.setattr(catalog, "connect", lambda *a, **k: called.append(1))
    catalog.catalog_safe(str(tmp_path), {})
    assert called == []


def test_parse_ioreg_serials_maps_whole_disks_and_drops_placeholders():
    text = """+-o USB3.1 Hub@02200000  <class IOUSBHostDevice>
  | |   |   "USB Product Name" = "USB to ATA/ATAPI Bridge"
  | |   |   "USB Serial Number" = "0123456789ABCDEF"
  | |   |                 |   "BSD Name" = "disk4"
  | |   |                         |   "BSD Name" = "disk5"
  | |   |   "USB Product Name" = "Elements 25A3"
  | |   |   "USB Serial Number" = "504E47315A394C50"
  | |   |                 |   "BSD Name" = "disk6"
  | |   |                 |   "BSD Name" = "disk6s1"
"""
    assert catalog.parse_ioreg_serials(text) == {"disk6": "504E47315A394C50"}


def test_missing_mount_is_rejected_without_db_entry(tmp_path):
    import pytest
    conn = catalog.connect(":memory:")
    with pytest.raises(FileNotFoundError):
        catalog.inventory(str(tmp_path / "gone"), conn, vol=_vol())
    assert conn.execute("SELECT COUNT(*) FROM volume").fetchone()[0] == 0


def test_hash_pass_prefix_only_hashes_project(tmp_path):
    _write(tmp_path, "P_1/a.mp4", b"a")
    _write(tmp_path, "PX1/b.mp4", b"b")   # '_' darf nicht als Wildcard greifen
    _write(tmp_path, "Q/c.mp4", b"c")
    conn = catalog.connect(":memory:")
    catalog.inventory(str(tmp_path), conn, vol=_vol())
    r = catalog.hash_pass(str(tmp_path), conn, uuid="U1", prefix="P_1")
    assert r["hashed"] == 1
    hashed = {x[0] for x in conn.execute("SELECT rel_path FROM file WHERE fingerprint IS NOT NULL")}
    assert hashed == {"P_1/a.mp4"}


def test_find_folders_across_volumes_and_depths(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    _write(a, "NXT STUDIOS/2025-04-01_LOCO CHIPS_FOOTAGE/FX3/C1.MP4", b"12")
    _write(a, "NXT STUDIOS/2025-04-01_LOCO CHIPS_WORKING/x.drp", b"1")
    _write(b, "2025-04-01_LOCO CHIPS_FOOTAGE/FX3/C1.MP4", b"12")
    conn = catalog.connect(":memory:")
    catalog.inventory(str(a), conn, vol=_vol("A", "NXT HUB 05"))
    catalog.inventory(str(b), conn, vol=_vol("B", "TOWER 7"))
    found = {(r["volume"], r["folder"]): r["files"] for r in catalog.find_folders(conn, "loco chips_footage")}
    assert found == {("NXT HUB 05", "NXT STUDIOS/2025-04-01_LOCO CHIPS_FOOTAGE"): 1,
                     ("TOWER 7", "2025-04-01_LOCO CHIPS_FOOTAGE"): 1}


def test_report_from_catalog_expands_containers(tmp_path):
    _write(tmp_path, "NXT STUDIOS/2025-01-01_A_FOOTAGE/FX3/c1.mp4", b"x" * 10)
    _write(tmp_path, "UNSORTED ARCHIVE/_TO CHECK/2024-03-12-ETP-VIDEOS/v.mp4", b"y" * 20)
    _write(tmp_path, "CONNOR/2025-02-06_KTM HQ DOKU_Footage/FX3 NILS/a.mp4", b"z" * 5)
    _write(tmp_path, "LOSE ORDNER/x.txt", b"1")
    _write(tmp_path, "#recycle/old.mp4", b"r")
    conn = catalog.connect(":memory:")
    catalog.inventory(str(tmp_path), conn, vol=_vol("nas:X", "NXT STUDIOS"))
    rep = catalog.report_from_catalog(conn, "nas:X", str(tmp_path))
    top = {n["rel_path"]: n for n in rep["folder_tree"] if n["depth"] == 1}
    assert set(top) == {"2025-01-01_A_FOOTAGE", "UNSORTED ARCHIVE/_TO CHECK/2024-03-12-ETP-VIDEOS",
                        "CONNOR/2025-02-06_KTM HQ DOKU_Footage", "LOSE ORDNER"}
    assert top["2025-01-01_A_FOOTAGE"]["abs_path"].endswith("NXT STUDIOS/2025-01-01_A_FOOTAGE")
    assert top["UNSORTED ARCHIVE/_TO CHECK/2024-03-12-ETP-VIDEOS"]["size_bytes"] == 20
    assert any(n["rel_path"] == "2025-01-01_A_FOOTAGE/FX3" and n["depth"] == 2 for n in rep["folder_tree"])
    assert rep["scan_info"]["complete"] and rep["scan_info"]["valid_folders"] == 3


def test_vanished_files_are_not_errors(tmp_path, monkeypatch):
    _write(tmp_path, "P/a.mp4"); _write(tmp_path, "P/b.mp4")
    real = os.DirEntry.stat if hasattr(os, "DirEntry") else None
    conn = catalog.connect(":memory:")
    orig = catalog.os.scandir

    class Gone:
        def __init__(self, e): self._e = e; self.name = e.name; self.path = e.path
        def is_dir(self, follow_symlinks=False): return self._e.is_dir(follow_symlinks=follow_symlinks)
        def is_file(self, follow_symlinks=False): return self._e.is_file(follow_symlinks=follow_symlinks)
        def stat(self, follow_symlinks=False):
            if self.name == "b.mp4":
                raise FileNotFoundError(2, "No such file or directory")
            return self._e.stat(follow_symlinks=follow_symlinks)

    class Ctx:
        def __init__(self, p): self._it = orig(p)
        def __enter__(self): return [Gone(e) for e in self._it.__enter__()]
        def __exit__(self, *a): return self._it.__exit__(*a)

    monkeypatch.setattr(catalog.os, "scandir", Ctx)
    r = catalog.inventory(str(tmp_path), conn, vol=_vol())
    assert r["complete"] and r["files"] == 1 and r["errors"] == 0


def test_system_folder_variants_are_skipped(tmp_path):
    _write(tmp_path, ".DocumentRevisions-V100-bad-1/x.db")
    _write(tmp_path, "P/a.mp4")
    conn = catalog.connect(":memory:")
    r = catalog.inventory(str(tmp_path), conn, vol=_vol())
    assert r["files"] == 1 and r["complete"]
