from pathlib import Path

from storage_scanner.ingest import organize


def _touch(p: Path, data: bytes):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def test_target_dir_footage_and_photos(tmp_path):
    d = organize.target_dir(tmp_path, "Macherfest", "2026-08-11", "video", "FX3 JO")
    assert d == tmp_path / "2026-08-11_Macherfest_FOOTAGE" / "2026-08-11" / "FX3 JO"
    p = organize.target_dir(tmp_path, "Macherfest", "2026-08-11", "photo", "FX3 JO")
    assert p == tmp_path / "2026-08-11_Macherfest_PHOTOS" / "2026-08-11" / "FX3 JO"


def test_plan_flatten_collision_different_content(tmp_path):
    # Zwei DCIM-Rollover-Ordner, gleicher Dateiname, UNTERSCHIEDLICHER Inhalt
    a = tmp_path / "100MSDCF" / "C0001.MP4"
    b = tmp_path / "101MSDCF" / "C0001.MP4"
    _touch(a, b"aaaa" * 100)
    _touch(b, b"bbbb" * 100)
    dest = tmp_path / "dest"

    plan = organize.plan_flatten([a, b], dest)
    dsts = [dst.name for _src, dst in plan]
    assert dsts[0] == "C0001.MP4"
    assert dsts[1] == "C0001__2.MP4"  # kollisions-sicher suffixt, kein Overwrite
    assert all(dst.parent == dest for _src, dst in plan)


def test_plan_flatten_collision_same_content_dedup(tmp_path):
    # Gleicher Name + gleicher Inhalt -> gleiches Ziel (Dedup, nicht doppelt)
    a = tmp_path / "x" / "C0001.MP4"
    b = tmp_path / "y" / "C0001.MP4"
    same = b"identical" * 50
    _touch(a, same)
    _touch(b, same)
    dest = tmp_path / "dest"

    plan = organize.plan_flatten([a, b], dest)
    dsts = {dst.name for _src, dst in plan}
    assert dsts == {"C0001.MP4"}
