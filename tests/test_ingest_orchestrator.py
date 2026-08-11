from pathlib import Path

from storage_scanner import db
from storage_scanner.ingest import store, orchestrator


def _card(root: Path):
    (root / "PRIVATE" / "M4ROOT" / "CLIP").mkdir(parents=True)
    (root / "DCIM" / "100MSDCF").mkdir(parents=True)
    (root / "PRIVATE" / "M4ROOT" / "CLIP" / "FX3 JO0001.MP4").write_bytes(b"vid1" * 500)
    (root / "PRIVATE" / "M4ROOT" / "CLIP" / "FX3 JO0002.MP4").write_bytes(b"vid2" * 500)
    (root / "DCIM" / "100MSDCF" / "FX3 JO0001.JPG").write_bytes(b"pic1" * 300)


def test_run_ingest_dual_target_success(tmp_path):
    conn = db.connect(tmp_path / "t.db")
    card = tmp_path / "card"
    card.mkdir()
    _card(card)
    a = tmp_path / "A"
    b = tmp_path / "B"

    pid = store.create_project("Macherfest", "2026-08-11", conn=conn)
    project = store.project_payload(pid, conn)
    sid = store.create_session(pid, "SONY_SD", 0, 0, conn=conn)

    res = orchestrator.run_ingest(
        project, card, camera="FX3 JO", date="2026-08-11",
        target_a=a, target_b=b, conn=conn, session_id=sid, host="mac",
    )

    assert res["state"] == "done"
    assert res["verify_status"] == "verified"
    assert res["copies_done"] == 2

    # Video flach in FOOTAGE/DATE/CAMERA in BEIDEN Zielen
    for root in (a, b):
        fdir = root / "2026-08-11_Macherfest_FOOTAGE" / "2026-08-11" / "FX3 JO"
        pdir = root / "2026-08-11_Macherfest_PHOTOS" / "2026-08-11" / "FX3 JO"
        assert (fdir / "FX3 JO0001.MP4").exists()
        assert (fdir / "FX3 JO0002.MP4").exists()
        assert (pdir / "FX3 JO0001.JPG").exists()

    # Board grün
    assert store.board_rows(conn=conn)[0]["status"] == "2x_verified"


def test_run_ingest_target_error_leaves_source_untouched(tmp_path):
    conn = db.connect(tmp_path / "t.db")
    card = tmp_path / "card"
    card.mkdir()
    _card(card)
    a = tmp_path / "A"
    # Ziel B ist eine DATEI statt Verzeichnis -> Schreiben scheitert
    b = tmp_path / "B_is_file"
    b.write_bytes(b"blocker")

    pid = store.create_project("P", "2026-08-11", conn=conn)
    project = store.project_payload(pid, conn)
    sid = store.create_session(pid, "SD", 0, 0, conn=conn)

    res = orchestrator.run_ingest(
        project, card, camera="FX3 JO", date="2026-08-11",
        target_a=a, target_b=b, conn=conn, session_id=sid,
    )
    assert res["state"] == "error"
    # Quelle unangetastet
    assert (card / "PRIVATE" / "M4ROOT" / "CLIP" / "FX3 JO0001.MP4").read_bytes() == b"vid1" * 500
    # Board NICHT grün
    assert store.board_rows(conn=conn)[0]["status"] != "2x_verified"
