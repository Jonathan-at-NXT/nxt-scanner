import json
from pathlib import Path
from storage_scanner.scan import run_scan


def _mkfile(p: Path, kb: int):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        f.write(b"\0" * (kb * 1024))


def test_report_has_folder_tree(tmp_path):
    vol = tmp_path / "vol"
    _mkfile(vol / "2025-01-01_PROJ_FOOTAGE" / "sub" / "a.bin", 50)
    _mkfile(vol / "LOOSEFOLDER" / "b.bin", 30)
    out = tmp_path / "r.json"
    run_scan(str(vol), str(out))
    report = json.loads(out.read_text())

    assert "folder_tree" in report
    rels = {n["rel_path"] for n in report["folder_tree"]}
    # Top-Level beider Ordner vorhanden (auch der lose)
    assert "2025-01-01_PROJ_FOOTAGE" in rels
    assert "LOOSEFOLDER" in rels
    # Unterebene erfasst
    assert "2025-01-01_PROJ_FOOTAGE/sub" in rels
    # bestehende Struktur unangetastet
    assert "scan_info" in report and "projects" in report and "unassigned" in report

    # Top-Level-Knoten liegen auf Volume-Ebene 1
    top = next(n for n in report["folder_tree"] if n["rel_path"] == "LOOSEFOLDER")
    assert top["depth"] == 1 and top["parent_rel_path"] is None
