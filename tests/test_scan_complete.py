import json

import pytest

from storage_scanner import scan, analyzer


def _vol(tmp_path):
    v = tmp_path / "vol"
    (v / "2025-01-01_P_FOOTAGE" / "FX3").mkdir(parents=True)
    (v / "2025-01-01_P_FOOTAGE" / "FX3" / "C1.MP4").write_bytes(b"x" * 2048)
    (v / "LOSE").mkdir()
    (v / "LOSE" / "a.txt").write_bytes(b"a")
    return v


def test_complete_scan_writes_report(tmp_path):
    out = tmp_path / "r.json"
    scan.run_scan(str(_vol(tmp_path)), str(out))
    rep = json.loads(out.read_text())
    assert rep["scan_info"]["complete"] is True
    assert rep["scan_info"]["incomplete_folders"] == []
    assert rep["scan_info"]["disk_usage"]["total_bytes"] > 0


def test_failed_tree_keeps_last_good_report(tmp_path, monkeypatch):
    vol, out = _vol(tmp_path), tmp_path / "r.json"
    out.write_text('{"last": "good"}')
    real = scan.walk_tree
    monkeypatch.setattr(scan, "walk_tree",
                        lambda folder, sub_depth=2: [] if folder.name == "LOSE" else real(folder, sub_depth))
    with pytest.raises(scan.ScanIncomplete):
        scan.run_scan(str(vol), str(out))
    assert json.loads(out.read_text()) == {"last": "good"}   # nicht überschrieben
    partial = json.loads((tmp_path / "r.incomplete.json").read_text())
    assert partial["scan_info"]["incomplete_folders"] == ["LOSE"]


def test_zero_files_with_data_is_incomplete(tmp_path, monkeypatch):
    vol, out = _vol(tmp_path), tmp_path / "r.json"
    monkeypatch.setattr(analyzer, "_find_count", lambda p: 0)            # find "zählt" 0
    monkeypatch.setattr(analyzer, "_du_size", lambda p: 5 * 1024 * 1024)  # du sieht Daten
    monkeypatch.setattr(scan, "analyze_folder", analyzer.analyze_folder)
    with pytest.raises(scan.ScanIncomplete):
        scan.run_scan(str(vol), str(out))
    assert not out.exists()


def test_fallback_read_errors_mark_incomplete(tmp_path, monkeypatch):
    monkeypatch.setattr(analyzer, "_du_size", lambda p: None)
    monkeypatch.setattr(analyzer, "_find_count", lambda p: None)

    def broken(path, size_ref, count_ref, errors=None):
        errors.append(f"{path}: [Errno 5] Input/output error")

    monkeypatch.setattr(analyzer, "_scan_recursive", broken)
    assert analyzer.analyze_folder(tmp_path)["complete"] is False


def test_find_count_ok_on_normal_folder(tmp_path):
    (tmp_path / "a").write_bytes(b"1")
    (tmp_path / "b").write_bytes(b"2")
    assert analyzer._find_count(str(tmp_path)) == 2


def test_auto_scan_patterns_include_enclosures(monkeypatch):
    from storage_scanner import auto_scan
    monkeypatch.setattr("storage_scanner.notion_sync.load_config", lambda: {})
    for n in ["NXT 012", "TOWER 7", "NXT HUB 05", "NH 01", "nb 02"]:
        assert auto_scan.is_auto_scan_volume(n), n
    assert not auto_scan.is_auto_scan_volume("LaCie")


def test_auto_scan_all_only_physical(monkeypatch):
    from storage_scanner import auto_scan
    monkeypatch.setattr("storage_scanner.notion_sync.load_config", lambda: {"auto_scan_all": True})
    monkeypatch.setattr(auto_scan, "_is_physical_local", lambda n: n == "LaCie")
    assert auto_scan.is_auto_scan_volume("LaCie")
    assert not auto_scan.is_auto_scan_volume("Blackmagic DaVinci Resolve Studio")


def test_unreadable_root_raises_instead_of_silent_success(tmp_path, monkeypatch):
    vol = _vol(tmp_path)
    real = scan.os.scandir

    def deny(p):
        if str(p) == str(vol):
            raise PermissionError(13, "Permission denied")
        return real(p)

    monkeypatch.setattr(scan.os, "scandir", deny)
    with pytest.raises(scan.ScanIncomplete, match="Lesezugriff"):
        scan.run_scan(str(vol), str(tmp_path / "r.json"))


def test_empty_disk_writes_report(tmp_path):
    vol = tmp_path / "leer"; vol.mkdir()
    out = tmp_path / "r.json"
    scan.run_scan(str(vol), str(out))
    assert json.loads(out.read_text())["scan_info"]["complete"] is True
