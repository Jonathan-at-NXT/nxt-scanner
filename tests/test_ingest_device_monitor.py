from storage_scanner.ingest import device_monitor as dm


def test_classify_card_with_camera_signature(tmp_path):
    # removable Volume, dessen Pfad eine Kamera-Signatur trägt
    (tmp_path / "PRIVATE" / "M4ROOT" / "CLIP").mkdir(parents=True)
    vol = {"name": "SONY_SD", "path": str(tmp_path),
           "is_removable": True, "total_bytes": 128 * 1000**3}
    assert dm.classify(vol) == "card"


def test_classify_target_large_internal(tmp_path):
    vol = {"name": "NXT 019", "path": str(tmp_path),
           "is_removable": False, "total_bytes": 8 * 1000**4}
    assert dm.classify(vol) == "target"


def test_classify_removable_no_signature_is_other(tmp_path):
    vol = {"name": "Backup", "path": str(tmp_path),
           "is_removable": True, "total_bytes": 4 * 1000**4}
    assert dm.classify(vol) == "other"


def test_list_volumes_uses_info(monkeypatch, tmp_path):
    (tmp_path / "SONY_SD").mkdir()
    monkeypatch.setattr(dm, "_volumes_root", lambda: tmp_path)
    monkeypatch.setattr(dm, "_volume_info", lambda p: {
        "name": p.name, "path": str(p), "is_removable": True,
        "total_bytes": 64 * 1000**3})
    vols = dm.list_volumes()
    assert any(v["name"] == "SONY_SD" and v["is_removable"] for v in vols)
