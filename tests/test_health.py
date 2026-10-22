import subprocess

from storage_scanner import health, catalog


def nvme(**kw):
    log = {"critical_warning": 0, "available_spare": 100, "available_spare_threshold": 10,
           "percentage_used": 13, "media_errors": 0, "power_on_hours": 3951, "temperature": 34}
    log.update(kw)
    return {"model_name": "Samsung T7", "smart_status": {"passed": True}, "nvme_smart_health_information_log": log,
            "_device_type": "sntasmedia"}


def test_healthy_ssd_is_ok():
    r = health.evaluate_smart(nvme())
    assert r["status"] == "ok" and r["wear_pct"] == 13


def test_worn_ssd_warns_and_media_errors_are_defect():
    assert health.evaluate_smart(nvme(percentage_used=85))["status"] == "warnung"
    r = health.evaluate_smart(nvme(media_errors=3))
    assert r["status"] == "defekt" and "Medienfehler" in r["reason"]
    assert health.evaluate_smart(nvme(critical_warning=4))["status"] == "defekt"
    assert health.evaluate_smart(nvme(available_spare=5))["status"] == "defekt"


def test_failed_selftest_and_ata_pending_are_defect():
    d = nvme(); d["smart_status"] = {"passed": False}
    assert health.evaluate_smart(d)["status"] == "defekt"
    ata = {"smart_status": {"passed": True}, "ata_smart_attributes": {"table": [
        {"id": 5, "raw": {"value": 0}}, {"id": 197, "raw": {"value": 8}}]}}
    assert health.evaluate_smart(ata)["status"] == "defekt"
    ata["ata_smart_attributes"]["table"] = [{"id": 5, "raw": {"value": 3}}, {"id": 197, "raw": {"value": 0}}]
    assert health.evaluate_smart(ata)["status"] == "warnung"


def test_no_smart_is_unknown_not_ok():
    assert health.evaluate_smart({})["status"] == "unbekannt"
    assert health.verdict({"status": "unbekannt"}) == "unbekannt"
    assert health.verdict({"status": "ok"}, {"status": "defekt"}) == "defekt"


def test_read_smart_tries_bridge_types(monkeypatch):
    calls = []

    def runner(cmd, **kw):
        calls.append(cmd)
        ok = "sntjmicron" in cmd
        out = '{"smart_status": {"passed": true}}' if ok else '{"smartctl": {"exit_status": 1}}'
        return subprocess.CompletedProcess(cmd, 0, out, "")

    monkeypatch.setattr(health, "_smartctl", lambda: "/x/smartctl")
    d = health.read_smart("/dev/disk4", runner=runner)
    assert d["_device_type"] == "sntjmicron"
    assert len(calls) == health._DEVICE_TYPES.index("sntjmicron") + 1


def test_verify_fs_parsing():
    run = lambda out, rc=0: (lambda cmd, **kw: subprocess.CompletedProcess(cmd, rc, out, ""))
    assert health.verify_fs("/V", runner=run("The volume appears to be OK\nFile system check exit code is 0"))["status"] == "ok"
    assert health.verify_fs("/V", runner=run("error: Invalid b-tree node\nFile system check exit code is 8", 1))["status"] == "defekt"
    assert health.verify_fs("/V", runner=run("Unable to unmount volume for verification", 1))["status"] == "unbekannt"


def test_read_test_records_unreadable_files(tmp_path, monkeypatch):
    (tmp_path / "a.mp4").write_bytes(b"a" * 100)
    (tmp_path / "b.mp4").write_bytes(b"b" * 100)
    conn = catalog.connect(":memory:")
    catalog.inventory(str(tmp_path), conn, vol={"uuid": "U", "name": "X", "fs_type": None, "capacity_bytes": 1,
        "free_bytes": 1, "device_id": None, "whole_disk": None, "physical_store": None, "media_name": None,
        "bus_protocol": None, "solid_state": None, "device_serial": None})
    from storage_scanner import transfer
    real = transfer.file_hash

    def flaky(p, chunk=transfer._CHUNK):
        if str(p).endswith("b.mp4"):
            raise OSError(5, "Input/output error")
        return real(p, chunk)

    monkeypatch.setattr(transfer, "file_hash", flaky)
    r = health.read_test(str(tmp_path), conn, "U")
    assert r["status"] == "defekt" and r["errors"] == 1 and r["left"] == 0
    # zweiter Lauf: gelesene Datei wird nicht erneut gelesen
    r2 = health.read_test(str(tmp_path), conn, "U")
    assert r2["read_files"] == 0
