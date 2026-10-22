import errno

import pytest

from storage_scanner import transfer


def _src(tmp_path, rel="card/C0001.MP4", data=b"video" * 300000):
    p = tmp_path / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return p


def test_copy_to_two_targets_single_read(tmp_path):
    src = _src(tmp_path)
    a, b = tmp_path / "A/x/C0001.MP4", tmp_path / "B/x/C0001.MP4"
    res = transfer.safe_copy(src, [a, b], chunk=65536)
    assert a.read_bytes() == src.read_bytes() == b.read_bytes()
    assert res["hash"] == transfer.file_hash(src)
    assert sorted(res["copied"]) == sorted([a, b])
    assert not list(tmp_path.rglob("*" + transfer.TMP_SUFFIX))


def test_existing_identical_is_skipped_not_rewritten(tmp_path):
    src = _src(tmp_path)
    dst = tmp_path / "A/C0001.MP4"
    transfer.safe_copy(src, [dst])
    before = dst.stat().st_mtime_ns
    res = transfer.safe_copy(src, [dst])
    assert res["skipped"] == [dst] and res["copied"] == []
    assert dst.stat().st_mtime_ns == before


def test_never_overwrites_different_file(tmp_path):
    src = _src(tmp_path, data=b"new" * 1000)
    dst = tmp_path / "A/C0001.MP4"
    dst.parent.mkdir(parents=True)
    dst.write_bytes(b"OLD" * 1000)  # gleiche Größe, anderer Inhalt
    with pytest.raises(transfer.Collision):
        transfer.safe_copy(src, [dst])
    assert dst.read_bytes() == b"OLD" * 1000


def test_stale_tmp_from_crash_is_discarded(tmp_path):
    src = _src(tmp_path)
    dst = tmp_path / "A/C0001.MP4"
    dst.parent.mkdir(parents=True)
    (dst.parent / ("C0001.MP4" + transfer.TMP_SUFFIX)).write_bytes(b"half")
    transfer.safe_copy(src, [dst])
    assert dst.read_bytes() == src.read_bytes()
    assert not (dst.parent / ("C0001.MP4" + transfer.TMP_SUFFIX)).exists()


def test_verification_failure_leaves_no_target(tmp_path, monkeypatch):
    src = _src(tmp_path)
    dst = tmp_path / "A/C0001.MP4"
    real = transfer.file_hash

    def corrupt(path, chunk=transfer._CHUNK):
        return "bad" if str(path).endswith(transfer.TMP_SUFFIX) else real(path, chunk)

    monkeypatch.setattr(transfer, "file_hash", corrupt)
    with pytest.raises(transfer.TransferError):
        transfer.safe_copy(src, [dst])
    assert not dst.exists()
    assert not list(dst.parent.glob("*" + transfer.TMP_SUFFIX))


def test_job_resumes_after_interruption(tmp_path, monkeypatch):
    for i in range(4):
        _src(tmp_path, f"SRC/P/f{i}.mp4", bytes([i]) * 5000)
    conn = transfer.connect(":memory:")
    monkeypatch.setattr(transfer, "keep_awake", _noop_cm)
    job = transfer.create_job(conn, tmp_path / "SRC", [tmp_path / "A", tmp_path / "B"])

    real = transfer.safe_copy
    calls = {"n": 0}

    def crash_on_third(src, dsts, **kw):
        calls["n"] += 1
        if calls["n"] == 3:
            raise KeyboardInterrupt  # simuliert Abbruch mitten im Job
        return real(src, dsts, **kw)

    monkeypatch.setattr(transfer, "safe_copy", crash_on_third)
    with pytest.raises(KeyboardInterrupt):
        transfer.run_job(conn, job, log=lambda *_: None)
    monkeypatch.setattr(transfer, "safe_copy", real)

    res = transfer.run_job(conn, job, log=lambda *_: None)
    assert res["state"] == "done" and res["open"] == 0
    for i in range(4):
        for t in "AB":
            assert (tmp_path / t / "P" / f"f{i}.mp4").read_bytes() == bytes([i]) * 5000


def test_job_waits_for_disk_to_return(tmp_path, monkeypatch):
    _src(tmp_path, "SRC/a.mp4", b"x" * 1000)
    conn = transfer.connect(":memory:")
    monkeypatch.setattr(transfer, "keep_awake", _noop_cm)
    monkeypatch.setattr(transfer.time, "sleep", lambda s: None)
    job = transfer.create_job(conn, tmp_path / "SRC", [tmp_path / "A"])

    real = transfer.safe_copy
    state = {"first": True}

    def flaky(src, dsts, **kw):
        if state["first"]:
            state["first"] = False
            raise OSError(errno.EIO, "I/O error")
        return real(src, dsts, **kw)

    monkeypatch.setattr(transfer, "safe_copy", flaky)
    res = transfer.run_job(conn, job, log=lambda *_: None)
    assert res["state"] == "done"
    assert (tmp_path / "A/a.mp4").exists()


def test_job_collision_is_reported_not_overwritten(tmp_path, monkeypatch):
    _src(tmp_path, "SRC/a.mp4", b"new!" * 100)
    (tmp_path / "A").mkdir()
    (tmp_path / "A/a.mp4").write_bytes(b"old!" * 100)
    conn = transfer.connect(":memory:")
    monkeypatch.setattr(transfer, "keep_awake", _noop_cm)
    job = transfer.create_job(conn, tmp_path / "SRC", [tmp_path / "A"])
    res = transfer.run_job(conn, job, log=lambda *_: None)
    assert res["collision"] == 1 and res["state"] == "incomplete"
    assert (tmp_path / "A/a.mp4").read_bytes() == b"old!" * 100


def test_check_space_refuses(tmp_path):
    with pytest.raises(transfer.TransferError):
        transfer.check_space(10 ** 18, [tmp_path])


from contextlib import contextmanager  # noqa: E402


@contextmanager
def _noop_cm():
    yield
