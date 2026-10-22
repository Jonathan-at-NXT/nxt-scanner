import httpx

from storage_scanner import outbox


def _report(tmp_path, name="NXT_001_report.json"):
    p = tmp_path / name
    p.write_text("{}")
    return str(p)


def _recorder(fail=None):
    sent = []

    def make(ch):
        def send(report, user):
            if fail and fail(ch, report):
                raise fail(ch, report)
            sent.append((ch, report))
        return send

    return sent, {ch: make(ch) for ch in outbox.CHANNELS}


def test_offline_keeps_everything_then_uploads_once_online(tmp_path):
    q = tmp_path / "q.json"
    r1, r2 = _report(tmp_path, "A_report.json"), _report(tmp_path, "B_report.json")
    outbox.enqueue(r1, path=q)
    outbox.enqueue(r2, path=q)

    _, offline = _recorder(fail=lambda ch, r: httpx.ConnectError("no route"))
    res = outbox.flush(path=q, senders=offline)
    assert res["offline"] and res["sent"] == 0 and res["left"] == 2

    sent, online = _recorder()
    res = outbox.flush(path=q, senders=online)
    assert not res["offline"] and res["left"] == 0
    assert sorted(sent) == sorted([(c, str(tmp_path / f)) for f in ("A_report.json", "B_report.json")
                                   for c in outbox.CHANNELS])


def test_new_scan_replaces_queued_entry(tmp_path):
    q = tmp_path / "q.json"
    r = _report(tmp_path)
    outbox.enqueue(r, path=q)
    outbox.enqueue(r, path=q)   # zweiter Scan derselben Platte
    entries = outbox.pending(q)
    assert len(entries) == 1 and next(iter(entries.values()))["generation"] == 2

    sent, online = _recorder()
    outbox.flush(path=q, senders=online)
    assert len(sent) == len(outbox.CHANNELS)  # nur einmal hochgeladen


def test_rescan_during_upload_is_not_lost(tmp_path):
    q = tmp_path / "q.json"
    r = _report(tmp_path)
    outbox.enqueue(r, path=q)

    def d1(report, user):
        outbox.enqueue(r, path=q)  # während des Uploads kommt ein neuer Scan rein

    res = outbox.flush(path=q, senders={"d1": d1})
    # der neue Scan muss noch komplett ausstehen
    entry = outbox.pending(q)[str(tmp_path / "NXT_001_report.json")]
    assert entry["generation"] == 2 and entry["pending"] == list(outbox.CHANNELS)
    assert res["left"] == 1


def test_partial_channel_success_only_retries_missing(tmp_path):
    q = tmp_path / "q.json"
    r = _report(tmp_path)
    outbox.enqueue(r, channels=["a", "b"], path=q)
    calls = []

    def ok(report, user):
        calls.append("a")

    def fail(report, user):
        raise httpx.ReadTimeout("t")

    outbox.flush(path=q, senders={"a": ok, "b": fail})
    assert next(iter(outbox.pending(q).values()))["pending"] == ["b"]


def test_non_network_error_does_not_block_others(tmp_path):
    q = tmp_path / "q.json"
    bad, good = _report(tmp_path, "BAD_report.json"), _report(tmp_path, "GOOD_report.json")
    outbox.enqueue(bad, path=q)
    outbox.enqueue(good, path=q)
    sent, s = _recorder(fail=lambda ch, rep: ValueError("kaputt") if "BAD" in rep else None)
    res = outbox.flush(path=q, senders=s)
    assert not res["offline"] and res["left"] == 1
    assert {rep for _, rep in sent} == {str(tmp_path / "GOOD_report.json")}
    assert "kaputt" in outbox.pending(q)[str(tmp_path / "BAD_report.json")]["last_error"]


def test_missing_report_is_dropped(tmp_path):
    q = tmp_path / "q.json"
    r = _report(tmp_path)
    outbox.enqueue(r, path=q)
    (tmp_path / "NXT_001_report.json").unlink()
    assert outbox.flush(path=q, senders={})["left"] == 0


def test_channels_follow_config():
    assert outbox.channels_for({}) == []
    assert outbox.channels_for({"notion_token": "x"}) == ["notion"]   # Rückfall ohne D1
    assert outbox.channels_for({"d1_ingest_url": "u", "d1_token": "t"}) == ["d1"]
    assert outbox.channels_for({"notion_token": "x", "d1_ingest_url": "u", "d1_token": "t"}) == ["d1"]


def test_enqueue_without_channels_is_noop(tmp_path):
    q = tmp_path / "q.json"
    outbox.enqueue(_report(tmp_path), channels=[], path=q)
    assert outbox.pending(q) == {}
