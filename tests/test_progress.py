from storage_scanner import progress


def test_reporter_throttles_and_finishes():
    sent = []
    r = progress.Reporter("inventur", "U", "NAS", total_files=100, interval=3600, sender=sent.append)
    r.update(10, 1000)          # erster Aufruf geht raus
    r.update(20, 2000)          # gedrosselt
    r.update(30, 3000, force=True)
    r.finish(100, 10000)
    assert [s["done_files"] for s in sent] == [10, 30, 100]
    assert sent[-1]["state"] == "done" and sent[0]["total_files"] == 100
    assert sent[0]["job_id"].endswith("|U|inventur")
