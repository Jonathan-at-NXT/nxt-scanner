from storage_scanner import paths


def test_alte_url_wird_umgeschrieben(monkeypatch):
    monkeypatch.setattr(paths, "OLD_D1_INGEST_URLS", ("https://alt.workers.dev",))
    monkeypatch.setattr(paths, "DEFAULT_D1_INGEST_URL", "https://neu.workers.dev")
    c = {"d1_ingest_url": "https://alt.workers.dev/", "d1_token": "x"}
    assert paths.migrate_ingest_url(c)
    assert c == {"d1_ingest_url": "https://neu.workers.dev", "d1_token": "x"}


def test_eigene_oder_leere_url_bleibt(monkeypatch):
    monkeypatch.setattr(paths, "OLD_D1_INGEST_URLS", ("https://alt.workers.dev",))
    monkeypatch.setattr(paths, "DEFAULT_D1_INGEST_URL", "https://neu.workers.dev")
    for c in ({"d1_ingest_url": "https://eigen.example"}, {}, {"d1_ingest_url": ""}):
        before = dict(c)
        assert not paths.migrate_ingest_url(c)
        assert c == before
