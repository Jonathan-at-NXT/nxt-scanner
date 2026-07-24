import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "cloud"))
import derive  # noqa: E402


def _node(uuid, rel, size=100):
    return {"disk_uuid": uuid, "rel_path": rel, "size_bytes": size, "file_count": 1, "mtime": "x"}


def test_classify_validated_container_unassigned():
    v = derive.classify(_node("D1", "2025-01-01_ACME_FOOTAGE"))
    assert v["status"] == "validated" and v["type"] == "FOOTAGE" and v["date"] == "2025-01-01"
    assert v["project_name"] == "ACME"

    c = derive.classify(_node("D1", "OLDER"))
    assert c["status"] == "container"

    u = derive.classify(_node("D1", "Downloads"))
    assert u["status"] == "unassigned" and u["date"] is None


def test_assign_clusters_fuzzy_merges_typos():
    nodes = [
        _node("D1", "2025-05-26_ZELLERFELD SEAN_FOOTAGE"),
        _node("D2", "2025-05-26_ZELLER FELD SEAN_FOOTAGE"),  # Tippvariante, gleiches Datum
        _node("D3", "2025-01-01_OTHER PROJECT_PHOTOS"),
        _node("D4", "LOOSE"),  # unassigned -> kein cluster
    ]
    projects = [derive.classify(n) for n in nodes]
    projects = derive.assign_clusters(projects)

    # die beiden ZELLERFELD-Varianten teilen sich einen cluster_id
    z = [p["cluster_id"] for p in projects if "ZELLER" in p["norm_name"]]
    assert len(z) == 2 and z[0] == z[1] and z[0] is not None
    # das andere Projekt hat einen eigenen cluster
    other = next(p for p in projects if "OTHER PROJECT" in p["norm_name"])
    assert other["cluster_id"] != z[0]
    # unassigned bleibt ohne cluster
    loose = next(p for p in projects if p["name"] == "LOOSE")
    assert loose["cluster_id"] is None


def test_scans_log_counts():
    projects = [
        {"disk_uuid": "D1", "status": "validated"},
        {"disk_uuid": "D1", "status": "unassigned"},
    ]
    nodes = [{"disk_uuid": "D1"}, {"disk_uuid": "D1"}, {"disk_uuid": "D1"}]
    disks = [{"uuid": "D1", "last_scan": "2026-07-24", "used_bytes": 500}]
    rows = derive.scans_log_rows(disks, projects, nodes)
    assert rows[0] == {"disk_uuid": "D1", "scan_date": "2026-07-24", "used_bytes": 500,
                       "valid_count": 1, "unassigned_count": 1, "node_count": 3}
