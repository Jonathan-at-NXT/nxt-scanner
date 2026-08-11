from storage_scanner import db
from storage_scanner.ingest import store


def _conn(tmp_path):
    return db.connect(tmp_path / "t.db")


def test_create_and_list_project(tmp_path):
    conn = _conn(tmp_path)
    pid = store.create_project(
        "Macherfest", "2026-08-11", client="Macher GmbH",
        ff_notion_url="https://notion.so/ff-123", created_by="JO", conn=conn,
    )
    rows = store.list_projects(conn=conn)
    assert len(rows) == 1
    assert rows[0]["id"] == pid
    assert rows[0]["client"] == "Macher GmbH"
    assert rows[0]["ff_notion_url"] == "https://notion.so/ff-123"


def test_session_lifecycle_and_board(tmp_path):
    conn = _conn(tmp_path)
    pid = store.create_project("Macherfest", "2026-08-11", conn=conn)
    sid = store.create_session(pid, "CANON_SD", 1000, 5, host="mac-jo", conn=conn)

    # frisch erkannt -> ungesichert
    rows = store.board_rows(conn=conn)
    assert rows[0]["status"] == "unsecured"
    assert rows[0]["project_name"] == "Macherfest"

    store.update_session(sid, copies_done=1, conn=conn)
    assert store.board_rows(conn=conn)[0]["status"] == "1x"

    store.update_session(sid, copies_done=2, verify_status="verified",
                         state="done", conn=conn)
    assert store.board_rows(conn=conn)[0]["status"] == "2x_verified"


def test_board_error_state(tmp_path):
    conn = _conn(tmp_path)
    pid = store.create_project("P", "2026-08-11", conn=conn)
    sid = store.create_session(pid, "CARD", 10, 1, conn=conn)
    store.update_session(sid, state="error", conn=conn)
    assert store.board_rows(conn=conn)[0]["status"] == "error"
