"""Lokaler Store für vom Menschen angelegte Projekte + Ingest-Sessions.

Nutzt die bestehende SQLite (`db.connect`), die die Tabellen `project_meta`
und `ingest_session` bereits anlegt. Alle Funktionen akzeptieren eine offene
`conn`; ohne conn wird die Standard-DB geöffnet.
"""

import sqlite3
import uuid
from datetime import datetime

from ..db import connect

_SESSION_FIELDS = {
    "card_label", "card_uuid", "source_bytes", "source_files",
    "target_a_disk", "target_b_disk", "copies_done", "verify_status",
    "state", "started_at", "finished_at", "host",
}


def _conn(conn):
    return conn if conn is not None else connect()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def create_project(name, date, client=None, ff_notion_url=None,
                   created_by=None, conn=None) -> str:
    conn = _conn(conn)
    pid = uuid.uuid4().hex
    with conn:
        conn.execute(
            """INSERT INTO project_meta(id,name,date,client,ff_notion_url,
                 created_by,created_at,status)
               VALUES(?,?,?,?,?,?,?,'active')""",
            (pid, name, date, client, ff_notion_url, created_by, _now()),
        )
    return pid


def list_projects(status="active", conn=None) -> list[dict]:
    conn = _conn(conn)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM project_meta WHERE status=? ORDER BY created_at DESC",
        (status,),
    ).fetchall()
    return [dict(r) for r in rows]


def create_session(project_id, card_label, source_bytes, source_files,
                   card_uuid=None, host=None, conn=None) -> str:
    conn = _conn(conn)
    sid = uuid.uuid4().hex
    with conn:
        conn.execute(
            """INSERT INTO ingest_session(id,project_id,card_label,card_uuid,
                 source_bytes,source_files,copies_done,verify_status,state,
                 started_at,host)
               VALUES(?,?,?,?,?,?,0,'pending','detected',?,?)""",
            (sid, project_id, card_label, card_uuid, source_bytes,
             source_files, _now(), host),
        )
    return sid


def update_session(session_id, conn=None, **fields) -> None:
    conn = _conn(conn)
    unknown = set(fields) - _SESSION_FIELDS
    if unknown:
        raise ValueError(f"Unbekannte Session-Felder: {unknown}")
    if not fields:
        return
    cols = ", ".join(f"{k}=?" for k in fields)
    with conn:
        conn.execute(
            f"UPDATE ingest_session SET {cols} WHERE id=?",
            (*fields.values(), session_id),
        )


def _status(row) -> str:
    if row["state"] == "error" or row["verify_status"] == "mismatch":
        return "error"
    if (row["copies_done"] or 0) >= 2 and row["verify_status"] == "verified":
        return "2x_verified"
    if (row["copies_done"] or 0) == 1:
        return "1x"
    return "unsecured"


def board_rows(conn=None) -> list[dict]:
    """Je Session eine Zeile mit Projektname + abgeleitetem Kartenstatus."""
    conn = _conn(conn)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT s.*, p.name AS project_name, p.client AS client,
                  p.ff_notion_url AS ff_notion_url
           FROM ingest_session s JOIN project_meta p ON p.id = s.project_id
           WHERE p.status='active'
           ORDER BY s.started_at DESC""",
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["status"] = _status(r)
        out.append(d)
    return out


def project_payload(project_id, conn) -> dict:
    conn.row_factory = sqlite3.Row
    r = conn.execute("SELECT * FROM project_meta WHERE id=?",
                     (project_id,)).fetchone()
    return dict(r) if r else {}


def session_payload(session_id, conn) -> dict:
    conn.row_factory = sqlite3.Row
    r = conn.execute("SELECT * FROM ingest_session WHERE id=?",
                     (session_id,)).fetchone()
    return dict(r) if r else {}
