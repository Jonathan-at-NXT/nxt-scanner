"""Ingest-State-Machine: Karte -> 2 Ziele -> verifiziert -> flach sortiert.

Sicherer Ablauf (Reihenfolge ist die Datenschutz-Invariante):
  detected -> copying -> verifying -> done   (oder error/mismatch)
Die Quelle wird nur gelesen; das Board wird erst bei 2 verifizierten Kopien grün.
"""

import logging
from datetime import datetime
from pathlib import Path

from . import camera_map, organize
from .copier import copy_to_targets
from .store import update_session

_log = logging.getLogger("ingest.orchestrator")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _pairs_for_target(target_root, slug, date, camera, video, photo):
    """(src, dst)-Paare für ein Ziel, Video->FOOTAGE + Foto->PHOTOS, flach."""
    pairs = []
    vdir = organize.target_dir(target_root, slug, date, "video", camera)
    pairs += organize.plan_flatten(video, vdir)
    pdir = organize.target_dir(target_root, slug, date, "photo", camera)
    pairs += organize.plan_flatten(photo, pdir)
    return pairs


def run_ingest(project, card_root, camera, date, target_a, target_b, conn,
               session_id, host=None, progress=None) -> dict:
    slug = project["name"]
    card_root = Path(card_root)

    try:
        media = camera_map.discover_media(card_root)
        video, photo = media["video"], media["photo"]
        source_files = len(video) + len(photo)
        source_bytes = sum(p.stat().st_size for p in video + photo)

        update_session(
            session_id, conn=conn, state="copying",
            source_files=source_files, source_bytes=source_bytes,
            target_a_disk=str(target_a), target_b_disk=str(target_b),
        )

        pairs_a = _pairs_for_target(target_a, slug, date, camera, video, photo)
        pairs_b = _pairs_for_target(target_b, slug, date, camera, video, photo)

        update_session(session_id, conn=conn, state="verifying")
        res = copy_to_targets(pairs_a, pairs_b, progress=progress)

        if res["verified_a"] and res["verified_b"]:
            fields = dict(state="done", verify_status="verified",
                          copies_done=2, finished_at=_now())
        else:
            fields = dict(state="error", verify_status="mismatch",
                          finished_at=_now())
        update_session(session_id, conn=conn, **fields)

    except Exception as e:  # noqa: BLE001 - Fehler wird als Session-Status geführt
        _log.error("Ingest fehlgeschlagen (session %s): %s", session_id, e)
        update_session(session_id, conn=conn, state="error", finished_at=_now())
        fields = dict(state="error")

    from .store import session_payload
    return session_payload(session_id, conn)
