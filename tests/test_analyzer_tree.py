import os
import subprocess
from pathlib import Path
from storage_scanner.analyzer import walk_tree


def _mkfile(p: Path, kb: int):
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        f.write(b"\0" * (kb * 1024))


def test_walk_tree_depths_and_cumulative(tmp_path):
    # top/ (depth0) -> top/a (depth1) -> top/a/x (depth2) -> top/a/x/deep (depth3, jenseits sub_depth)
    _mkfile(tmp_path / "top" / "root.bin", 100)
    _mkfile(tmp_path / "top" / "a" / "a.bin", 200)
    _mkfile(tmp_path / "top" / "a" / "x" / "x.bin", 300)
    _mkfile(tmp_path / "top" / "a" / "x" / "deep" / "d.bin", 400)

    nodes = walk_tree(tmp_path / "top", sub_depth=2)
    by = {n["rel_path"]: n for n in nodes}

    # Knotenmenge: "", "a", "a/x"  (deep liegt jenseits sub_depth=2 -> kein eigener Knoten)
    assert set(by) == {"", "a", "a/x"}
    assert by[""]["depth"] == 0 and by[""]["parent_rel_path"] is None
    assert by["a"]["depth"] == 1 and by["a"]["parent_rel_path"] == ""
    assert by["a/x"]["depth"] == 2 and by["a/x"]["parent_rel_path"] == "a"

    # kumulative Dateizahlen
    assert by[""]["file_count"] == 4      # root.bin + a.bin + x.bin + d.bin
    assert by["a"]["file_count"] == 3     # a.bin + x.bin + d.bin
    assert by["a/x"]["file_count"] == 2   # x.bin + d.bin (inkl. deep)

    # kumulative Größen (>= Summe der bekannten Nutzbytes, Blockgröße toleriert)
    assert by[""]["size_bytes"] >= (100 + 200 + 300 + 400) * 1024
    assert by["a/x"]["size_bytes"] >= (300 + 400) * 1024
    assert by[""]["size_bytes"] > by["a"]["size_bytes"] > by["a/x"]["size_bytes"]
