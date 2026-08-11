from storage_scanner.ingest import copier


def test_copy_file_bytewise(tmp_path):
    src = tmp_path / "src" / "a.bin"
    src.parent.mkdir()
    data = b"hello" * 100000
    src.write_bytes(data)
    dst = tmp_path / "out" / "sub" / "a.bin"

    copier.copy_file(src, dst)
    assert dst.read_bytes() == data
    assert not (dst.parent / "a.bin.tmp").exists()  # tmp aufgeräumt


def test_copy_file_resumable_skips(tmp_path):
    src = tmp_path / "a.bin"
    src.write_bytes(b"x" * 5000)
    dst = tmp_path / "out" / "a.bin"
    copier.copy_file(src, dst)
    first_mtime = dst.stat().st_mtime_ns

    # zweiter Lauf: valide Kopie existiert -> nicht neu kopieren
    copier.copy_file(src, dst)
    assert dst.stat().st_mtime_ns == first_mtime


def test_copy_to_targets_dual(tmp_path):
    src = tmp_path / "card" / "C0001.MP4"
    src.parent.mkdir()
    src.write_bytes(b"video-bytes" * 1000)
    a = tmp_path / "A" / "C0001.MP4"
    b = tmp_path / "B" / "C0001.MP4"

    res = copier.copy_to_targets([(src, a)], [(src, b)])
    assert res["copied"] == 2
    assert res["verified_a"] is True
    assert res["verified_b"] is True
    assert a.read_bytes() == src.read_bytes()
    assert b.read_bytes() == src.read_bytes()
