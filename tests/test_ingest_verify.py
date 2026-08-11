from storage_scanner.ingest import verify


def test_verify_copy_identical(tmp_path):
    src = tmp_path / "a.bin"
    dst = tmp_path / "b.bin"
    data = b"0123456789" * 1000
    src.write_bytes(data)
    dst.write_bytes(data)
    assert verify.verify_copy(src, dst) is True


def test_verify_copy_size_mismatch(tmp_path):
    src = tmp_path / "a.bin"
    dst = tmp_path / "b.bin"
    src.write_bytes(b"x" * 100)
    dst.write_bytes(b"x" * 99)
    assert verify.verify_copy(src, dst) is False


def test_verify_copy_changed_middle_byte(tmp_path):
    # Größe gleich, aber ein Byte in der Mitte kippt -> Fingerprint muss abweichen
    n = 12 * 1024 * 1024
    data = bytearray(b"a" * n)
    src = tmp_path / "a.bin"
    src.write_bytes(bytes(data))
    data[n // 2] = ord("b")
    dst = tmp_path / "b.bin"
    dst.write_bytes(bytes(data))
    assert verify.verify_copy(src, dst) is False


def test_fingerprint_stable(tmp_path):
    p = tmp_path / "a.bin"
    p.write_bytes(b"hello world" * 500)
    assert verify.fingerprint(p) == verify.fingerprint(p)
