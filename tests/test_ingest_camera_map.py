from pathlib import Path

from storage_scanner.ingest import camera_map as cm


def _touch(p: Path, data: bytes = b"x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def test_discover_media_deep_and_shallow(tmp_path):
    # Sony: Video tief unter PRIVATE/M4ROOT/CLIP, Foto unter DCIM
    _touch(tmp_path / "PRIVATE" / "M4ROOT" / "CLIP" / "C0001.MP4")
    _touch(tmp_path / "DCIM" / "100MSDCF" / "DSC0001.JPG")
    _touch(tmp_path / "PRIVATE" / "M4ROOT" / "readme.txt")  # kein Medium

    media = cm.discover_media(tmp_path)
    vids = {p.name for p in media["video"]}
    photos = {p.name for p in media["photo"]}
    assert vids == {"C0001.MP4"}
    assert photos == {"DSC0001.JPG"}


def test_detect_vendor_sony(tmp_path):
    _touch(tmp_path / "PRIVATE" / "M4ROOT" / "CLIP" / "C0001.MP4")
    assert cm.detect_vendor(tmp_path) == "Sony"


def test_detect_vendor_gopro(tmp_path):
    _touch(tmp_path / "DCIM" / "100GOPRO" / "GX010001.MP4")
    assert cm.detect_vendor(tmp_path) == "GoPro"


def test_detect_vendor_unknown(tmp_path):
    _touch(tmp_path / "random" / "file.mp4")
    assert cm.detect_vendor(tmp_path) is None


def test_infer_camera_name_custom_prefix():
    files = [Path("FX3 JO00001.MP4"), Path("FX3 JO00002.MP4")]
    assert cm.infer_camera_name(files) == "FX3 JO"


def test_infer_camera_name_default_returns_none():
    files = [Path("C0001.MP4"), Path("C0002.MP4")]
    assert cm.infer_camera_name(files) is None

    files2 = [Path("DSC0001.JPG"), Path("DSC0002.JPG")]
    assert cm.infer_camera_name(files2) is None
