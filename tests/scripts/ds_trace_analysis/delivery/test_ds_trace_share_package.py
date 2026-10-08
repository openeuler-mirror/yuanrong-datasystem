from trace_test_loader import load_fresh
from pathlib import Path
import pytest

share = load_fresh("packaging")


def test_linked_only_content_deduplication_and_dynamic_download(tmp_path):
    root = tmp_path / "input"
    root.mkdir()
    (root / "index.html").write_text(
        '<html><head></head><body><a href="a.bin">a</a><a href="child.html#x">child</a></body></html>'
    )
    (root / "child.html").write_text(
        '<head></head><script>const data={"download_path":"b.bin"}</script>'
    )
    (root / "a.bin").write_bytes(b"raw input bytes")
    (root / "b.bin").write_bytes(b"raw input bytes")
    (root / "summary.json").write_text("not linked")
    output = tmp_path / "out"
    result = share.export(root, output, ["index.html"])
    assert result["input_files"] == 2
    assert result["files"]["a.bin"] == result["files"]["b.bin"]
    assert not (output / "summary.json").exists()
    assert "child.html#x" in (output / "index.html").read_text()
    assert '"download_path":"assets/' in (output / "child.html").read_text()
    assert (output / result["files"]["a.bin"]).read_bytes() == b"raw input bytes"


def test_input_tree_and_existing_output_are_protected(tmp_path):
    root = tmp_path / "in"
    root.mkdir()
    for out in [root, root / "nested", tmp_path]:
        with pytest.raises(ValueError):
            share.export(root, out, [])


def test_large_duplicate_inputs_have_bounded_memory(tmp_path):
    import hashlib
    import tracemalloc

    root = tmp_path / "in"
    root.mkdir()
    page = '<head></head><a href="a.bin">a</a><a href="b.bin">b</a>'
    (root / "index.html").write_text(page)
    block = b"x" * share.ASSET_CHUNK_BYTES
    blocks = 16
    digest = hashlib.sha256()
    for _ in range(blocks):
        digest.update(block)
    for name in ("a.bin", "b.bin"):
        with (root / name).open("wb") as output:
            for _ in range(blocks):
                output.write(block)
    tracemalloc.start()
    try:
        result = share.export(root, tmp_path / "out", ["index.html"])
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 8 * share.ASSET_CHUNK_BYTES
    assert result["files"]["a.bin"] == result["files"]["b.bin"]
    target = tmp_path / "out" / result["files"]["a.bin"]
    assert target.stem == digest.hexdigest()
    assert target.stat().st_size == blocks * len(block)
    assert result["input_bytes"] == 2 * blocks * len(block) + len(page)
    assert len(list(target.parent.iterdir())) == 2


def test_escape_and_missing_dependency_fail_before_output(tmp_path):
    root = tmp_path / "in"
    root.mkdir()
    for link in ["../outside.log", "missing.html"]:
        (root / "index.html").write_text('<a href="' + link + '">bad</a>')
        with pytest.raises(ValueError):
            share.export(root, tmp_path / "out", ["index.html"])
        assert not (tmp_path / "out").exists()


def test_large_echarts_asset_is_shared_and_small_scripts_keep_order(tmp_path):
    root = tmp_path / "in"
    root.mkdir()
    library = "/* echarts */" + " " * 100001
    (root / "index.html").write_text(
        "<head></head><script>" + library + "</script><script>useCharts()</script>"
    )
    result = share.export(root, tmp_path / "out", ["index.html"])
    page = (tmp_path / "out/index.html").read_text()
    assert '<script src="assets/' in page
    assert "<script>useCharts()</script>" in page
    assert result["unique_assets"] == 2


def test_echarts_cdn_reference_uses_bundled_library(tmp_path):
    root = tmp_path / "in"
    root.mkdir()
    (root / "index.html").write_text(
        '<head></head><script src="https://cdn.example/dist/echarts.min.js"></script>'
    )
    share.export(root, tmp_path / "out", ["index.html"])
    page = (tmp_path / "out/index.html").read_text()
    assert "https://cdn.example" not in page
    assert '<script src="assets/' in page


def test_real_overview_dynamic_download_is_copied_and_rewritten(tmp_path):
    overview = load_fresh("overview")
    root = tmp_path / "input"
    root.mkdir()
    (root / "suite.analysis.json").write_text('{"valid": true}')
    (root / "index.html").write_text(overview.render({
        "title": "Example", "runs": [], "analysis_download": "suite.analysis.json",
    }, "/* echarts */"))
    output = tmp_path / "output"
    result = share.export(root, output, ["index.html"])
    target = result["files"]["suite.analysis.json"]
    assert (output / target).read_text() == '{"valid": true}'
    assert share.overview_downloads((output / "index.html").read_text()) == [target]


@pytest.mark.parametrize("href", ["../outside.json", "missing.json"])
def test_overview_dynamic_download_respects_dependency_boundaries(tmp_path, href):
    import json
    root = tmp_path / "input"
    root.mkdir()
    (root / "index.html").write_text(
        '<script>const REPORT_DATA=' + json.dumps({"downloads": [{"href": href}]}) + ';</script>'
    )
    output = tmp_path / "output"
    with pytest.raises(ValueError):
        share.export(root, output, ["index.html"])
    assert not output.exists()


def test_json_escaped_overview_download_is_rewritten(tmp_path):
    import json
    root = tmp_path / "input"
    root.mkdir()
    name = "model<version.json"
    (root / name).write_text("{}")
    data = json.dumps({"downloads": [{"href": name}]}).replace("<", "\\u003c")
    (root / "index.html").write_text('<script>const REPORT_DATA=' + data + ';</script>')
    output = tmp_path / "output"
    result = share.export(root, output, ["index.html"])
    assert share.overview_downloads((output / "index.html").read_text()) == [result["files"][name]]


def test_interrupted_asset_read_removes_temporary_file(tmp_path, monkeypatch):
    import io
    source = tmp_path / "input.bin"
    source.write_bytes(b"source")
    assets = tmp_path / "assets"
    assets.mkdir()
    original_open = Path.open

    class InterruptedRead(io.BytesIO):
        def __init__(self):
            super().__init__(b"partial data")
            self.calls = 0

        def read(self, size=-1):
            self.calls += 1
            if self.calls > 1:
                raise OSError("injected read failure")
            return super().read(size)

    def open_source(path, *args, **kwargs):
        return InterruptedRead() if path == source else original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_source)
    with pytest.raises(OSError, match="injected read failure"):
        share.copy_asset(source, assets)
    assert not list(assets.iterdir())
