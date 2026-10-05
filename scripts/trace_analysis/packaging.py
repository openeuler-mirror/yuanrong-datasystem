#!/usr/bin/env python3
"""Export linked local report artifacts, deduplicating assets and raw inputs."""
from __future__ import annotations

import argparse
import hashlib
from html import escape
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
from urllib.parse import unquote, urlsplit
import zipfile

from .resources import asset_path, echarts_path
from .delivery_validation import validate_pipeline_bundle

ASSET_CHUNK_BYTES = 1024 * 1024


def copy_asset(source, assets):
    digest = hashlib.sha256()
    size = 0
    temporary = None
    try:
        with source.open("rb") as stream, tempfile.NamedTemporaryFile(
            dir=assets, delete=False
        ) as output:
            temporary = Path(output.name)
            while True:
                chunk = stream.read(ASSET_CHUNK_BYTES)
                if not chunk:
                    break
                digest.update(chunk)
                output.write(chunk)
                size += len(chunk)
        target = assets / (digest.hexdigest() + source.suffix)
        if target.exists():
            temporary.unlink()
        else:
            temporary.replace(target)
        return target, size
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []

    def handle_starttag(self, tag, attrs):
        for name, value in attrs:
            if name in ("href", "src") and value:
                self.urls.append(value)



def overview_downloads(text):
    marker = re.search(r"<script>\s*const\s+REPORT_DATA\s*=\s*", text)
    if not marker:
        return []
    data, _ = json.JSONDecoder().raw_decode(text[marker.end():])
    if not isinstance(data, dict):
        raise ValueError("Overview data must be an object")
    downloads = data.get("downloads", [])
    if not isinstance(downloads, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("href"), str) for item in downloads
    ):
        raise ValueError("Overview downloads must contain href strings")
    return [item["href"] for item in downloads]


def contained(root, path):
    path = path.resolve()
    if not path.is_relative_to(root):
        raise ValueError("Report dependency escapes the input directory")
    return path


def packaged_page_path(root, source):
    relative = source.relative_to(root)
    parts = relative.parts
    is_run_triage_path = len(parts) == 5 and parts[0] == "runs" and parts[2] == "triage"
    is_local_report = is_run_triage_path and parts[4] == "report.local.html"
    if is_local_report:
        short_generation = hashlib.sha256(relative.as_posix().encode()).hexdigest()[:12]
        return Path("runs") / parts[1] / "triage" / short_generation / parts[4]
    return relative


def export(root, output, entrypoints, *, manifest=None):
    root = root.resolve()
    output = output.resolve()
    if output == root or output.is_relative_to(root) or root.is_relative_to(output):
        raise ValueError("Input and output trees must be disjoint")
    if output.exists():
        raise ValueError("Output must be a new directory")
    delivery = validate_pipeline_bundle(root, allow_legacy=True)
    if not delivery["valid"]:
        raise ValueError("delivery validation failed: " + "; ".join(delivery["errors"]))
    selected_root = Path(delivery.pop("source_directory"))
    entries = list(entrypoints)
    if delivery["kind"] == "publication":
        prefix = selected_root.relative_to(root) if selected_root.is_relative_to(root) else Path()
        entries = [str(Path(entry).relative_to(prefix)) if prefix.parts and Path(entry).is_relative_to(prefix)
                   else entry for entry in entries]
        manifest = selected_root / "suite.manifest.json"
        entries.append("index.html")
    root = selected_root
    homepage_links = []
    if manifest is not None:
        selected_manifest = json.loads(Path(manifest).read_text(encoding="utf-8"))
        for run in selected_manifest["runs"]:
            for key in (
                "triage_report", "bottleneck_report", "write_bottleneck_report", "numa_report",
                "set_triage_report", "set_numa_report", "issues_analysis_json",
            ):
                if run.get(key):
                    entries.append(run[key])
                    if key.endswith("report"):
                        homepage_links.append(run[key])
    if delivery["kind"] == "legacy":
        delivery = None
    queue = [contained(root, root / entry) for entry in entries]
    pages = {}
    files = {}
    while queue:
        source = queue.pop()
        if source in pages or source in files:
            continue
        if not source.is_file():
            raise ValueError(
                "Missing report dependency: " + str(source.relative_to(root))
            )
        if source.suffix.lower() != ".html":
            files[source] = 0
            continue
        text = source.read_text(encoding="utf-8")
        links = Links()
        links.feed(text)
        if source == root / "index.html":
            links.urls.extend(homepage_links)
        links.urls.extend(overview_downloads(text))
        links.urls.extend(re.findall(r'"download_path"\s*:\s*"([^"\\]+)"', text))
        pages[source] = (text, links.urls)
        for url in links.urls:
            parsed = urlsplit(url)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            dependency = contained(root, source.parent / unquote(parsed.path))
            if dependency.suffix.lower() == ".html" or dependency.is_file():
                queue.append(dependency)
            else:
                raise ValueError("Missing report dependency")
    echarts_notices = [(relative, echarts_path().parent / relative)
                       for relative in ("LICENSE", "NOTICE", "licenses/LICENSE-d3", "provenance.json")]
    missing_notices = [relative for relative, source in echarts_notices if not source.is_file()]
    if missing_notices:
        raise ValueError("ECharts notice missing: " + ", ".join(missing_notices))
    output.mkdir(parents=True)
    assets = output / "assets"
    assets.mkdir()
    notice_paths = []
    for relative, source in echarts_notices:
        target = output / "THIRD_PARTY" / "echarts" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        notice_paths.append(target.relative_to(output).as_posix())
    shared_css = asset_path("shared.css")
    css_payload = shared_css.read_bytes()
    css_target = assets / (hashlib.sha256(css_payload).hexdigest() + ".css")
    css_target.write_bytes(css_payload)
    mapping = {}
    for source in files:
        target, files[source] = copy_asset(source, assets)
        mapping[source] = target
    for source in pages:
        mapping[source] = output / packaged_page_path(root, source)
    if len({mapping[source] for source in pages}) != len(pages):
        raise ValueError("Packaged report paths collide")
    for source, (text, urls) in pages.items():
        target = mapping[source]
        target.parent.mkdir(parents=True, exist_ok=True)
        for url in sorted(set(urls), key=len, reverse=True):
            parsed = urlsplit(url)
            if parsed.scheme in ("http", "https") and parsed.path.endswith(
                "/echarts.min.js"
            ):
                library_path = (
                    echarts_path()
                )
                payload = library_path.read_bytes()
                local_library = assets / (hashlib.sha256(payload).hexdigest() + ".js")
                local_library.write_bytes(payload)
                relative = Path(
                    os.path.relpath(local_library, target.parent)
                ).as_posix()
                text = text.replace('"' + url + '"', '"' + relative + '"')
                text = text.replace("'" + url + "'", "'" + relative + "'")
                continue
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            dependency = contained(root, source.parent / unquote(parsed.path))
            relative = Path(
                os.path.relpath(mapping[dependency], target.parent)
            ).as_posix()
            if parsed.query:
                relative += "?" + parsed.query
            if parsed.fragment:
                relative += "#" + parsed.fragment
            text = text.replace(
                json.dumps(url, ensure_ascii=False).replace("<", "\\u003c"),
                json.dumps(relative, ensure_ascii=False).replace("<", "\\u003c"),
            )
            for quote in ('"', "'"):
                text = text.replace(
                    quote + escape(url, quote=True) + quote,
                    quote + escape(relative, quote=True) + quote,
                )
                text = text.replace(quote + url + quote, quote + relative + quote)
            text = text.replace(
                '<a download href="' + escape(relative, quote=True) + '"',
                '<a download="'
                + escape(dependency.name, quote=True)
                + '" href="'
                + escape(relative, quote=True)
                + '"',
            )

        # Only large inline libraries are extracted; page data and small scripts keep order.
        def library(match):
            script = match.group(1)
            if len(script) < 100000 or "echarts" not in script[:5000]:
                return match.group(0)
            digest = hashlib.sha256(script.encode()).hexdigest()
            shared = assets / (digest + ".js")
            shared.write_text(script, encoding="utf-8")
            relative = Path(os.path.relpath(shared, target.parent)).as_posix()
            return '<script src="' + relative + '"></script>'

        text = re.sub(r"<script>(.*?)</script>", library, text, flags=re.S)
        stylesheet = Path(os.path.relpath(css_target, target.parent)).as_posix()
        text = text.replace(
            "</head>", '<link rel="stylesheet" href="' + stylesheet + '"></head>'
        )
        target.write_text(text, encoding="utf-8")
    result = {
        "html_pages": len(pages),
        "input_files": len(files),
        "unique_assets": len(list(assets.iterdir())),
        "input_bytes": sum(files.values())
        + sum(len(v[0].encode()) for v in pages.values()),
        "output_bytes": sum(p.stat().st_size for p in output.rglob("*") if p.is_file()),
        "third_party_notices": notice_paths,
        "files": {
            str(p.relative_to(root)): str(t.relative_to(output))
            for p, t in mapping.items()
        },
    }
    if delivery is not None:
        result["delivery_validation"] = delivery
    (output / "package.manifest.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--entry",
        action="append",
        default=[],
        help="HTML path relative to root; repeat for dynamically linked run pages",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Suite manifest with per-run report links relative to root",
    )
    parser.add_argument("--zip", type=Path)
    args = parser.parse_args()
    if args.zip and (
        args.zip.exists() or args.zip.resolve().is_relative_to(args.output.resolve())
    ):
        parser.error("ZIP must be a new file outside the output directory")
    if not args.entry and not args.manifest:
        parser.error("At least one --entry or --manifest is required")
    result = export(args.root, args.output, args.entry, manifest=args.manifest)
    if args.zip:
        if args.zip.exists():
            raise ValueError("Refusing to overwrite an existing archive")
        with zipfile.ZipFile(
            args.zip, "w", zipfile.ZIP_DEFLATED, compresslevel=6
        ) as archive:
            for path in sorted(args.output.rglob("*")):
                if path.is_file():
                    archive.write(
                        path,
                        args.output.name
                        + "/"
                        + path.relative_to(args.output).as_posix(),
                    )
        with zipfile.ZipFile(args.zip) as archive:
            if archive.testzip() is not None:
                raise ValueError("Archive CRC validation failed")
        result["zip_bytes"] = args.zip.stat().st_size
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
