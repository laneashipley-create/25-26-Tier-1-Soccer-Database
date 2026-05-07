#!/usr/bin/env python3
"""Download Sportradar country flags (single size per country).

Uses the Country Flags manifest and downloads only ``h500-max-resize.png`` per
country. Reads ``API_KEY`` from ``config`` (same as the rest of this project),
with fallback to ``SPORTRADAR_API_KEY`` in the environment / ``.env``.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


MANIFEST_URL = "https://api.sportradar.com/flags-images-t3/sr/country-flags/flags/manifest.xml"
BASE_URL = "https://api.sportradar.com/flags-images-t3/sr"
FLAG_FILENAME = "h500-max-resize.png"
NS = {"sr": "http://feed.elasticstats.com/schema/assets/manifest-v2.5.xsd"}


def read_env_file(path: pathlib.Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def get_with_api_key(url: str, api_key: str) -> bytes:
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query.append(("api_key", api_key))
    new_query = urllib.parse.urlencode(query)
    full_url = urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment)
    )
    with urllib.request.urlopen(full_url, timeout=60) as response:
        return response.read()


def slugify(value: str) -> str:
    chars = []
    for ch in value.lower():
        if ch.isalnum():
            chars.append(ch)
        elif ch in (" ", "-", "_", "/"):
            chars.append("-")
    slug = "".join(chars).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "unknown-country"


def resolve_api_key(env_var_name: str) -> str:
    """Prefer ``config.API_KEY``, then OS env / .env-loaded vars."""
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    read_env_file(repo_root / ".env")
    read_env_file(repo_root / ".env.local")
    sys.path.insert(0, str(repo_root))
    try:
        from config import API_KEY  # noqa: E402

        if API_KEY:
            return str(API_KEY).strip()
    except ImportError:
        pass
    return (os.getenv(env_var_name) or "").strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        default="assets/sportradar-country-flags",
        help="Directory where files and index JSON will be written.",
    )
    parser.add_argument(
        "--api-key-env",
        default="SPORTRADAR_API_KEY",
        help="Fallback env var if config.API_KEY is empty.",
    )
    args = parser.parse_args()

    api_key = resolve_api_key(args.api_key_env)
    if not api_key:
        print(
            "Missing API key. Set API_KEY via config_local.py or SPORTRADAR_API_KEY.",
            file=sys.stderr,
        )
        return 1

    project_root = pathlib.Path(__file__).resolve().parents[1]
    os.chdir(project_root)

    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest_xml = get_with_api_key(MANIFEST_URL, api_key)
    manifest_path = output_dir / "manifest.xml"
    manifest_path.write_bytes(manifest_xml)

    root = ET.fromstring(manifest_xml)
    catalog = []
    downloaded = 0

    for asset in root.findall("sr:asset", NS):
        asset_id = asset.attrib.get("id", "")
        title_node = asset.find("sr:title", NS)
        country_name = "".join(title_node.itertext()).strip() if title_node is not None else ""
        ref_node = asset.find("sr:refs/sr:ref", NS)
        iso_code = ref_node.attrib.get("iso_country_code", "") if ref_node is not None else ""
        country_slug = slugify(country_name or iso_code or asset_id)
        country_dir = output_dir / country_slug
        country_dir.mkdir(parents=True, exist_ok=True)

        href_h500 = None
        meta_h500: dict[str, object] | None = None
        for link in asset.findall("sr:links/sr:link", NS):
            href = link.attrib.get("href", "") or ""
            if pathlib.Path(href).name != FLAG_FILENAME:
                continue
            href_h500 = href
            meta_h500 = {
                "width": int(link.attrib.get("width", "0") or "0"),
                "height": int(link.attrib.get("height", "0") or "0"),
            }
            break

        if not href_h500:
            print(
                f"warning: no {FLAG_FILENAME} for {country_name!r} ({asset_id})",
                file=sys.stderr,
            )
            catalog.append(
                {
                    "asset_id": asset_id,
                    "country_name": country_name,
                    "iso_country_code": iso_code,
                    "directory": str(country_dir.as_posix()),
                    "file": None,
                }
            )
            continue

        target_path = country_dir / FLAG_FILENAME
        data = get_with_api_key(f"{BASE_URL}{href_h500}", api_key)
        target_path.write_bytes(data)
        downloaded += 1

        catalog.append(
            {
                "asset_id": asset_id,
                "country_name": country_name,
                "iso_country_code": iso_code,
                "directory": str(country_dir.as_posix()),
                "file": {
                    "filename": FLAG_FILENAME,
                    "path": str(target_path.as_posix()),
                    "relative_to_repo": pathlib.Path(
                        os.path.relpath(target_path.resolve(), project_root.resolve())
                    ).as_posix(),
                    "width": meta_h500["width"] if meta_h500 else 0,
                    "height": meta_h500["height"] if meta_h500 else 0,
                    "source_href": href_h500,
                },
            }
        )

    index_path = output_dir / "index.json"
    index_path.write_text(json.dumps(catalog, indent=2), encoding="utf-8")

    print(f"Saved manifest: {manifest_path}")
    print(f"Saved index: {index_path}")
    print(f"Downloaded files: {downloaded}")
    print(f"Countries: {len(catalog)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
