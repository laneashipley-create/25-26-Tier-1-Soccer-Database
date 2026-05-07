from __future__ import annotations

import base64
import pathlib


ROOT = pathlib.Path(__file__).resolve().parents[1]
HTML_PATH = ROOT / "wc2026_schedule.html"
CSV_PATH = ROOT / "wc2026_schedule.csv"


def main() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")
    start = html.index('    const EMBEDDED_CSV = "')
    end = html.index("\n\n    const SUPABASE_URL", start)
    b64 = base64.b64encode(CSV_PATH.read_bytes()).decode("ascii")
    replacement = (
        f'    const EMBEDDED_CSV_BASE64 = "{b64}";\n'
        "    const EMBEDDED_CSV = atob(EMBEDDED_CSV_BASE64);"
    )
    updated = html[:start] + replacement + html[end:]
    HTML_PATH.write_text(updated, encoding="utf-8")
    print("Fixed embedded CSV encoding")


if __name__ == "__main__":
    main()
