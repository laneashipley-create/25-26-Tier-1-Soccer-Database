"""
Build the final HTML email body for the weekly update.

Usage:
  python build_email_body.py summary.html output.html
"""

from __future__ import annotations

import sys
from pathlib import Path


REPORT_URL = "https://laneashipley-create.github.io/25-26-Tier-1-Soccer-Database/report.html"
IMG_URL = "https://laneashipley-create.github.io/25-26-Tier-1-Soccer-Database/assets/lanes_sportsdata.png"


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: python build_email_body.py summary.html output.html")
        return 1

    summary_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    summary_html = summary_path.read_text(encoding="utf-8") if summary_path.exists() else ""

    html = (
        "<p>Esteemed Colleague,</p>"
        "<p>Your weekly Tier 1 - Own Goals report has been updated.</p>"
        f"{summary_html}"
        f'<p><a href="{REPORT_URL}">View the report</a></p>'
        "<p>Kind Regards,</p>"
        "<p>Lane's SportsData Bot</p>"
        f'<p><img src="{IMG_URL}" alt="Lane\'s SportsData" width="360" '
        'style="max-width:360px;height:auto;display:block;" /></p>'
    )

    output_path.write_text(html, encoding="utf-8")
    print(f"Wrote email body to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
