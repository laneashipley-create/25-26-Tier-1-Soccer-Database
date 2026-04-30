"""Shared top navigation for static HTML reports (GitHub Pages)."""

from __future__ import annotations

import html

from config import REPORT_HTML

# (href relative to site root, short label)
REPORT_NAV_ITEMS: list[tuple[str, str]] = [
    ("report_hub.html", "Home"),
    (REPORT_HTML, "Own goals"),
    ("report_penalty_shootouts.html", "Penalty shootouts"),
    ("report_var_events.html", "VAR events"),
    ("report_var_unpaired.html", "VAR unpaired"),
]

NAV_CSS = """
    .report-sticky-top {
      position: sticky;
      top: 0;
      z-index: 200;
      box-shadow: 0 4px 14px rgba(0,0,0,0.35);
    }
    .report-nav {
      text-align: center;
      background: #1a0000;
      padding: 0.55rem 1rem 0.65rem;
      font-size: 0.88rem;
      border-bottom: 1px solid #440000;
    }
    .report-nav a {
      color: #ff9999;
      margin: 0 0.4rem;
      text-decoration: none;
    }
    .report-nav a:hover { text-decoration: underline; color: #ffcccc; }
    .report-nav .nav-active {
      color: #fff;
      font-weight: 700;
      margin: 0 0.4rem;
    }
"""


def navigation_html(current_href: str) -> str:
    """Breadcrumb-style links; `current_href` matches REPORT_NAV_ITEMS href (e.g. report_own_goals.html)."""
    parts: list[str] = []
    for href, label in REPORT_NAV_ITEMS:
        esc_h = html.escape(href, quote=True)
        esc_l = html.escape(label, quote=False)
        if href == current_href:
            parts.append(f'<span class="nav-active">{esc_l}</span>')
        else:
            parts.append(f'<a href="{esc_h}">{esc_l}</a>')
    sep = '\n      <span class="nav-sep" aria-hidden="true"> · </span>\n      '
    inner = sep.join(parts)
    return f'  <nav class="report-nav" aria-label="Reports">{inner}\n  </nav>'
