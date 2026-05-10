"""Shared top navigation for static HTML reports (GitHub Pages)."""

from __future__ import annotations

import html

from config import REPORT_HTML

# (href relative to site root, short label)
REPORT_NAV_ITEMS: list[tuple[str, str]] = [
    ("report_hub.html", "Home"),
    ("report_master_games.html", "List of All Games"),
    ("wc2026_schedule.html", "WC 2026 schedule"),
    (REPORT_HTML, "Own goals"),
    ("report_penalty_shootouts.html", "Penalty shootouts"),
    ("report_var_events.html", "VAR events"),
    ("report_var_unpaired.html", "VAR unpaired"),
    ("report_water_break_events.html", "Water break events"),
    ("report_water_break_unpaired.html", "Water break unpaired"),
    ("report_recordings_library.html", "Recordings library"),
]

NAV_CSS = """
    .report-sticky-top {
      position: sticky;
      top: 0;
      z-index: 200;
      box-shadow: 0 4px 14px rgba(0,0,0,0.35);
    }
    .report-nav {
      display: flex;
      flex-wrap: nowrap;
      align-items: stretch;
      justify-content: flex-start;
      gap: 0;
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: thin;
      text-align: left;
      background: #1a0000;
      padding: 0.35rem 0.5rem;
      font-size: 0.88rem;
      border-bottom: 1px solid #440000;
    }
    .report-nav a,
    .report-nav .nav-active {
      display: inline-flex;
      align-items: center;
      flex-shrink: 0;
      min-height: 2.75rem;
      padding: 0.35rem 0.5rem;
      color: #ff9999;
      text-decoration: none;
      white-space: nowrap;
    }
    .report-nav a:hover { text-decoration: underline; color: #ffcccc; }
    .report-nav .nav-active {
      color: #fff;
      font-weight: 700;
    }
    .report-nav .nav-sep {
      display: inline-flex;
      align-items: center;
      flex-shrink: 0;
      color: #5c3030;
      font-size: 0.75rem;
      padding: 0 0.05rem;
      user-select: none;
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
