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
      justify-content: safe center;
      gap: 0;
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      scrollbar-width: thin;
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


COLUMN_RESIZE_CSS = """
    .table-wrap .col-resize-handle {
      position: absolute;
      top: 0;
      right: -6px;
      width: 13px;
      height: 100%;
      cursor: col-resize;
      user-select: none;
      z-index: 10;
      background: transparent;
      transition: background 0.12s ease;
    }
    .table-wrap .col-resize-handle::after {
      content: "";
      position: absolute;
      top: 12%;
      bottom: 12%;
      left: 50%;
      width: 2px;
      background: rgba(255, 170, 170, 0.55);
      box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.25);
      border-radius: 1px;
      transform: translateX(-50%);
      pointer-events: none;
      transition: background 0.12s ease, width 0.12s ease;
    }
    .table-wrap .col-resize-handle:hover,
    .table-wrap .col-resize-handle.is-active {
      background: rgba(255, 153, 153, 0.55);
    }
    .table-wrap .col-resize-handle:hover::after,
    .table-wrap .col-resize-handle.is-active::after {
      background: #ffffff;
      width: 3px;
    }
    .table-wrap thead tr:first-child th {
      position: relative;
    }
    body.is-col-resizing,
    body.is-col-resizing * {
      cursor: col-resize !important;
      user-select: none !important;
    }
    .table-resize-toolbar {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      flex-wrap: wrap;
    }
    .col-resize-reset-btn {
      font: inherit;
      cursor: pointer;
      font-size: 0.7rem;
      font-weight: 600;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      padding: 0.3rem 0.55rem;
      border-radius: 6px;
      border: 1px solid #ddd8d0;
      background: #faf8f5;
      color: #444;
    }
    .col-resize-reset-btn:hover {
      border-color: #cc0000;
      color: #990000;
      background: #fff0f0;
    }
    @media (max-width: 899px) {
      .table-wrap .col-resize-handle { display: none; }
      .col-resize-reset-btn { display: none; }
    }
"""


COLUMN_RESIZE_SCRIPT = r"""<script>
(function () {
  if (window.__reportColumnResizeInit) return;
  window.__reportColumnResizeInit = true;

  var STORAGE_PREFIX = "report-col-widths:";
  var MIN_COL_PX = 48;

  function storageKey(table) {
    return STORAGE_PREFIX + (table.id || table.dataset.colResizeKey || "anon");
  }

  function ensureColgroup(table) {
    var colgroup = table.querySelector(":scope > colgroup");
    var headerRow = table.querySelector("thead tr:first-child");
    if (!headerRow) return null;
    var ths = headerRow.querySelectorAll("th");
    if (!colgroup) {
      colgroup = document.createElement("colgroup");
      for (var i = 0; i < ths.length; i++) colgroup.appendChild(document.createElement("col"));
      table.insertBefore(colgroup, table.firstChild);
    } else {
      while (colgroup.children.length < ths.length) {
        colgroup.appendChild(document.createElement("col"));
      }
    }
    return colgroup;
  }

  function saveWidths(table) {
    var colgroup = table.querySelector(":scope > colgroup");
    if (!colgroup) return;
    var widths = Array.prototype.map.call(colgroup.children, function (col) {
      return col.style.width || "";
    });
    try {
      localStorage.setItem(storageKey(table), JSON.stringify(widths));
    } catch (e) { /* ignore */ }
  }

  function loadWidths(table) {
    try {
      var raw = localStorage.getItem(storageKey(table));
      if (!raw) return null;
      var arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr : null;
    } catch (e) { return null; }
  }

  function clearWidths(table) {
    try { localStorage.removeItem(storageKey(table)); } catch (e) { /* ignore */ }
  }

  function applyStoredWidths(table) {
    var stored = loadWidths(table);
    if (!stored) return false;
    var colgroup = ensureColgroup(table);
    if (!colgroup) return false;
    var cols = colgroup.children;
    var applied = false;
    for (var i = 0; i < cols.length && i < stored.length; i++) {
      if (stored[i]) {
        cols[i].style.width = stored[i];
        applied = true;
      }
    }
    return applied;
  }

  function snapshotInitialWidths(table) {
    var colgroup = ensureColgroup(table);
    if (!colgroup) return;
    var ths = table.querySelectorAll("thead tr:first-child th");
    var cols = colgroup.children;
    for (var i = 0; i < ths.length; i++) {
      var c = cols[i];
      if (!c) continue;
      if (!c.style.width) {
        var w = ths[i].getBoundingClientRect().width;
        if (w > 0) c.style.width = Math.round(w) + "px";
      }
    }
    table.style.tableLayout = "fixed";
  }

  function startResize(e, col, th, handle, table) {
    e.preventDefault();
    e.stopPropagation();
    var isTouch = e.type === "touchstart";
    var startX = isTouch ? e.touches[0].pageX : e.pageX;
    var startWidth = col.getBoundingClientRect().width || th.getBoundingClientRect().width;
    document.body.classList.add("is-col-resizing");
    handle.classList.add("is-active");

    function onMove(ev) {
      var x = ev.touches ? ev.touches[0].pageX : ev.pageX;
      var newWidth = Math.max(MIN_COL_PX, Math.round(startWidth + (x - startX)));
      col.style.width = newWidth + "px";
      if (ev.cancelable) ev.preventDefault();
    }
    function onUp() {
      document.body.classList.remove("is-col-resizing");
      handle.classList.remove("is-active");
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      document.removeEventListener("touchmove", onMove);
      document.removeEventListener("touchend", onUp);
      saveWidths(table);
    }
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
    document.addEventListener("touchmove", onMove, { passive: false });
    document.addEventListener("touchend", onUp);
  }

  function setupTable(table) {
    if (!table || table.dataset.colResizeInit === "1") return;
    var headerRow = table.querySelector("thead tr:first-child");
    if (!headerRow) return;
    var ths = headerRow.querySelectorAll("th");
    if (ths.length < 2) return;

    var hadStored = applyStoredWidths(table);
    if (!hadStored) snapshotInitialWidths(table);
    else table.style.tableLayout = "fixed";

    var colgroup = ensureColgroup(table);
    var cols = colgroup.children;

    Array.prototype.forEach.call(ths, function (th, idx) {
      if (idx >= cols.length - 1) return;
      if (th.querySelector(":scope > .col-resize-handle")) return;
      var handle = document.createElement("span");
      handle.className = "col-resize-handle";
      handle.setAttribute("aria-hidden", "true");
      handle.addEventListener("click", function (ev) { ev.stopPropagation(); });
      handle.addEventListener("dblclick", function (ev) {
        ev.stopPropagation();
        ev.preventDefault();
        cols[idx].style.width = "";
        snapshotInitialWidths(table);
        saveWidths(table);
      });
      handle.addEventListener("mousedown", function (ev) {
        startResize(ev, cols[idx], th, handle, table);
      });
      handle.addEventListener("touchstart", function (ev) {
        startResize(ev, cols[idx], th, handle, table);
      }, { passive: false });
      th.appendChild(handle);
    });

    table.dataset.colResizeInit = "1";
  }

  function attachResetHandlers() {
    document.querySelectorAll("[data-col-resize-reset]").forEach(function (btn) {
      if (btn.dataset.colResetWired === "1") return;
      btn.dataset.colResetWired = "1";
      btn.addEventListener("click", function () {
        var targetSel = btn.getAttribute("data-col-resize-reset");
        var tables = targetSel ? document.querySelectorAll(targetSel) : document.querySelectorAll(".table-wrap table");
        tables.forEach(function (t) {
          clearWidths(t);
          var cg = t.querySelector(":scope > colgroup");
          if (cg) {
            Array.prototype.forEach.call(cg.children, function (c) { c.style.width = ""; });
          }
          t.style.tableLayout = "";
          requestAnimationFrame(function () { snapshotInitialWidths(t); });
        });
      });
    });
  }

  function init() {
    document.querySelectorAll(".table-wrap table").forEach(function (t) {
      try { setupTable(t); } catch (e) { /* swallow */ }
    });
    attachResetHandlers();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    requestAnimationFrame(init);
  }
})();
</script>"""


ROW_CAP_CSS = """
    /* Off-screen rows are skipped during layout/paint — huge win for very
       large tables (master games, VAR events). Only applied at desktop
       widths where the table uses table-layout: fixed; on phones the table
       switches to auto layout and column widths derive from content, so
       content-visibility could mis-size columns. Browser-supported:
       Chrome/Edge/Opera, Safari 18+, Firefox 125+. */
    @media (min-width: 900px) {
      .table-wrap tbody tr.derived-data-row,
      .table-wrap tbody tr.mg-data-row {
        content-visibility: auto;
        contain-intrinsic-size: auto 40px;
      }
    }
    .table-wrap tbody tr.row-over-cap { display: none !important; }

    .row-cap-banner {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 0.6rem 0.9rem;
      padding: 0.55rem 0.85rem;
      margin: 0 0 0.65rem 0.15rem;
      background: #fff8e7;
      border: 1px solid #e6d99c;
      border-radius: 8px;
      font-size: 0.78rem;
      color: #604000;
      line-height: 1.45;
    }
    .row-cap-banner[hidden] { display: none; }
    .row-cap-banner-text { flex: 1 1 240px; }
    .row-cap-banner-text strong { color: #804000; }
    .row-cap-banner-text .row-cap-emoji { display: none; }
    .row-cap-actions {
      display: inline-flex;
      gap: 0.35rem;
      flex-wrap: wrap;
    }
    .row-cap-btn {
      font: inherit;
      cursor: pointer;
      font-size: 0.7rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      padding: 0.3rem 0.6rem;
      border-radius: 6px;
      border: 1px solid #d5a85a;
      background: #fff;
      color: #604000;
      transition: background 0.12s, color 0.12s, border-color 0.12s;
    }
    .row-cap-btn:hover {
      background: #fffaf0;
      border-color: #a07000;
      color: #402800;
    }
    .row-cap-btn.is-active {
      background: #804000;
      color: #fff;
      border-color: #804000;
    }
"""


ROW_CAP_SCRIPT = r"""<script>
(function () {
  if (window.__reportRowCapInit) return;
  window.__reportRowCapInit = true;

  var state = {};

  function fmt(n) { return Number(n).toLocaleString(); }

  function isFilterHidden(tr) {
    return tr.classList.contains("derived-row--hidden") ||
           tr.classList.contains("mg-row--hidden");
  }

  function getDataRows(table) {
    return table.querySelectorAll("tbody tr.derived-data-row, tbody tr.mg-data-row");
  }

  function applyCapToTable(tableId) {
    var table = document.getElementById(tableId);
    if (!table) return;
    var s = state[tableId];
    if (!s) return;
    var rows = getDataRows(table);
    var visIdx = 0;
    var totalVis = 0;
    rows.forEach(function (tr) {
      if (isFilterHidden(tr)) {
        if (tr.classList.contains("row-over-cap")) tr.classList.remove("row-over-cap");
        return;
      }
      totalVis += 1;
      if (s.cap === Infinity || visIdx < s.cap) {
        if (tr.classList.contains("row-over-cap")) tr.classList.remove("row-over-cap");
      } else {
        if (!tr.classList.contains("row-over-cap")) tr.classList.add("row-over-cap");
      }
      visIdx += 1;
    });
    s.lastTotalVis = totalVis;
    s.lastTotalRows = rows.length;
    updateBanner(tableId);
    try {
      table.dispatchEvent(new CustomEvent("report-row-cap-applied", {
        bubbles: false,
        detail: { tableId: tableId, cap: s.cap, totalVisible: totalVis, totalRows: rows.length }
      }));
    } catch (e) {}
  }

  function updateBanner(tableId) {
    var s = state[tableId];
    if (!s) return;
    var banner = document.querySelector('[data-row-cap-banner-for="' + tableId + '"]');
    if (!banner) return;
    var stat = banner.querySelector(".row-cap-banner-text");
    var totalVis = s.lastTotalVis || 0;
    var totalRows = s.lastTotalRows || 0;
    var shown = s.cap === Infinity ? totalVis : Math.min(s.cap, totalVis);
    if (s.cap === Infinity || totalVis <= s.cap) {
      banner.hidden = totalRows <= (s.defaultCap || 0);
      if (stat) {
        stat.innerHTML = "Showing all <strong>" + fmt(totalVis) + "</strong> matching row" +
          (totalVis === 1 ? "" : "s") + " (of " + fmt(totalRows) + " total).";
      }
    } else {
      banner.hidden = false;
      var hidden = totalVis - shown;
      if (stat) {
        stat.innerHTML = "Showing the top <strong>" + fmt(shown) + "</strong> of " +
          "<strong>" + fmt(totalVis) + "</strong> matching rows " +
          "(<strong>" + fmt(hidden) + "</strong> more hidden for performance). " +
          "The cap follows the current sort + filters — change them to see different rows in the top " + fmt(shown) + ".";
      }
    }
    banner.querySelectorAll(".row-cap-btn").forEach(function (btn) {
      var v = btn.getAttribute("data-row-cap-value");
      var match = (v === "all" && s.cap === Infinity) ||
                  (v !== "all" && parseInt(v, 10) === s.cap);
      btn.classList.toggle("is-active", match);
    });
  }

  function setCap(tableId, value) {
    if (!state[tableId]) state[tableId] = { defaultCap: Infinity };
    state[tableId].cap = value === "all" ? Infinity : parseInt(value, 10);
    applyCapToTable(tableId);
    try {
      localStorage.setItem("rowCap:" + tableId,
        state[tableId].cap === Infinity ? "all" : String(state[tableId].cap));
    } catch (e) {}
  }

  window.applyReportRowCap = applyCapToTable;
  window.setReportRowCap = setCap;

  function init() {
    document.querySelectorAll("[data-row-cap-banner-for]").forEach(function (banner) {
      var tableId = banner.getAttribute("data-row-cap-banner-for");
      var defaultCap = parseInt(banner.getAttribute("data-row-cap-default") || "1500", 10);
      var stored = null;
      try { stored = localStorage.getItem("rowCap:" + tableId); } catch (e) {}
      var cap;
      if (stored === "all") cap = Infinity;
      else if (stored && !isNaN(parseInt(stored, 10))) cap = parseInt(stored, 10);
      else cap = defaultCap;
      state[tableId] = { cap: cap, defaultCap: defaultCap };
      banner.addEventListener("click", function (e) {
        var btn = e.target.closest && e.target.closest(".row-cap-btn");
        if (!btn) return;
        setCap(tableId, btn.getAttribute("data-row-cap-value"));
      });
      applyCapToTable(tableId);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
</script>"""


def row_cap_banner_html(table_id: str, total_rows: int, default_cap: int = 1500) -> str:
    """Return the row-cap banner HTML for a given table.

    Always renders the banner. The JS hides it automatically when no cap
    is in effect AND the row count is below the default cap; this keeps
    the markup simple for callers that don't know the row count yet.
    """
    import html as _html

    options = [
        ("1500", "Top 1,500"),
        ("3000", "Top 3,000"),
        ("all", "Show all"),
    ]
    btns = "".join(
        f'<button type="button" class="row-cap-btn" data-row-cap-value="{v}">{_html.escape(label)}</button>'
        for v, label in options
    )
    return (
        f'<div class="row-cap-banner" data-row-cap-banner-for="{_html.escape(table_id, quote=True)}" '
        f'data-row-cap-default="{int(default_cap)}" data-row-cap-total="{int(total_rows)}" hidden>'
        f'<div class="row-cap-banner-text"></div>'
        f'<div class="row-cap-actions">{btns}</div>'
        f"</div>"
    )


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
