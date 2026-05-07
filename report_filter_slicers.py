"""
Shared HTML/CSS/JS for competition + date + migration slicers on HTML reports.

Used by step7_generate_reports and derived report pages.
"""

from __future__ import annotations

import html
import json


def ymd_from_start_time(raw: object) -> str:
    """First 10 chars as YYYY-MM-DD if value looks like ISO datetime; else ''."""
    if raw is None:
        return ""
    s = str(raw).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return ""


def slicer_meta_from_rows(
    rows: list[dict], *, comp_key: str, date_key: str
) -> tuple[list[str], str, str]:
    """Distinct sorted season/competition labels and date bounds from row dicts."""
    comps: set[str] = set()
    dates: list[str] = []
    for r in rows:
        c = str(r.get(comp_key) or "").strip()
        if c:
            comps.add(c)
        d = ymd_from_start_time(r.get(date_key))
        if len(d) == 10:
            dates.append(d)
    names = sorted(comps)
    if not dates:
        return names, "", ""
    return names, min(dates), max(dates)


REPORT_TILE_FILTER_CSS = """
    .stats-section { max-width: 1100px; margin: 1rem auto 0; padding: 0 1rem; }
    .stats-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.55rem; align-items: start; }
    .stat-card { background: #ffffff; border: 1px solid #ddd8d0; border-radius: 10px; padding: 0.75rem 0.65rem; text-align: center;
      transition: border-color 0.2s, box-shadow 0.2s; box-shadow: 0 1px 3px rgba(0,0,0,0.06); }
    .stat-card:hover { border-color: #cc0000; box-shadow: 0 2px 8px rgba(204,0,0,0.12); }
    .stat-card--wide { grid-column: 1 / -1; }
    .stat-card--text-left { text-align: left; }
    .stat-card--filter-tile { padding: 0.75rem 0.85rem; text-align: left; }
    .competition-slicer--tile { margin: 0; padding: 0; max-width: none; background: transparent; border: none; text-align: left; }
    .stat-card--filter-tile .slicer-head { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 0.4rem; margin-bottom: 0.35rem; }
    .stat-card--filter-tile .slicer-title { font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; color: #cc0000; }
    .stat-card--filter-tile .slicer-actions { display: inline-flex; gap: 0.35rem; }
    .stat-card--filter-tile .slicer-btn {
      font: inherit; cursor: pointer; font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
      letter-spacing: 0.06em; padding: 0.3rem 0.55rem; border-radius: 6px; border: 1px solid #ddd8d0;
      background: #faf8f5; color: #333;
    }
    .stat-card--filter-tile .slicer-btn:hover { border-color: #cc0000; background: #fff0f0; color: #990000; }
    .stat-card--filter-tile .slicer-chips { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px 12px; }
    .stat-card--filter-tile .slicer-chip { display: flex; align-items: center; gap: 6px; white-space: nowrap; font-size: 0.72rem; color: #333; cursor: pointer; user-select: none; }
    .stat-card--filter-tile .slicer-chip input { accent-color: #cc0000; width: 0.85rem; height: 0.85rem; }
    .stat-card--filter-tile .slicer-chip .comp-idx { color: #999; min-width: 1.35em; text-align: right; }
    .date-filter-tile { text-align: left; padding: 0.7rem 0.9rem; }
    .date-filter-head { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 0.45rem; margin-bottom: 0.4rem; }
    .date-filter-title { font-size: 0.68rem; font-weight: 700; color: #888; text-transform: uppercase; letter-spacing: 0.08em; }
    .date-filter-presets { display: flex; flex-wrap: wrap; gap: 0.35rem; }
    .migration-filter-presets { display: flex; flex-wrap: wrap; gap: 0.35rem; }
    .date-preset-btn { font: inherit; cursor: pointer; font-size: 0.68rem; font-weight: 600; padding: 0.2rem 0.45rem; border-radius: 6px; border: 1px solid #ddd8d0; background: #faf8f5; color: #333; }
    .date-preset-btn:hover { border-color: #cc0000; color: #cc0000; }
    .date-preset-btn.is-active { border-color: #cc0000; background: #fff0f0; color: #990000; }
    .migration-filter-head { margin-top: 0.4rem; margin-bottom: 0.25rem; }
    .date-filter-custom { display: flex; flex-wrap: wrap; align-items: flex-end; gap: 0.6rem; }
    .date-filter-label { display: flex; flex-direction: column; gap: 0.15rem; font-size: 0.68rem; font-weight: 600; color: #555; }
    .date-filter-label input[type="date"] { font: inherit; padding: 0.22rem 0.35rem; border: 1px solid #ccc; border-radius: 6px; background: #fff; color: #111; }
    .date-filter-hint { display: none; }
    @media (max-width: 900px) {
      .stats-grid { grid-template-columns: repeat(3, 1fr); }
      .stat-card--filter-tile .slicer-chips { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 599px) {
      .stats-grid { grid-template-columns: repeat(2, 1fr); }
      .stat-card--filter-tile .slicer-chips { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      html { font-size: 13px; }
    }
"""


def build_numbered_competition_slicer(
    names: list[str],
    *,
    checkbox_name: str,
    all_btn_id: str,
    none_btn_id: str,
) -> str:
    if not names:
        return ""
    chips = []
    for i, name in enumerate(names, 1):
        ev = html.escape(name, quote=True)
        disp = html.escape(name, quote=False)
        cn = html.escape(checkbox_name, quote=True)
        chips.append(
            f'<label class="slicer-chip"><input type="checkbox" name="{cn}" value="{ev}" checked />'
            f'<span class="comp-idx">{i}.</span><span>{disp}</span></label>'
        )
    chips_html = "\n".join(chips)
    aid = html.escape(all_btn_id, quote=True)
    nid = html.escape(none_btn_id, quote=True)
    return f"""    <div class="stat-card stat-card--wide stat-card--filter-tile stat-card--text-left" role="region" aria-label="Filter by competition">
      <div class="competition-slicer competition-slicer--tile" role="group">
        <div class="slicer-head">
          <span class="slicer-title">Competitions</span>
          <span class="slicer-actions">
            <button type="button" class="slicer-btn" id="{aid}">All</button>
            <button type="button" class="slicer-btn" id="{nid}">None</button>
          </span>
        </div>
        <div class="slicer-chips">{chips_html}</div>
      </div>
    </div>"""


def build_date_migration_tile(
    date_min: str,
    date_max: str,
    *,
    from_id: str,
    to_id: str,
    hint_id: str | None = None,
) -> str:
    if not date_min or not date_max:
        return f"""    <div class="stat-card stat-card--wide stat-card--filter-tile stat-card--text-left date-filter-tile" role="region" aria-label="Kickoff date filter">
      <div class="date-filter-title">Kickoff date range (UTC)</div>
      <p style="margin-top:0.5rem;font-size:0.82rem;color:#888;">No kickoff dates in this report yet.</p>
    </div>"""
    dmin = html.escape(date_min, quote=True)
    dmax = html.escape(date_max, quote=True)
    fid = html.escape(from_id, quote=True)
    tid = html.escape(to_id, quote=True)
    hint_html = ""
    if hint_id:
        hint_html = f'\n      <p class="date-filter-hint" id="{html.escape(hint_id, quote=True)}"></p>'
    return f"""    <div class="stat-card stat-card--wide stat-card--filter-tile stat-card--text-left date-filter-tile" role="region" aria-label="Kickoff date filter">
      <div class="date-filter-head">
        <span class="date-filter-title">Kickoff date range (UTC)</span>
        <span class="date-filter-presets">
          <button type="button" class="date-preset-btn is-active" data-preset="all">All time</button>
          <button type="button" class="date-preset-btn" data-preset="today">Today</button>
          <button type="button" class="date-preset-btn" data-preset="week">This week</button>
          <button type="button" class="date-preset-btn" data-preset="month">This month</button>
        </span>
      </div>
      <div class="date-filter-custom">
        <label class="date-filter-label">From
          <input type="date" id="{fid}" min="1990-01-01" max="2099-12-31" value="{dmin}" />
        </label>
        <label class="date-filter-label">To
          <input type="date" id="{tid}" min="1990-01-01" max="2099-12-31" value="{dmax}" />
        </label>
      </div>
      <div class="date-filter-head migration-filter-head">
        <span class="date-filter-title">Migration period (UTC start time)</span>
        <span class="migration-filter-presets">
          <button type="button" class="date-preset-btn" data-migration-scope="pre">Pre-Migration<br />Before 4/27/26</button>
          <button type="button" class="date-preset-btn" data-migration-scope="post">Post-Migration<br />On or after 4/27/26</button>
        </span>
      </div>{hint_html}
    </div>"""

def build_derived_controls_html(names: list[str], date_min: str, date_max: str) -> str:
    comp = build_numbered_competition_slicer(
        names,
        checkbox_name="drv-comp",
        all_btn_id="drv-slicer-all",
        none_btn_id="drv-slicer-none",
    )
    date_tile = build_date_migration_tile(
        date_min, date_max, from_id="drv-date-from", to_id="drv-date-to"
    )
    inner = "\n".join(x for x in (comp, date_tile) if x)
    if not inner:
        return ""
    return f"""  <div class="stats-section">
    <div class="stats-grid">
{inner}
    </div>
  </div>
"""


def filter_payload_script_tag(payload: dict) -> str:
    j = json.dumps(payload, ensure_ascii=False)
    return f'  <script type="application/json" id="report-filter-data-derived">{j}</script>\n'


DERIVED_TABLE_SCRIPT_WITH_TOP_SLICER = r"""<script>
(function () {
  var filterPayload = (function () {
    var el = document.getElementById("report-filter-data-derived");
    if (!el) return {};
    try { return JSON.parse(el.textContent); } catch (e) { return {}; }
  })();
  var allNames = filterPayload.allSeasonNames || [];
  var dataDateMin = filterPayload.dataDateMin || "";
  var dataDateMax = filterPayload.dataDateMax || "";
  var topEnabled = allNames.length > 0 && dataDateMin && dataDateMax;
  var MIGRATION_CUTOFF = "2026-04-27";
  var MIGRATION_PRE_END = "2026-04-26";

  function getSelectedComps() {
    var boxes = document.querySelectorAll('input[name="drv-comp"]');
    if (!boxes.length) return null;
    return Array.prototype.slice.call(document.querySelectorAll('input[name="drv-comp"]:checked')).map(function (cb) { return cb.value; });
  }
  function compMatchesTr(tr, selected) {
    if (!topEnabled) return true;
    if (selected === null) return true;
    if (!selected.length) return false;
    return selected.indexOf(tr.getAttribute("data-competition") || "") !== -1;
  }
  function pad(n) { return (n < 10 ? "0" : "") + n; }
  function toYMD(d) {
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate());
  }
  function startOfISOWeek(d) {
    var x = new Date(d.getFullYear(), d.getMonth(), d.getDate());
    var day = x.getDay();
    var diff = day === 0 ? -6 : 1 - day;
    x.setDate(x.getDate() + diff);
    return x;
  }
  function endOfISOWeek(d) {
    var s = startOfISOWeek(d);
    var e = new Date(s.getFullYear(), s.getMonth(), s.getDate());
    e.setDate(e.getDate() + 6);
    return e;
  }
  function getDateRange() {
    var lo = dataDateMin;
    var hi = dataDateMax;
    var fromEl = document.getElementById("drv-date-from");
    var toEl = document.getElementById("drv-date-to");
    if (!fromEl || !toEl || !lo || !hi) {
      return { from: lo, to: hi, lo: lo, hi: hi, hasInputs: false };
    }
    var from = fromEl.value || null;
    var to = toEl.value || null;
    if (from && to && from > to) {
      var x = from; from = to; to = x;
      fromEl.value = from;
      toEl.value = to;
    }
    return { from: from, to: to, lo: lo, hi: hi, hasInputs: true };
  }
  function dateMatchesTr(tr, dr) {
    if (!topEnabled) return true;
    var d = tr.getAttribute("data-match-date") || "";
    if (!d || d.length < 10) return false;
    if (!dr.hasInputs || !dr.lo || !dr.hi) return true;
    if (dr.from && d < dr.from) return false;
    if (dr.to && d > dr.to) return false;
    return true;
  }
  function clearPresetActive() {
    document.querySelectorAll(".date-filter-tile [data-preset]").forEach(function (b) { b.classList.remove("is-active"); });
  }
  function clearMigrationActive() {
    document.querySelectorAll(".date-filter-tile [data-migration-scope]").forEach(function (b) { b.classList.remove("is-active"); });
  }
  function setMigrationScope(scope) {
    clearPresetActive();
    var fromEl = document.getElementById("drv-date-from");
    var toEl = document.getElementById("drv-date-to");
    if (!fromEl || !toEl || !dataDateMin || !dataDateMax) return;
    if (scope === "pre") {
      fromEl.value = dataDateMin;
      toEl.value = MIGRATION_PRE_END;
    } else if (scope === "post") {
      fromEl.value = MIGRATION_CUTOFF;
      toEl.value = dataDateMax;
    }
    document.querySelectorAll("[data-migration-scope]").forEach(function (btn) {
      btn.classList.toggle("is-active", btn.getAttribute("data-migration-scope") === scope);
    });
  }
  var tables = [];
  function applyAllDerivedFilters() {
    tables.forEach(function (t) {
      if (typeof t._derivedApplyFilters === "function") t._derivedApplyFilters();
    });
  }
  function wireTopSlicers() {
    if (!topEnabled) return;
    var fromEl = document.getElementById("drv-date-from");
    var toEl = document.getElementById("drv-date-to");
    if (fromEl) fromEl.addEventListener("change", function () { clearPresetActive(); clearMigrationActive(); applyAllDerivedFilters(); });
    if (toEl) toEl.addEventListener("change", function () { clearPresetActive(); clearMigrationActive(); applyAllDerivedFilters(); });
    document.querySelectorAll(".date-filter-tile [data-preset]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var preset = btn.getAttribute("data-preset");
        clearPresetActive();
        clearMigrationActive();
        btn.classList.add("is-active");
        var today = new Date();
        var lo = dataDateMin;
        var hi = dataDateMax;
        if (preset === "all") {
          fromEl.value = lo;
          toEl.value = hi;
        } else if (preset === "today") {
          var td = toYMD(today);
          fromEl.value = td;
          toEl.value = td;
        } else if (preset === "week") {
          fromEl.value = toYMD(startOfISOWeek(today));
          toEl.value = toYMD(endOfISOWeek(today));
        } else if (preset === "month") {
          var y = today.getFullYear();
          var m = today.getMonth();
          fromEl.value = toYMD(new Date(y, m, 1));
          toEl.value = toYMD(new Date(y, m + 1, 0));
        }
        if (fromEl.value && toEl.value && fromEl.value > toEl.value) {
          var x = fromEl.value; fromEl.value = toEl.value; toEl.value = x;
        }
        applyAllDerivedFilters();
      });
    });
    document.querySelectorAll("[data-migration-scope]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var scope = btn.getAttribute("data-migration-scope") || "pre";
        setMigrationScope(scope);
        applyAllDerivedFilters();
      });
    });
    var allBtn = document.getElementById("drv-slicer-all");
    var noneBtn = document.getElementById("drv-slicer-none");
    if (allBtn) {
      allBtn.addEventListener("click", function () {
        document.querySelectorAll('input[name="drv-comp"]').forEach(function (cb) { cb.checked = true; });
        applyAllDerivedFilters();
      });
    }
    if (noneBtn) {
      noneBtn.addEventListener("click", function () {
        document.querySelectorAll('input[name="drv-comp"]').forEach(function (cb) { cb.checked = false; });
        applyAllDerivedFilters();
      });
    }
    document.querySelectorAll('input[name="drv-comp"]').forEach(function (cb) {
      cb.addEventListener("change", applyAllDerivedFilters);
    });
  }

  document.querySelectorAll("table.sortable-derived").forEach(function (table) {
    var tbody = table.querySelector("tbody");
    if (!tbody) return;
    table._excelColSelections = table._excelColSelections || {};
    var headers = table.querySelectorAll("thead tr:first-child th[data-col]");
    var sortCol = null;
    var sortAsc = true;

    function cellVal(tr, idx) {
      var c = tr.children[idx];
      if (!c) return "";
      var dv = c.getAttribute("data-val");
      if (dv !== null && dv !== "") return dv;
      return (c.textContent || "").trim();
    }

    function rowPassesColSelections(tr) {
      if (tr.querySelector("td[colspan]")) return true;
      var sel = table._excelColSelections;
      for (var k in sel) {
        if (!Object.prototype.hasOwnProperty.call(sel, k)) continue;
        var sset = sel[k];
        if (sset == null) continue;
        var col = parseInt(k, 10);
        var v = cellVal(tr, col) || "";
        if (!sset.has(v)) return false;
      }
      return true;
    }

    function rowPassesTopFilters(tr) {
      if (tr.querySelector("td[colspan]")) return true;
      if (!topEnabled) return true;
      var selected = getSelectedComps();
      var dr = getDateRange();
      return compMatchesTr(tr, selected) && dateMatchesTr(tr, dr);
    }

    table._derivedApplyFilters = function () {
      tbody.querySelectorAll("tr").forEach(function (tr) {
        if (tr.querySelector("td[colspan]")) return;
        if (rowPassesColSelections(tr) && rowPassesTopFilters(tr)) tr.classList.remove("derived-row--hidden");
        else tr.classList.add("derived-row--hidden");
      });
      updateFilterButtons();
    };

    function distinctForColumn(col, excludeCol) {
      var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr")).filter(function (tr) {
        return !tr.querySelector("td[colspan]");
      });
      function passesOther(tr) {
        var sel = table._excelColSelections;
        for (var k in sel) {
          if (!Object.prototype.hasOwnProperty.call(sel, k)) continue;
          if (parseInt(k, 10) === excludeCol) continue;
          var sset = sel[k];
          if (sset == null) continue;
          var v = cellVal(tr, parseInt(k, 10)) || "";
          if (!sset.has(v)) return false;
        }
        return true;
      }
      var s = new Set();
      rows.forEach(function (tr) {
        if (!passesOther(tr)) return;
        if (!rowPassesTopFilters(tr)) return;
        s.add(cellVal(tr, col) || "");
      });
      return Array.from(s).sort(function (a, b) {
        return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
      });
    }

    function updateFilterButtons() {
      table.querySelectorAll(".excel-filter-btn").forEach(function (btn) {
        var c = String(btn.getAttribute("data-col"));
        var sel = table._excelColSelections[c];
        if (sel == null) {
          btn.textContent = "Values…";
          btn.classList.remove("is-filtered");
        } else if (sel.size === 0) {
          btn.textContent = "None";
          btn.classList.add("is-filtered");
        } else {
          btn.textContent = sel.size + " picked";
          btn.classList.add("is-filtered");
        }
      });
    }

    function sortTable(col, asc) {
      var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr")).filter(function (tr) {
        return !tr.querySelector("td[colspan]");
      });
      var vis = rows.filter(function (tr) { return !tr.classList.contains("derived-row--hidden"); });
      var hid = rows.filter(function (tr) { return tr.classList.contains("derived-row--hidden"); });
      vis.sort(function (a, b) {
        var aVal = cellVal(a, col);
        var bVal = cellVal(b, col);
        var as = String(aVal);
        var bs = String(bVal);
        var isoLike = /^\d{4}-\d{2}-\d{2}/;
        var cmp;
        if (isoLike.test(as) && isoLike.test(bs)) {
          var ad = Date.parse(as);
          var bd = Date.parse(bs);
          if (!isNaN(ad) && !isNaN(bd)) cmp = ad - bd;
          else cmp = as.localeCompare(bs);
        } else {
          var aNum = parseFloat(aVal);
          var bNum = parseFloat(bVal);
          if (!isNaN(aNum) && !isNaN(bNum) && aVal !== "" && bVal !== "") cmp = aNum - bNum;
          else cmp = as.localeCompare(bs, undefined, { numeric: true, sensitivity: "base" });
        }
        return asc ? cmp : -cmp;
      });
      vis.concat(hid).forEach(function (r) { tbody.appendChild(r); });
    }

    function applyCurrentSort() {
      if (sortCol !== null) sortTable(sortCol, sortAsc);
      else if (table.id === "table-var-events" || table.id === "table-var-unpaired") sortTable(2, false);
      else if (table.id === "table-recordings-library") sortTable(0, false);
    }

    headers.forEach(function (th) {
      th.addEventListener("click", function () {
        var col = parseInt(th.getAttribute("data-col"), 10);
        if (sortCol === col) sortAsc = !sortAsc;
        else {
          sortCol = col;
          sortAsc = th.getAttribute("data-sort-first") === "desc" ? false : true;
        }
        headers.forEach(function (h) { h.classList.remove("sorted-asc", "sorted-desc"); });
        th.classList.add(sortAsc ? "sorted-asc" : "sorted-desc");
        sortTable(col, sortAsc);
      });
    });

    table.querySelectorAll(".excel-filter-btn").forEach(function (btn) {
      btn.addEventListener("click", function (ev) {
        ev.stopPropagation();
        var col = parseInt(btn.getAttribute("data-col"), 10);
        var dist = distinctForColumn(col, col);
        var key = String(col);
        var cur = table._excelColSelections[key];
        var initial = cur == null ? null : new Set(cur);
        if (!window.ReportExcelFilter) return;
        window.ReportExcelFilter.open({
          anchor: btn,
          title: "Filter column",
          distinctSnapshot: dist,
          selectedSet: initial,
          onApply: function (set) {
            if (set == null) delete table._excelColSelections[key];
            else if (set.size === 0) table._excelColSelections[key] = new Set();
            else table._excelColSelections[key] = set;
            table._derivedApplyFilters();
            applyCurrentSort();
          }
        });
      });
    });

    function applyDefaultSort() {
      if (table.id === "table-recordings-library") {
        headers.forEach(function (h) { h.classList.remove("sorted-asc", "sorted-desc"); });
        var th0 = table.querySelector('thead tr:first-child th[data-col="0"]');
        if (th0) th0.classList.add("sorted-desc");
        sortTable(0, false);
        sortCol = null;
      } else if (table.id === "table-var-events" || table.id === "table-var-unpaired") {
        headers.forEach(function (h) { h.classList.remove("sorted-asc", "sorted-desc"); });
        var th2 = table.querySelector('thead tr:first-child th[data-col="2"]');
        if (th2) th2.classList.add("sorted-desc");
        sortTable(2, false);
        sortCol = null;
      }
    }

    table._derivedApplyFilters();
    applyDefaultSort();
    tables.push(table);
  });

  wireTopSlicers();
  applyAllDerivedFilters();
})();
</script>"""


def recordings_json_payload_from_overlay_rows(data_rows: list[dict]) -> dict:
    names, dmin, dmax = slicer_meta_from_rows(data_rows, comp_key="league", date_key="sched")
    return {"allSeasonNames": names, "dataDateMin": dmin, "dataDateMax": dmax}
