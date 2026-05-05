"""
STEP 6 — Generate HTML reports.

Writes derived static pages:

- report_own_goals.html — own goals (CSV or Supabase)
- report_penalty_shootouts.html, report_var_events.html, report_var_unpaired.html,
  report_recordings_library.html — from Supabase when USE_SUPABASE is set;
  otherwise stub HTML for those four files. When
  `soccer record replay list of sr sport event ids.json` is present, the recordings
  page is then overlaid by `gen_report_recordings_library_html.py` (one column per
  `apis[].description`, compact layout).
- report_master_games.html — full Supabase \"All Games\" schedule (master_games_report.py)

Without Supabase, own goals are read from data/own_goals.csv.
"""

import csv
import html
import json
import os
from datetime import datetime, timezone

from config import (
    OWN_GOALS_CSV,
    REPORT_BLURB_OWN_GOALS,
    REPORT_BLURB_OWN_GOALS_NOTE,
    REPORT_BLURB_PENALTY_SHOOTOUTS,
    REPORT_BLURB_RECORDINGS_LIBRARY,
    REPORT_BLURB_VAR_EVENTS,
    REPORT_BLURB_VAR_UNPAIRED,
    REPORT_HTML,
    REPORT_HTML_LEGACY_REDIRECT,
    REPORT_HTML_PENALTY_SHOOTOUTS,
    REPORT_HTML_VAR_EVENTS,
    REPORT_HTML_VAR_UNPAIRED,
    REPORT_HTML_RECORDINGS_LIBRARY,
    SEASON_NAME,
    SEASON_LABEL,
    TIMELINES_DIR,
    USE_SUPABASE,
)
from report_navigation import NAV_CSS, navigation_html

ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")
_RECORDINGS_EXPORT_JSON = os.path.join(
    os.path.dirname(__file__), "soccer record replay list of sr sport event ids.json"
)

# Timeline commentary from Sportradar is only retained ~14 days after kickoff; after a DB reload,
# meaningful commentary text exists only from this match_date onward (YYYY-MM-DD).
COMMENTARY_AVAILABLE_FROM_DATE = "2026-04-20"


def row_in_commentary_coverage_window(row: dict) -> bool:
    md = (row.get("match_date") or "")[:10]
    return len(md) == 10 and md[4] == "-" and md[7] == "-" and md >= COMMENTARY_AVAILABLE_FROM_DATE


def count_completed_matches() -> int:
    """Number of cached timeline files = completed matches reviewed."""
    if not os.path.isdir(TIMELINES_DIR):
        return 0
    return sum(1 for f in os.listdir(TIMELINES_DIR) if f.endswith(".json"))


def load_own_goals(csv_path: str) -> list[dict]:
    if not os.path.exists(csv_path):
        return []
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_svg(filename: str) -> str:
    path = os.path.join(ASSETS_DIR, filename)
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as f:
        return f.read()


def format_minute(minute: str, stoppage: str) -> str:
    if not minute:
        return "—"
    if stoppage:
        return f"{minute}+{stoppage}'"
    return f"{minute}'"


def format_score(home: str, away: str) -> str:
    if home == "" or away == "":
        return "—"
    return f"{home}–{away}"


def build_competition_slicer(names: list[str]) -> str:
    """Full-width KPI-grid tile: multi-select by season_name (matches Competition column)."""
    if not names:
        return ""
    chips = []
    for name in names:
        ev = html.escape(name, quote=True)
        disp = html.escape(name, quote=False)
        chips.append(
            f'<label class="slicer-chip"><input type="checkbox" name="og-comp" value="{ev}" checked />'
            f"<span>{disp}</span></label>"
        )
    chips_html = "\n".join(chips)
    return f"""    <div class="stat-card stat-card--wide stat-card--filter-tile stat-card--text-left" role="region" aria-label="Filter by competition">
      <div class="competition-slicer competition-slicer--tile" role="group">
        <div class="slicer-head">
          <span class="slicer-title">Competitions</span>
          <span class="slicer-actions">
            <button type="button" class="slicer-btn" id="slicer-all">All</button>
            <button type="button" class="slicer-btn" id="slicer-none">None</button>
          </span>
        </div>
        <div class="slicer-chips">{chips_html}</div>
      </div>
    </div>"""


def date_bounds_from_rows(rows: list[dict]) -> tuple[str, str]:
    """Earliest and latest match_date (YYYY-MM-DD) across own-goal rows; ("","") if none."""
    dates: list[str] = []
    for r in rows:
        d = (r.get("match_date") or "")[:10]
        if len(d) == 10 and d[4] == "-" and d[7] == "-":
            dates.append(d)
    if not dates:
        return "", ""
    return min(dates), max(dates)


def build_date_filter_tile(date_min: str, date_max: str) -> str:
    """Card-style match-date filter (client-side); presets + custom from/to."""
    if not date_min or not date_max:
        return """    <div class="stat-card stat-card--wide stat-card--filter-tile stat-card--text-left date-filter-tile" role="region" aria-label="Match date filter">
      <div class="date-filter-title">Match date range</div>
      <p class="date-filter-muted">No match dates in this report yet.</p>
    </div>"""
    dmin = html.escape(date_min, quote=True)
    dmax = html.escape(date_max, quote=True)
    # Wide min/max so presets use the viewer's real calendar (today / this week / this month)
    # even when that range extends past the last match in the export.
    return f"""    <div class="stat-card stat-card--wide stat-card--filter-tile stat-card--text-left date-filter-tile" role="region" aria-label="Match date filter">
      <div class="date-filter-head">
        <span class="date-filter-title">Match date range</span>
        <span class="date-filter-presets">
          <button type="button" class="date-preset-btn is-active" data-preset="all">All time</button>
          <button type="button" class="date-preset-btn" data-preset="today">Today</button>
          <button type="button" class="date-preset-btn" data-preset="week">This week</button>
          <button type="button" class="date-preset-btn" data-preset="month">This month</button>
        </span>
      </div>
      <div class="date-filter-custom">
        <label class="date-filter-label">From
          <input type="date" id="og-date-from" min="1990-01-01" max="2099-12-31" value="{dmin}" />
        </label>
        <label class="date-filter-label">To
          <input type="date" id="og-date-to" min="1990-01-01" max="2099-12-31" value="{dmax}" />
        </label>
      </div>
      <p class="date-filter-hint" id="og-date-hint"></p>
    </div>"""


def _og_recorded_attrs(raw) -> tuple[str, str, str]:
    """Sort/filter value, cell text, CSS class for games.recorded (true/false/unknown)."""
    if raw is True:
        return "true", "true", "recorded-true"
    if raw is False:
        return "false", "false", "recorded-false"
    return "", "—", "recorded-unknown"


def build_table_rows(rows: list[dict]) -> str:
    if not rows:
        return '<tr><td colspan="15" style="text-align:center;padding:2rem;color:#666;">No own goals found yet.</td></tr>'

    html_rows = []
    for i, r in enumerate(rows, 1):
        minute_display = format_minute(r["minute"], r["stoppage_time"])
        score_at_og = format_score(r["home_score_after"], r["away_score_after"])
        final_score = format_score(r["final_home_score"], r["final_away_score"])
        match_title = f"{r['home_team']} / {r['away_team']}"
        commentary = r.get("commentary", "")

        # Flip "Last, First" → "First Last"
        raw_name = r["og_player"]
        if ", " in raw_name:
            last, first = raw_name.split(", ", 1)
            display_name = f"{first} {last}"
        else:
            display_name = raw_name

        # Data attributes drive the JS sort (sortable by raw value, not display HTML)
        minute_val = r["minute"] if r["minute"] else "0"

        og_mentioned = "own goal" in commentary.lower()
        og_class = "og-yes" if og_mentioned else "og-no"
        og_label = "Yes" if og_mentioned else "No"
        og_sort = "1" if og_mentioned else "0"

        rec_val, rec_txt, rec_cls = _og_recorded_attrs(r.get("recorded"))

        comp = (r.get("season_name") or "").strip()
        comp_attr = html.escape(comp, quote=True)
        ev_attr = html.escape(str(r.get("sport_event_id", "")), quote=True)
        pl_attr = html.escape(str(r.get("og_player", "")), quote=True)
        tm_attr = html.escape(str(r.get("og_player_team", "")), quote=True)
        match_date_raw = (r.get("match_date") or "")[:10]
        date_attr = html.escape(match_date_raw if len(match_date_raw) == 10 else "", quote=True)
        date_display = match_date_raw if len(match_date_raw) == 10 else "—"
        date_sort_val = match_date_raw if len(match_date_raw) == 10 else ""
        html_rows.append(f"""
        <tr class="og-data-row" data-competition="{comp_attr}" data-match-date="{date_attr}" data-event-id="{ev_attr}" data-og-player="{pl_attr}" data-og-team="{tm_attr}">
          <td class="num" data-val="{i}" data-label="#">{i}</td>
          <td data-val="{r.get('season_name','')}" data-label="Competition">
            <div class="match-name">{r.get('season_name','—')}</div>
            <div class="meta">{r.get('competition_id','')}</div>
          </td>
          <td data-val="{html.escape(match_title, quote=True)}" data-label="Title">
            <div class="match-name">{html.escape(match_title, quote=False)}</div>
          </td>
          <td data-val="{html.escape(date_sort_val, quote=True)}" data-label="Date">{html.escape(date_display, quote=False)}</td>
          <td class="id-cell match-id-cell" data-val="{r['sport_event_id']}" data-label="Match ID"><code title="{r['sport_event_id']}">{r['sport_event_id']}</code></td>
          <td class="center {rec_cls}" data-val="{html.escape(rec_val, quote=True)}" data-label="Recorded">{html.escape(rec_txt, quote=False)}</td>
          <td class="id-cell" data-val="{html.escape(str(r.get('recording_id', '') or ''), quote=True)}" data-label="Recording ID"><code title="{html.escape(str(r.get('recording_id', '') or ''), quote=True)}">{html.escape(str(r.get('recording_id', '') or ''), quote=False)}</code></td>
          <td data-val="{r['og_player']}" data-label="Scorer">
            <div class="player-name">{display_name}</div>
            <div class="meta">{r['og_player_team']}</div>
          </td>
          <td class="id-cell" data-val="{r['og_player_id']}" data-label="Player ID"><code title="{r['og_player_id']}">{r['og_player_id']}</code></td>
          <td class="minute" data-val="{minute_val}" data-label="Min">{minute_display}</td>
          <td data-val="{r['benefiting_team']}" data-label="Benefiting"><div class="team-badge">{r['benefiting_team']}</div></td>
          <td class="score" data-val="{r.get('home_score_after','0')}" data-label="At OG">{score_at_og}</td>
          <td class="score final" data-val="{r['final_home_score']}" data-label="Final">{final_score}</td>
          <td class="{og_class}" data-val="{og_sort}" data-label="Mentions OG?">{og_label}</td>
          <td class="commentary-cell" data-val="{commentary}" data-label="Commentary"><span class="commentary">{commentary}</span></td>
        </tr>""")

    return "\n".join(html_rows)


def _inline_report_script() -> str:
    """Client-side sort, competition filter, and KPI recompute (no f-string braces)."""
    return r"""<script>
(function () {
  const tbody = document.getElementById('og-tbody');
  if (!tbody) return;
  var ogTable = document.getElementById('og-table');
  if (ogTable) ogTable._excelColSelections = ogTable._excelColSelections || {};
  const filterPayload = (function () {
    const el = document.getElementById('report-filter-data');
    if (!el) return {};
    try { return JSON.parse(el.textContent); } catch (e) { return {}; }
  })();

  const allNames = filterPayload.allSeasonNames || [];
  const pb = filterPayload.pipelineBySeason || {};
  const pg = filterPayload.pipelineGlobal || { matches: 0 };
  const dataDateMin = filterPayload.dataDateMin || '';
  const dataDateMax = filterPayload.dataDateMax || '';
  const commentaryAvailableFrom = filterPayload.commentaryAvailableFrom || '';

  const headers = document.querySelectorAll('thead tr:first-child th[data-col]');
  let sortCol = null;
  let sortAsc = true;

  function cellVal(tr, idx) {
    const c = tr.children[idx];
    return c ? (c.getAttribute('data-val') || '') : '';
  }

  function sortTable(colIndex, asc) {
    const all = Array.from(tbody.querySelectorAll('tr.og-data-row'));
    const visible = all.filter(function (tr) { return !tr.classList.contains('og-row--hidden'); });
    const hidden = all.filter(function (tr) { return tr.classList.contains('og-row--hidden'); });
    visible.sort(function (a, b) {
      const aVal = cellVal(a, colIndex);
      const bVal = cellVal(b, colIndex);
      let cmp;
      const isoDate = /^\\d{4}-\\d{2}-\\d{2}$/;
      if (isoDate.test(aVal) && isoDate.test(bVal)) {
        cmp = aVal.localeCompare(bVal);
      } else {
        const aNum = parseFloat(aVal);
        const bNum = parseFloat(bVal);
        if (!isNaN(aNum) && !isNaN(bNum) && aVal !== '' && bVal !== '') {
          cmp = aNum - bNum;
        } else {
          cmp = aVal.localeCompare(bVal, undefined, { numeric: true, sensitivity: 'base' });
        }
      }
      return asc ? cmp : -cmp;
    });
    visible.concat(hidden).forEach(function (r) { tbody.appendChild(r); });
    all.forEach(function (r) { r.style.background = ''; });
  }

  headers.forEach(function (th) {
    th.addEventListener('click', function () {
      const col = parseInt(th.getAttribute('data-col'), 10);
      if (sortCol === col) sortAsc = !sortAsc;
      else { sortCol = col; sortAsc = true; }
      headers.forEach(function (h) { h.classList.remove('sorted-asc', 'sorted-desc'); });
      th.classList.add(sortAsc ? 'sorted-asc' : 'sorted-desc');
      sortTable(col, sortAsc);
      renumberRows();
    });
  });

  function renumberRows() {
    let n = 1;
    tbody.querySelectorAll('tr.og-data-row').forEach(function (tr) {
      if (tr.classList.contains('og-row--hidden')) return;
      const cell = tr.querySelector('td.num');
      if (cell) {
        cell.textContent = String(n);
        cell.setAttribute('data-val', String(n));
        n += 1;
      }
    });
  }

  function flipName(raw) {
    const i = raw.indexOf(', ');
    if (i === -1) return raw;
    return raw.slice(i + 2) + ' ' + raw.slice(0, i);
  }

  function fmtInt(n) {
    return n.toLocaleString('en-US');
  }

  function pipelineStats(selected) {
    const keys = Object.keys(pb);
    if (keys.length === 0) {
      return { matches: pg.matches || 0 };
    }
    if (selected.length === 0) return { matches: 0 };
    let m = 0;
    selected.forEach(function (sn) {
      const row = pb[sn];
      if (row) {
        m += row.matches_reviewed || 0;
      }
    });
    return { matches: m };
  }

  function rowEligibleForCommentaryKpis(tr) {
    if (!commentaryAvailableFrom) return true;
    const md = tr.getAttribute('data-match-date') || '';
    return md.length === 10 && md >= commentaryAvailableFrom;
  }

  function aggVisibleStats() {
    const visible = Array.prototype.slice.call(tbody.querySelectorAll('tr.og-data-row')).filter(function (tr) {
      return !tr.classList.contains('og-row--hidden');
    });
    const total = visible.length;
    const eventsSeen = {};
    const pc = {};
    const tc = {};
    let corr = 0;
    let commentaryEligible = 0;
    visible.forEach(function (tr) {
      const evId = tr.getAttribute('data-event-id') || '';
      if (evId) eventsSeen[evId] = true;
      const pl = tr.getAttribute('data-og-player') || '';
      const tm = tr.getAttribute('data-og-team') || '';
      if (pl) pc[pl] = (pc[pl] || 0) + 1;
      if (tm) tc[tm] = (tc[tm] || 0) + 1;
      if (rowEligibleForCommentaryKpis(tr)) {
        commentaryEligible += 1;
        const rowSays = tr.querySelector('td.og-yes, td.og-no');
        if (rowSays && rowSays.classList.contains('og-yes')) corr += 1;
      }
    });
    const games = Object.keys(eventsSeen).length;
    let maxP = 0;
    let namesP = [];
    Object.keys(pc).forEach(function (k) {
      if (pc[k] > maxP) { maxP = pc[k]; namesP = [flipName(k)]; }
      else if (pc[k] === maxP && maxP > 0) namesP.push(flipName(k));
    });
    namesP.sort();
    let maxT = 0;
    let namesT = [];
    Object.keys(tc).forEach(function (k) {
      if (tc[k] > maxT) { maxT = tc[k]; namesT = [k]; }
      else if (tc[k] === maxT && maxT > 0) namesT.push(k);
    });
    namesT.sort();
    return {
      total: total,
      games: games,
      maxP: maxP,
      namesP: namesP,
      maxT: maxT,
      namesT: namesT,
      corr: corr,
      incorr: commentaryEligible - corr
    };
  }

  function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function getSelectedComps() {
    const boxes = document.querySelectorAll('input[name="og-comp"]');
    if (!boxes.length) return null;
    return Array.prototype.slice.call(document.querySelectorAll('input[name="og-comp"]:checked')).map(function (cb) { return cb.value; });
  }

  /** True when every competition slicer box is checked — same scope as terminal / pipelineGlobal. */
  function allCompetitionChipsSelected() {
    const boxes = document.querySelectorAll('input[name="og-comp"]');
    if (!boxes.length) return true;
    return document.querySelectorAll('input[name="og-comp"]:checked').length === boxes.length;
  }

  function compMatches(tr, selected) {
    if (selected === null) return true;
    if (!selected.length) return false;
    return selected.indexOf(tr.getAttribute('data-competition') || '') !== -1;
  }

  function pad(n) { return (n < 10 ? '0' : '') + n; }
  function toYMD(d) {
    return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate());
  }
  function startOfISOWeek(d) {
    const x = new Date(d.getFullYear(), d.getMonth(), d.getDate());
    const day = x.getDay();
    const diff = day === 0 ? -6 : 1 - day;
    x.setDate(x.getDate() + diff);
    return x;
  }
  function endOfISOWeek(d) {
    const s = startOfISOWeek(d);
    const e = new Date(s.getFullYear(), s.getMonth(), s.getDate());
    e.setDate(e.getDate() + 6);
    return e;
  }
  /** Ensure from <= to only; do not clamp to dataset (presets use real calendar). */
  function normalizeFromTo(fromEl, toEl, fallbackFrom, fallbackTo) {
    var from = fromEl.value || fallbackFrom;
    var to = toEl.value || fallbackTo;
    if (from > to) {
      var x = from;
      from = to;
      to = x;
      fromEl.value = from;
      toEl.value = to;
    }
    return { from: from, to: to };
  }

  function getDateRange() {
    const lo = dataDateMin;
    const hi = dataDateMax;
    const fromEl = document.getElementById('og-date-from');
    const toEl = document.getElementById('og-date-to');
    if (!fromEl || !toEl || !lo || !hi) {
      return { from: lo, to: hi, lo: lo, hi: hi, hasInputs: false };
    }
    var c = normalizeFromTo(fromEl, toEl, lo, hi);
    return { from: c.from, to: c.to, lo: lo, hi: hi, hasInputs: true };
  }

  function dateMatches(tr, dr) {
    var d = tr.getAttribute('data-match-date') || '';
    if (!d || d.length < 10) return false;
    if (!dr.hasInputs || !dr.lo || !dr.hi) return true;
    return d >= dr.from && d <= dr.to;
  }

  function updateDateHint(dr) {
    var el = document.getElementById('og-date-hint');
    if (!el) return;
    if (!dr.hasInputs || !dr.lo || !dr.hi) { el.textContent = ''; return; }
    if (dr.from === dr.lo && dr.to === dr.hi) {
      el.textContent = 'Showing all match dates in this report (' + dr.lo + ' to ' + dr.hi + ').';
    } else {
      el.textContent = 'Showing goals from matches on ' + dr.from + ' through ' + dr.to + ' (inclusive).';
    }
  }

  function clearPresetActive() {
    document.querySelectorAll('.date-preset-btn').forEach(function (b) { b.classList.remove('is-active'); });
  }

  function wireDateFilter() {
    var fromEl = document.getElementById('og-date-from');
    var toEl = document.getElementById('og-date-to');
    if (!fromEl || !toEl || !dataDateMin || !dataDateMax) return;
    fromEl.addEventListener('change', function () { clearPresetActive(); applyFilter(); });
    toEl.addEventListener('change', function () { clearPresetActive(); applyFilter(); });
    document.querySelectorAll('.date-preset-btn').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var preset = btn.getAttribute('data-preset');
        var lo = dataDateMin;
        var hi = dataDateMax;
        clearPresetActive();
        btn.classList.add('is-active');
        var today = new Date();
        if (preset === 'all') {
          fromEl.value = lo;
          toEl.value = hi;
        } else if (preset === 'today') {
          var td = toYMD(today);
          fromEl.value = td;
          toEl.value = td;
        } else if (preset === 'week') {
          fromEl.value = toYMD(startOfISOWeek(today));
          toEl.value = toYMD(endOfISOWeek(today));
        } else if (preset === 'month') {
          var y = today.getFullYear();
          var m = today.getMonth();
          var first = new Date(y, m, 1);
          var last = new Date(y, m + 1, 0);
          fromEl.value = toYMD(first);
          toEl.value = toYMD(last);
        }
        normalizeFromTo(fromEl, toEl, lo, hi);
        applyFilter();
      });
    });
  }

  function colFiltersMatch(tr) {
    if (!ogTable || !ogTable._excelColSelections) return true;
    var selMap = ogTable._excelColSelections;
    for (var k in selMap) {
      if (!Object.prototype.hasOwnProperty.call(selMap, k)) continue;
      var sset = selMap[k];
      if (sset == null) continue;
      var col = parseInt(k, 10);
      var v = cellVal(tr, col) || '';
      if (!sset.has(v)) return false;
    }
    return true;
  }

  function distinctOgColumn(activeCol) {
    var selected = getSelectedComps();
    var dr = getDateRange();
    var selMap = ogTable && ogTable._excelColSelections ? ogTable._excelColSelections : {};
    var s = new Set();
    tbody.querySelectorAll('tr.og-data-row').forEach(function (tr) {
      if (!compMatches(tr, selected) || !dateMatches(tr, dr)) return;
      for (var k in selMap) {
        if (!Object.prototype.hasOwnProperty.call(selMap, k)) continue;
        if (parseInt(k, 10) === activeCol) continue;
        var sset = selMap[k];
        if (sset == null) continue;
        var v = cellVal(tr, parseInt(k, 10)) || '';
        if (!sset.has(v)) return;
      }
      s.add(cellVal(tr, activeCol) || '');
    });
    return Array.from(s).sort(function (a, b) {
      return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' });
    });
  }

  function updateOgFilterButtons() {
    if (!ogTable) return;
    document.querySelectorAll('#og-table .excel-filter-btn').forEach(function (btn) {
      var c = String(btn.getAttribute('data-col'));
      var sel = ogTable._excelColSelections[c];
      if (sel == null) {
        btn.textContent = 'Values…';
        btn.classList.remove('is-filtered');
      } else if (sel.size === 0) {
        btn.textContent = 'None';
        btn.classList.add('is-filtered');
      } else {
        btn.textContent = sel.size + ' picked';
        btn.classList.add('is-filtered');
      }
    });
  }

  function applyFilter() {
    var selected = getSelectedComps();
    var dr = getDateRange();
    tbody.querySelectorAll('tr.og-data-row').forEach(function (tr) {
      if (compMatches(tr, selected) && dateMatches(tr, dr) && colFiltersMatch(tr)) tr.classList.remove('og-row--hidden');
      else tr.classList.add('og-row--hidden');
    });
    renumberRows();

    var pipeKeys = selected === null ? (allNames.length ? allNames : Object.keys(pb)) : selected;
    // Summing pipelineBySeason misses timelines that do not map to a configured season / closed game
    // (or empty JSON rows still counted globally). When every comp chip is on, match the printed totals.
    var st = allCompetitionChipsSelected()
      ? { matches: pg.matches || 0 }
      : pipelineStats(pipeKeys);
    setText('kpi-matches', fmtInt(st.matches));

    var ag = aggVisibleStats();
    setText('kpi-total-og', String(ag.total));
    setText('kpi-games', String(ag.games));
    setText('kpi-most-player', String(ag.maxP || 0));
    setText('kpi-most-team', String(ag.maxT || 0));
    setText('kpi-most-player-names', ag.namesP.length ? ag.namesP.join(' & ') : '—');
    setText('kpi-most-team-names', ag.namesT.length ? ag.namesT.join(' & ') : '—');
    setText('kpi-commentary-yes', String(ag.corr));
    setText('kpi-commentary-no', String(ag.incorr));

    setText('subtitle-og-total', String(ag.total));
    var pl = document.getElementById('subtitle-og-plural');
    if (pl) pl.textContent = ag.total === 1 ? '' : 's';

    var selArr = selected === null ? allNames : selected;
    var scopeEl = document.getElementById('subtitle-filter-scope');
    if (scopeEl) {
      if (!selArr.length) scopeEl.textContent = '— no competitions selected';
      else if (selArr.length === allNames.length) scopeEl.textContent = '· all competitions';
      else scopeEl.textContent = '· filtered to ' + selArr.length + ' competition' + (selArr.length === 1 ? '' : 's');
    }
    var ttitle = document.getElementById('table-filter-title');
    if (ttitle) {
      if (!selArr.length) ttitle.textContent = 'All Own Goals — (none selected)';
      else if (selArr.length === allNames.length) ttitle.textContent = 'All Own Goals';
      else ttitle.textContent = 'All Own Goals — ' + selArr.join(', ');
    }
    updateDateHint(dr);
  }

  const allBtn = document.getElementById('slicer-all');
  const noneBtn = document.getElementById('slicer-none');
  if (allBtn) {
    allBtn.addEventListener('click', function () {
      document.querySelectorAll('input[name="og-comp"]').forEach(function (cb) { cb.checked = true; });
      applyFilter();
    });
  }
  if (noneBtn) {
    noneBtn.addEventListener('click', function () {
      document.querySelectorAll('input[name="og-comp"]').forEach(function (cb) { cb.checked = false; });
      applyFilter();
    });
  }
  document.querySelectorAll('input[name="og-comp"]').forEach(function (cb) {
    cb.addEventListener('change', applyFilter);
  });
  if (ogTable) {
    document.querySelectorAll('#og-table .excel-filter-btn').forEach(function (btn) {
      btn.addEventListener('click', function (ev) {
        ev.stopPropagation();
        var col = parseInt(btn.getAttribute('data-col'), 10);
        var dist = distinctOgColumn(col);
        var key = String(col);
        var cur = ogTable._excelColSelections[key];
        var initial = cur == null ? null : new Set(cur);
        if (!window.ReportExcelFilter) return;
        window.ReportExcelFilter.open({
          anchor: btn,
          title: 'Filter column',
          distinctSnapshot: dist,
          selectedSet: initial,
          onApply: function (set) {
            if (set == null) delete ogTable._excelColSelections[key];
            else if (set.size === 0) ogTable._excelColSelections[key] = new Set();
            else ogTable._excelColSelections[key] = set;
            updateOgFilterButtons();
            applyFilter();
          }
        });
      });
    });
    updateOgFilterButtons();
  }
  wireDateFilter();
  applyFilter();
  sortCol = 3;
  sortAsc = false;
  headers.forEach(function (h) { h.classList.remove('sorted-asc', 'sorted-desc'); });
  var dateSortTh = document.querySelector('#og-table thead tr:first-child th[data-col="3"]');
  if (dateSortTh) dateSortTh.classList.add('sorted-desc');
  sortTable(3, false);
  renumberRows();
})();
</script>"""


def write_legacy_report_redirect() -> None:
    """Write legacy report.html so old URLs land on REPORT_HTML (with shared nav)."""
    dest = html.escape(REPORT_HTML, quote=True)
    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="0; url={dest}">
  <link rel="canonical" href="{dest}">
  <title>Redirecting…</title>
</head>
<body style="font-family:system-ui,sans-serif;padding:2rem;background:#1a0000;color:#ccc;text-align:center">
  <p>Own goals report: <a href="{dest}" style="color:#ff9999">open {html.escape(REPORT_HTML, quote=False)}</a></p>
</body>
</html>"""
    with open(REPORT_HTML_LEGACY_REDIRECT, "w", encoding="utf-8") as f:
        f.write(doc)


# --- Excel-style column filter (checkbox popover; own goals + derived tables) --------

EXCEL_FILTER_CSS = """
    .excel-filter-btn {
      width: 100%;
      max-width: 100%;
      font: inherit;
      font-size: 0.65rem;
      padding: 0.28rem 0.35rem;
      cursor: pointer;
      border-radius: 5px;
      border: 1px solid #8a6666;
      background: #faf8f5;
      color: #222;
      text-align: center;
    }
    .excel-filter-btn:hover { border-color: #cc0000; color: #800; }
    .excel-filter-btn.is-filtered {
      background: #fff4e6;
      border-color: #cc7700;
      font-weight: 700;
    }
    .excel-filter-popover {
      position: fixed;
      z-index: 400;
      min-width: 280px;
      max-width: min(360px, 92vw);
      max-height: min(420px, 70vh);
      display: flex;
      flex-direction: column;
      background: #fff;
      border: 1px solid #c8c0b8;
      border-radius: 10px;
      box-shadow: 0 8px 28px rgba(0,0,0,0.22);
      font-size: 0.82rem;
    }
    .excel-filter-popover[hidden] { display: none !important; }
    .excel-filter-popover-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 0.5rem;
      padding: 0.5rem 0.65rem;
      border-bottom: 1px solid #e0dcd4;
      background: #2a1515;
      color: #fff;
      border-radius: 10px 10px 0 0;
    }
    .excel-filter-title { font-weight: 700; font-size: 0.78rem; flex: 1; }
    .excel-filter-close {
      border: none;
      background: transparent;
      color: #fff;
      font-size: 1.25rem;
      line-height: 1;
      cursor: pointer;
      padding: 0 0.2rem;
    }
    .excel-filter-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 0.35rem;
      padding: 0.45rem 0.6rem;
      border-bottom: 1px solid #e8e4dc;
    }
    .excel-filter-toolbar button {
      font: inherit;
      font-size: 0.72rem;
      padding: 0.25rem 0.5rem;
      border-radius: 5px;
      border: 1px solid #ccc;
      background: #faf8f5;
      cursor: pointer;
    }
    .excel-filter-toolbar button:hover { border-color: #cc0000; }
    .excel-filter-search {
      margin: 0.4rem 0.6rem 0;
      width: calc(100% - 1.2rem);
      font: inherit;
      padding: 0.35rem 0.45rem;
      border: 1px solid #ccc;
      border-radius: 5px;
      box-sizing: border-box;
    }
    .excel-filter-note {
      padding: 0.25rem 0.65rem 0;
      font-size: 0.7rem;
      color: #a60;
      line-height: 1.3;
    }
    .excel-filter-list {
      flex: 1;
      overflow-y: auto;
      padding: 0.35rem 0.5rem 0.5rem;
      max-height: 240px;
    }
    .excel-filter-item {
      display: flex;
      align-items: flex-start;
      gap: 0.4rem;
      padding: 0.2rem 0.15rem;
      cursor: pointer;
      border-radius: 4px;
    }
    .excel-filter-item:hover { background: #f5f2ec; }
    .excel-filter-item input { margin-top: 0.15rem; flex-shrink: 0; }
    .excel-filter-item span { word-break: break-word; line-height: 1.25; }
    .excel-filter-foot {
      padding: 0.45rem 0.6rem;
      border-top: 1px solid #e8e4dc;
      display: flex;
      justify-content: flex-end;
      gap: 0.4rem;
    }
    .excel-filter-foot button {
      font: inherit;
      font-size: 0.78rem;
      font-weight: 600;
      padding: 0.35rem 0.75rem;
      border-radius: 6px;
      border: 1px solid #cc0000;
      background: #cc0000;
      color: #fff;
      cursor: pointer;
    }
    .excel-filter-foot .excel-filter-cancel {
      background: #f5f2ec;
      color: #333;
      border-color: #ccc;
    }
"""

EXCEL_FILTER_CORE_SCRIPT = r"""<script>
(function () {
  if (window.ReportExcelFilter) return;
  var activeCtx = null;
  var repositionHandler = null;
  var scrollParents = [];

  function clearScrollTargets() {
    if (!repositionHandler) return;
    scrollParents.forEach(function (t) {
      t.removeEventListener("scroll", repositionHandler);
    });
    window.removeEventListener("scroll", repositionHandler);
    window.removeEventListener("resize", repositionHandler);
    scrollParents = [];
    repositionHandler = null;
  }

  /**
   * Popover is position:fixed — use viewport coords only (do not add scrollY/scrollX).
   * Clamp to window; prefer below anchor, else above, else pin inside viewport.
   */
  function repositionPopover() {
    if (!activeCtx || !activeCtx.anchor) return;
    var pop = document.getElementById("excel-filter-popover");
    if (!pop || pop.hasAttribute("hidden")) return;
    var anchor = activeCtx.anchor;
    var r = anchor.getBoundingClientRect();
    var m = 8;
    var g = 4;
    var pw = pop.offsetWidth || 300;
    var ph = pop.offsetHeight || 320;
    var left = r.left;
    if (left + pw > window.innerWidth - m) left = window.innerWidth - pw - m;
    if (left < m) left = m;
    var topBelow = r.bottom + g;
    var topAbove = r.top - g - ph;
    var top;
    if (topBelow + ph <= window.innerHeight - m) {
      top = topBelow;
    } else if (topAbove >= m) {
      top = topAbove;
    } else {
      top = Math.max(m, Math.min(topBelow, window.innerHeight - ph - m));
    }
    pop.style.left = left + "px";
    pop.style.top = top + "px";
    pop.style.right = "auto";
    pop.style.bottom = "auto";
  }

  function bindScrollTargets(anchor) {
    clearScrollTargets();
    repositionHandler = repositionPopover;
    var el = anchor;
    while (el) {
      el.addEventListener("scroll", repositionHandler, { passive: true });
      scrollParents.push(el);
      el = el.parentElement;
    }
    window.addEventListener("scroll", repositionHandler, { passive: true });
    window.addEventListener("resize", repositionHandler);
  }

  function installPopover() {
    if (document.getElementById("excel-filter-popover")) return;
    var el = document.createElement("div");
    el.id = "excel-filter-popover";
    el.className = "excel-filter-popover";
    el.setAttribute("hidden", "");
    el.innerHTML =
      '<div class="excel-filter-popover-head">' +
      '<span class="excel-filter-title" id="excel-filter-title">Filter</span>' +
      '<button type="button" class="excel-filter-close" id="excel-filter-close" aria-label="Close">×</button></div>' +
      '<div class="excel-filter-toolbar">' +
      '<button type="button" id="excel-filter-all-column">All values</button>' +
      '<button type="button" id="excel-filter-visible-all">Select visible</button>' +
      '<button type="button" id="excel-filter-visible-none">Clear visible</button></div>' +
      '<input type="search" class="excel-filter-search" id="excel-filter-search" placeholder="Search values…" />' +
      '<div class="excel-filter-note" id="excel-filter-note"></div>' +
      '<div class="excel-filter-list" id="excel-filter-list"></div>' +
      '<div class="excel-filter-foot">' +
      '<button type="button" class="excel-filter-cancel" id="excel-filter-cancel">Cancel</button>' +
      '<button type="button" id="excel-filter-apply">Apply</button></div>';
    document.body.appendChild(el);
    document.getElementById("excel-filter-close").addEventListener("click", close);
    document.getElementById("excel-filter-cancel").addEventListener("click", close);
    document.getElementById("excel-filter-apply").addEventListener("click", onApplyClick);
    document.getElementById("excel-filter-all-column").addEventListener("click", function () {
      if (!activeCtx) return;
      var cb = activeCtx.onApply;
      close();
      if (cb) cb(null);
    });
    document.getElementById("excel-filter-visible-all").addEventListener("click", function () {
      document.querySelectorAll("#excel-filter-list input[type=checkbox]").forEach(function (c) { c.checked = true; });
    });
    document.getElementById("excel-filter-visible-none").addEventListener("click", function () {
      document.querySelectorAll("#excel-filter-list input[type=checkbox]").forEach(function (c) { c.checked = false; });
    });
    document.getElementById("excel-filter-search").addEventListener("input", function () {
      if (activeCtx) renderList(activeCtx);
    });
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") close();
    });
    document.addEventListener("click", function (e) {
      var pop = document.getElementById("excel-filter-popover");
      if (!pop || pop.hidden) return;
      if (pop.contains(e.target)) return;
      if (e.target.closest && e.target.closest(".excel-filter-btn")) return;
      close();
    });
  }

  function close() {
    clearScrollTargets();
    var pop = document.getElementById("excel-filter-popover");
    if (pop) pop.setAttribute("hidden", "");
    activeCtx = null;
  }

  function renderList(ctx) {
    var list = document.getElementById("excel-filter-list");
    var note = document.getElementById("excel-filter-note");
    var q = (document.getElementById("excel-filter-search").value || "").trim().toLowerCase();
    var all = ctx.distinctSnapshot;
    var filtered = all.filter(function (v) { return !q || String(v).toLowerCase().indexOf(q) !== -1; });
    var cap = 350;
    var show = filtered.slice(0, cap);
    if (note) {
      if (all.length > cap) note.textContent = "Column has " + all.length + " unique values; list capped — use search to find more.";
      else if (filtered.length > cap) note.textContent = "Too many matches — refine search.";
      else note.textContent = "";
    }
    list.innerHTML = "";
    var sel = ctx.selectedSet;
    show.forEach(function (v) {
      var lab = document.createElement("label");
      lab.className = "excel-filter-item";
      var inp = document.createElement("input");
      inp.type = "checkbox";
      var key = encodeURIComponent(v);
      inp.dataset.ev = key;
      inp.checked = sel === null || sel.has(v);
      lab.appendChild(inp);
      var sp = document.createElement("span");
      sp.textContent = v === "" ? "(blank)" : String(v);
      lab.appendChild(sp);
      list.appendChild(lab);
    });
  }

  function onApplyClick() {
    if (!activeCtx) return;
    var ctx = activeCtx;
    var checked = [];
    document.querySelectorAll("#excel-filter-list input[type=checkbox]").forEach(function (inp) {
      if (inp.checked) checked.push(decodeURIComponent(inp.dataset.ev));
    });
    var all = ctx.distinctSnapshot;
    var set = new Set(checked);
    var out;
    if (checked.length === 0) {
      out = new Set();
    } else {
      var full = true;
      for (var i = 0; i < all.length; i++) {
        if (!set.has(all[i])) { full = false; break; }
      }
      out = full ? null : set;
    }
    var cb = ctx.onApply;
    close();
    if (cb) cb(out);
  }

  function open(opts) {
    installPopover();
    clearScrollTargets();
    var pop = document.getElementById("excel-filter-popover");
    document.getElementById("excel-filter-title").textContent = opts.title || "Filter";
    document.getElementById("excel-filter-search").value = "";
    var snap = opts.distinctSnapshot || [];
    activeCtx = {
      distinctSnapshot: snap.slice(),
      selectedSet: opts.selectedSet == null ? null : new Set(opts.selectedSet),
      onApply: opts.onApply,
      forceAll: false,
      anchor: opts.anchor
    };
    renderList(activeCtx);
    pop.removeAttribute("hidden");
    requestAnimationFrame(function () {
      requestAnimationFrame(function () {
        repositionPopover();
        bindScrollTargets(opts.anchor);
      });
    });
  }

  window.ReportExcelFilter = { open: open, close: close };
})();
</script>"""


# --- Supabase-derived reports (penalty shootouts, VAR, VAR unpaired) -----------------

DERIVED_TABLE_SCRIPT = r"""<script>
(function () {
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

    function applyFilters() {
      tbody.querySelectorAll("tr").forEach(function (tr) {
        if (tr.querySelector("td[colspan]")) return;
        if (rowPassesColSelections(tr)) tr.classList.remove("derived-row--hidden");
        else tr.classList.add("derived-row--hidden");
      });
      updateFilterButtons();
    }

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
        var isoLike = /^\\d{4}-\\d{2}-\\d{2}/;
        var cmp;
        if (isoLike.test(as) && isoLike.test(bs)) {
          cmp = as.localeCompare(bs);
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

    headers.forEach(function (th) {
      th.addEventListener("click", function () {
        var col = parseInt(th.getAttribute("data-col"), 10);
        if (sortCol === col) sortAsc = !sortAsc;
        else { sortCol = col; sortAsc = true; }
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
            applyFilters();
            if (sortCol !== null) sortTable(sortCol, sortAsc);
          }
        });
      });
    });

    function applyDefaultSort() {
      if (table.id === "table-recordings-library") {
        sortCol = 0;
        sortAsc = false;
        headers.forEach(function (h) { h.classList.remove("sorted-asc", "sorted-desc"); });
        var th0 = table.querySelector('thead tr:first-child th[data-col="0"]');
        if (th0) th0.classList.add("sorted-desc");
        sortTable(0, false);
      } else if (table.id === "table-var-events" || table.id === "table-var-unpaired") {
        sortCol = 2;
        sortAsc = false;
        headers.forEach(function (h) { h.classList.remove("sorted-asc", "sorted-desc"); });
        var th2 = table.querySelector('thead tr:first-child th[data-col="2"]');
        if (th2) th2.classList.add("sorted-desc");
        sortTable(2, false);
      }
    }

    applyFilters();
    applyDefaultSort();
  });
})();
</script>"""


def _derived_fmt(v) -> str:
    if v is True:
        return "Yes"
    if v is False:
        return "No"
    if v is None or v == "":
        return "—"
    return str(v)


def _derived_fmt_recorded(v) -> str:
    """Literal true/false for games.recorded (HTML reports / SQL parity)."""
    if v is True:
        return "true"
    if v is False:
        return "false"
    if v is None or v == "":
        return "—"
    if isinstance(v, str):
        lo = v.strip().lower()
        if lo in ("true", "t", "1", "yes"):
            return "true"
        if lo in ("false", "f", "0", "no"):
            return "false"
    return "—"


def _derived_esc(v) -> str:
    return html.escape(_derived_fmt(v), quote=False)


def _derived_build_table(headers: list[str], keys: list[str], rows: list[dict], table_id: str) -> str:
    safe_id = html.escape(table_id, quote=True)
    th_row = "".join(
        f'<th data-col="{i}" title="Click to sort">{html.escape(h, quote=False)}</th>'
        for i, h in enumerate(headers)
    )
    filter_row = "".join(
        f'<th><button type="button" class="excel-filter-btn" data-col="{i}" data-table="{safe_id}" '
        f'title="Pick values like Excel">Values…</button></th>'
        for i in range(len(headers))
    )
    body: list[str] = []
    for r in rows:
        tds = []
        for k in keys:
            raw = r.get(k)
            if k == "row_num":
                iv = raw if raw is not None else ""
                dv = html.escape(str(iv), quote=True)
                tds.append(f'<td class="num" data-val="{dv}">{html.escape(str(iv), quote=False)}</td>')
                continue
            if k == "commentary" and raw and len(str(raw)) > 240:
                raw = str(raw)[:240] + "…"
            if k == "recorded":
                lit = _derived_fmt_recorded(raw)
                dv = html.escape(lit, quote=True)
                tds.append(f'<td class="center" data-val="{dv}">{html.escape(lit, quote=False)}</td>')
                continue
            dv = html.escape(_derived_fmt(raw), quote=True)
            if k in (
                "sport_event_id",
                "game_id",
                "recording_id",
                "sr_sport_event_id",
            ) and raw:
                inner = f'<code>{html.escape(str(raw), quote=False)}</code>'
                tds.append(f'<td class="mono" data-val="{dv}">{inner}</td>')
            else:
                tds.append(f'<td data-val="{dv}">{_derived_esc(raw)}</td>')
        body.append("<tr>" + "".join(tds) + "</tr>")
    tbody = "\n".join(body) if body else '<tr><td colspan="' + str(len(headers)) + '">No rows.</td></tr>'
    return f"""<div class="table-wrap">
      <table id="{safe_id}" class="sortable-derived">
        <thead>
          <tr>{th_row}</tr>
          <tr class="derived-col-filters">{filter_row}</tr>
        </thead>
        <tbody>{tbody}</tbody>
      </table>
    </div>"""


def _derived_page_shell(
    *,
    title: str,
    badge: str,
    headline: str,
    subtitle: str,
    meta: str,
    table_html: str,
    nav_href: str,
    footer_mid: str | None = None,
) -> str:
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    nav = navigation_html(nav_href)
    footer_note = footer_mid if footer_mid is not None else "(Sportradar timelines)"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{html.escape(title, quote=False)}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html {{ font-size: 14px; }}
    body {{
      font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
      background: #f5f2ec;
      color: #1a1a2a;
      min-height: 100vh;
      padding-bottom: 3rem;
    }}
    .header {{
      background: linear-gradient(160deg, #1a0000 0%, #2e0000 45%, #1a0000 100%);
      border-bottom: 3px solid #cc0000;
      padding: 1.65rem 1.25rem 1.4rem;
      text-align: center;
    }}
    .header-badge {{
      display: inline-block;
      background: #cc0000;
      color: #fff;
      font-size: 0.7rem;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      padding: 0.2rem 0.75rem;
      border-radius: 999px;
      margin-bottom: 0.45rem;
    }}
    h1 {{ font-size: 1.6rem; font-weight: 800; color: #fff; letter-spacing: -0.02em; }}
    .subtitle {{ margin-top: 0.4rem; font-size: 0.9rem; color: #ccc; max-width: 52rem; margin-left: auto; margin-right: auto; line-height: 1.45; }}
    .meta {{
      max-width: 1100px;
      margin: 0.85rem auto 0;
      padding: 0 1rem;
      color: #444;
      font-size: 0.88rem;
      line-height: 1.45;
    }}
    .table-section {{ padding: 1rem 1rem 0; max-width: 1480px; margin: 0 auto; }}
    .table-wrap {{
      overflow-x: auto;
      border: 1px solid #d8d2c8;
      border-radius: 8px;
      background: #fff;
      box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}
    table {{ border-collapse: collapse; width: 100%; min-width: 640px; font-size: 0.82rem; }}
    th, td {{ border-bottom: 1px solid #e8e4dc; padding: 0.38rem 0.5rem; text-align: center; vertical-align: top; }}
    thead tr:first-child th {{
      background: #2a1515;
      color: #fff;
      font-weight: 600;
      white-space: nowrap;
      cursor: pointer;
      user-select: none;
    }}
    thead tr:first-child th:hover {{ background: #3d2020; }}
    thead tr:first-child th.sorted-asc::after  {{ content: " ▲"; font-size: 0.65rem; color: #ffaaaa; }}
    thead tr:first-child th.sorted-desc::after {{ content: " ▼"; font-size: 0.65rem; color: #ffaaaa; }}
    thead tr.derived-col-filters th {{
      background: #352020;
      cursor: default;
      padding: 0.35rem 0.4rem;
      border-bottom: 2px solid #cc0000;
    }}
    .derived-row--hidden {{ display: none !important; }}
    tr:nth-child(even) td {{ background: #faf8f4; }}
    td.mono, td code {{ font-size: 0.76rem; }}
    td.num {{ text-align: center; color: #bbb; font-size: 0.75rem; }}
    .table-toolbar-hint {{
      font-size: 0.78rem;
      color: #555;
      margin: 0 0 0.65rem 0.15rem;
      line-height: 1.35;
    }}
    .footer {{ text-align: center; margin-top: 2rem; font-size: 0.82rem; color: #666; }}
    {EXCEL_FILTER_CSS}
    {NAV_CSS}
  </style>
</head>
<body>
  <div class="report-sticky-top">
    <div class="header">
      <div class="header-badge">{html.escape(badge, quote=False)}</div>
      <h1>{html.escape(headline, quote=False)}</h1>
      <div class="subtitle">{html.escape(subtitle, quote=False)}</div>
    </div>
{nav}
  </div>
  <p class="meta">{meta}</p>
  <div class="table-section">
    <p class="table-toolbar-hint">Click a column header to sort. Use <strong>Values…</strong> under each column for an Excel-style checklist (search within the list when there are many values).</p>
    {table_html}
  </div>
  <div class="footer">
    <p>Data from Supabase {html.escape(footer_note, quote=False)} · Generated {html.escape(gen, quote=False)}</p>
  </div>
{EXCEL_FILTER_CORE_SCRIPT}
{DERIVED_TABLE_SCRIPT}
</body>
</html>"""


def _derived_stub_page(title: str, nav_href: str) -> str:
    nav = navigation_html(nav_href)
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>{html.escape(title)}</title>
<style>body{{font-family:system-ui;padding:2rem;background:#f5f2ec;}}{NAV_CSS}</style></head>
<body><div class="report-sticky-top">{nav}</div><p>Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or config_local) to build this report.</p></body></html>"""


def _overlay_recordings_library_from_export_json() -> None:
    """
    After the Supabase-derived shell exists, replace the table + meta with the JSON export
    layout (see gen_report_recordings_library_html.py). No-op if export file is missing.
    """
    if not os.path.isfile(_RECORDINGS_EXPORT_JSON):
        return
    try:
        from gen_report_recordings_library_html import main as _gen_recordings_library_html
    except ImportError as e:
        print(f"  Recordings library: skipped JSON overlay — {e}")
        return
    try:
        _gen_recordings_library_html()
    except SystemExit as e:
        print(f"  Recordings library: JSON overlay failed ({e})")
        return
    print(
        "  Overlaid report_recordings_library.html from export JSON "
        "(gen_report_recordings_library_html.py)."
    )


def _prepare_recordings_library_rows(rows: list[dict]) -> list[dict]:
    """Oldest kickoff first; renumber ID column 1..n for the HTML report (not DB identity)."""

    def sort_key(r: dict) -> tuple:
        start = r.get("sport_event_start")
        if start is None or start == "":
            return (1, "", str(r.get("sr_sport_event_id") or ""))
        return (0, str(start), str(r.get("sr_sport_event_id") or ""))

    rows.sort(key=sort_key)
    for i, r in enumerate(rows, start=1):
        r["id"] = i
    return rows


def write_derived_reports() -> None:
    """Penalty shootouts, VAR events, VAR unpaired, recordings library — Supabase or stubs."""
    if not USE_SUPABASE:
        print("USE_SUPABASE is False — writing stub HTML for derived reports.")
        stubs = [
            (REPORT_HTML_PENALTY_SHOOTOUTS, "Penalty shootouts", "report_penalty_shootouts.html"),
            (REPORT_HTML_VAR_EVENTS, "VAR events", "report_var_events.html"),
            (REPORT_HTML_VAR_UNPAIRED, "VAR unpaired", "report_var_unpaired.html"),
            (REPORT_HTML_RECORDINGS_LIBRARY, "Recordings library", "report_recordings_library.html"),
        ]
        for path, title, nav in stubs:
            with open(path, "w", encoding="utf-8") as f:
                f.write(_derived_stub_page(title, nav))
            print(f"  Wrote stub: {path}")
        return

    import db

    ps = db.fetch_penalty_shootout_match_rows()
    for i, row in enumerate(ps, 1):
        row["row_num"] = i
    ps_headers = [
        "#",
        "sr_sport_event_id",
        "recording_id",
        "Competition Name",
        "Sport Event Start",
        "Round",
        "Home",
        "Away",
        "Attempts",
        "Sudden Death",
    ]
    ps_keys = [
        "row_num",
        "sport_event_id",
        "recording_id",
        "competition_name",
        "sport_event_start",
        "round",
        "home_team",
        "away_team",
        "shootout_attempts",
        "sudden_death",
    ]
    ps_html = _derived_build_table(ps_headers, ps_keys, ps, "table-penalty-shootouts")
    ps_doc = _derived_page_shell(
        title=f"Penalty shootouts — {SEASON_LABEL}",
        badge="Sportradar Soccer",
        headline="Penalty shootout matches",
        subtitle=REPORT_BLURB_PENALTY_SHOOTOUTS,
        meta=f"<strong>{len(ps):,}</strong> matches with timeline <code>match_status = ap</code> (after penalties). "
        "Attempts count <code>penalty_shootout</code> events with <code>period_type = penalties</code>. "
        "<strong>Sudden death</strong> = more than 10 attempts in that feed.",
        table_html=ps_html,
        nav_href="report_penalty_shootouts.html",
    )
    with open(REPORT_HTML_PENALTY_SHOOTOUTS, "w", encoding="utf-8") as f:
        f.write(ps_doc)
    print(f"  Wrote {REPORT_HTML_PENALTY_SHOOTOUTS} ({len(ps)} rows)")

    vr = db.fetch_var_timeline_event_rows()
    for i, row in enumerate(vr, 1):
        row["row_num"] = i
    vr_headers = [
        "#",
        "ID",
        "Date",
        "recording_id",
        "title",
        "Competition Name",
        "Competition",
        "Home",
        "Away",
        "Type",
        "Description",
        "Decision",
        "Min",
        "ST",
        "Clock",
        "Period",
        "Event ID",
    ]
    vr_keys = [
        "row_num",
        "sport_event_id",
        "sport_event_start",
        "recording_id",
        "title",
        "competition_name",
        "sportradar_competition_id",
        "home_team",
        "away_team",
        "var_event_type",
        "description",
        "decision",
        "match_minute",
        "stoppage_minute",
        "match_clock",
        "period_type",
        "timeline_event_id",
    ]
    vr_html = _derived_build_table(vr_headers, vr_keys, vr, "table-var-events")
    vr_doc = _derived_page_shell(
        title=f"VAR events — {SEASON_LABEL}",
        badge="Sportradar Soccer",
        headline="VAR timeline events",
        subtitle=REPORT_BLURB_VAR_EVENTS,
        meta=f"<strong>{len(vr):,}</strong> rows from <code>video_assistant_referee</code> and "
        "<code>video_assistant_referee_over</code> (completed matches). "
        "<strong>Decision</strong> is often empty on standard timelines (see Extended API docs).",
        table_html=vr_html,
        nav_href="report_var_events.html",
    )
    with open(REPORT_HTML_VAR_EVENTS, "w", encoding="utf-8") as f:
        f.write(vr_doc)
    print(f"  Wrote {REPORT_HTML_VAR_EVENTS} ({len(vr)} rows)")

    vu = db.fetch_var_unpaired_match_rows()
    for i, row in enumerate(vu, 1):
        row["row_num"] = i
    vu_headers = [
        "#",
        "ID",
        "Date",
        "recording_id",
        "title",
        "Competition Name",
        "Competition",
        "Home",
        "Away",
        "VAR starts",
        "VAR overs",
        "Δ (starts − overs)",
    ]
    vu_keys = [
        "row_num",
        "sport_event_id",
        "sport_event_start",
        "recording_id",
        "title",
        "competition_name",
        "sportradar_competition_id",
        "home_team",
        "away_team",
        "video_assistant_referee",
        "video_assistant_referee_over",
        "unpaired_var_starts",
    ]
    vu_html = _derived_build_table(vu_headers, vu_keys, vu, "table-var-unpaired")
    vu_doc = _derived_page_shell(
        title=f"VAR unpaired — {SEASON_LABEL}",
        badge="Sportradar Soccer",
        headline="Matches with unpaired VAR counts",
        subtitle=REPORT_BLURB_VAR_UNPAIRED,
        meta="<strong>Review queue:</strong> matches where counts of <code>video_assistant_referee</code> "
        "and <code>video_assistant_referee_over</code> differ (feed may omit <code>_over</code> events). "
        f"<strong>{len(vu):,}</strong> match(es) in this export.",
        table_html=vu_html,
        nav_href="report_var_unpaired.html",
    )
    with open(REPORT_HTML_VAR_UNPAIRED, "w", encoding="utf-8") as f:
        f.write(vu_doc)
    print(f"  Wrote {REPORT_HTML_VAR_UNPAIRED} ({len(vu)} rows)")

    rl = _prepare_recordings_library_rows(db.fetch_recordings_library_rows())
    rl_headers = [
        "ID",
        "sr_sport_event_id",
        "recording_id",
        "Title",
        "Season",
        "Competition Name",
        "Category",
        "Sport Event Status",
        "Sport Event Start",
        "Sport Event Venue",
        "SR Sport Event Venue ID",
        "Sport Event City",
        "Event Summary",
        "Event Statistics",
        "Event Lineup",
        "Event VAR",
        "Event Commentary",
        "Event Timeline",
        "Event Win Prob",
        "Status",
    ]
    rl_keys = [
        "id",
        "sr_sport_event_id",
        "recording_id",
        "title",
        "season",
        "competition_name",
        "category",
        "sport_event_status",
        "sport_event_start",
        "sport_event_venue",
        "sr_sport_event_venue_id",
        "sport_event_city",
        "event_summary",
        "event_statistics",
        "event_lineup",
        "event_var",
        "event_commentary",
        "event_timeline",
        "event_win_prob",
        "status",
    ]
    rl_html = _derived_build_table(rl_headers, rl_keys, rl, "table-recordings-library")
    rl_doc = _derived_page_shell(
        title=f"Recordings library — {SEASON_LABEL}",
        badge="Sportradar Soccer",
        headline="Soccer Recording/Replay Library",
        subtitle=REPORT_BLURB_RECORDINGS_LIBRARY,
        meta=f"<strong>{len(rl):,}</strong> recording row(s) from Supabase "
        "<code>recordings_library_report</code> (sync via <code>sync_recordings_library.py</code>). "
        "<strong>ID</strong> is row order in this report (1 = oldest <strong>Sport Event Start</strong>). "
        "Boolean columns reflect which APIs were captured in the export.",
        table_html=rl_html,
        nav_href="report_recordings_library.html",
        footer_mid="(Record/Replay library table Soccer // Recordings Library)",
    )
    with open(REPORT_HTML_RECORDINGS_LIBRARY, "w", encoding="utf-8") as f:
        f.write(rl_doc)
    print(f"  Wrote {REPORT_HTML_RECORDINGS_LIBRARY} ({len(rl)} rows)")
    _overlay_recordings_library_from_export_json()


def generate_html(
    rows: list[dict],
    completed_matches: int,
    pipeline_by_season: dict[str, dict[str, int]] | None = None,
) -> str:
    generated = datetime.now(timezone.utc).strftime("%d %B %Y, %H:%M UTC")
    total = len(rows)
    logo_svg = load_svg("lanes_sportsdata.svg")
    pipeline_by_season = pipeline_by_season or {}
    all_season_names = sorted({(r.get("season_name") or "").strip() for r in rows if (r.get("season_name") or "").strip()})
    competition_tile = build_competition_slicer(all_season_names)
    data_date_min, data_date_max = date_bounds_from_rows(rows)
    date_filter_tile = build_date_filter_tile(data_date_min, data_date_max)
    filter_payload = {
        "pipelineBySeason": pipeline_by_season,
        "pipelineGlobal": {"matches": completed_matches},
        "allSeasonNames": all_season_names,
        "dataDateMin": data_date_min,
        "dataDateMax": data_date_max,
        "commentaryAvailableFrom": COMMENTARY_AVAILABLE_FROM_DATE,
    }
    filter_json = json.dumps(filter_payload, ensure_ascii=False)

    if rows:
        player_counts: dict[str, int] = {}
        team_counts: dict[str, int] = {}
        for r in rows:
            player_counts[r["og_player"]] = player_counts.get(r["og_player"], 0) + 1
            team_counts[r["og_player_team"]] = team_counts.get(r["og_player_team"], 0) + 1

        unlucky_player_count = max(player_counts.values())
        unlucky_team_count = max(team_counts.values())

        def flip_name(raw: str) -> str:
            if ", " in raw:
                last, first = raw.split(", ", 1)
                return f"{first} {last}"
            return raw

        top_players = sorted(
            [flip_name(p) for p, c in player_counts.items() if c == unlucky_player_count]
        )
        top_teams = sorted(
            [t for t, c in team_counts.items() if c == unlucky_team_count]
        )

        player_sub = " &amp; ".join(top_players)
        team_sub = " &amp; ".join(top_teams)

        commentary_rows = [r for r in rows if row_in_commentary_coverage_window(r)]
        commentary_correct = sum(
            1 for r in commentary_rows if "own goal" in (r.get("commentary") or "").lower()
        )
        commentary_eligible = len(commentary_rows)
        commentary_incorrect = commentary_eligible - commentary_correct
        games_with_og = len(set(r["sport_event_id"] for r in rows))

        commentary_cutoff_label = datetime.strptime(COMMENTARY_AVAILABLE_FROM_DATE, "%Y-%m-%d").strftime(
            "%B %d, %Y"
        )
        commentary_note = (
            f"Commentary KPIs count only own goals from matches on or after {commentary_cutoff_label} "
            "(Sportradar drops timeline commentary after about 14 days, so older reloads lack "
            "feed text)."
        )
        commentary_note_html = html.escape(commentary_note, quote=False)

        stats_cards_html = f"""
          <div class="stat-card">
            <div class="stat-number" id="kpi-matches">{completed_matches:,}</div>
            <div class="stat-label">Matches Reviewed</div>
          </div>
          <div class="stat-card">
            <div class="stat-number" id="kpi-total-og">{total}</div>
            <div class="stat-label">Total # of Own Goals</div>
          </div>
          <div class="stat-card">
            <div class="stat-number" id="kpi-games">{games_with_og}</div>
            <div class="stat-label"># of Games with an Own Goal</div>
          </div>
          <div class="stat-card">
            <div class="stat-number" id="kpi-most-player">{unlucky_player_count}</div>
            <div class="stat-label">Most by One Player<br><span class="stat-sub" id="kpi-most-player-names">{player_sub}</span></div>
          </div>
          <div class="stat-card">
            <div class="stat-number" id="kpi-most-team">{unlucky_team_count}</div>
            <div class="stat-label">Most by One Team<br><span class="stat-sub" id="kpi-most-team-names">{team_sub}</span></div>
          </div>
          <div class="stat-card stat-card--correct">
            <div class="stat-number stat-number--correct" id="kpi-commentary-yes">{commentary_correct}</div>
            <div class="stat-label">Commentary Correct<br><span class="stat-sub stat-sub--muted">Mentions &quot;own goal&quot;</span></div>
          </div>
          <div class="stat-card stat-card--incorrect">
            <div class="stat-number stat-number--incorrect" id="kpi-commentary-no">{commentary_incorrect}</div>
            <div class="stat-label">Commentary Incorrect<br><span class="stat-sub stat-sub--muted">No &quot;own goal&quot; mention</span></div>
          </div>
          <div class="stat-card stat-card--wide commentary-coverage-note" role="note">
            <span>{commentary_note_html}</span>
          </div>"""
    else:
        stats_cards_html = f"""
          <div class="stat-card">
            <div class="stat-number" id="kpi-matches">{completed_matches:,}</div>
            <div class="stat-label">Matches Reviewed</div>
          </div>"""

    stats_html = (
        '<div class="stats-grid">\n'
        + competition_tile
        + "\n"
        + date_filter_tile
        + "\n"
        + stats_cards_html
        + "\n</div>"
    )

    table_rows = build_table_rows(rows)

    logo_section = ""
    if logo_svg:
        logo_section = f"""
  <div class="branding">
    <span class="branding-label">Brought to you by</span>
    <div class="branding-logo">{logo_svg}</div>
  </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Own Goals — {SEASON_LABEL}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    html {{ font-size: 14px; }}
    body {{
      font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
      background: #f5f2ec;
      color: #1a1a2a;
      min-height: 100vh;
      padding-bottom: 3rem;
    }}

    /* ── Header ───────────────────────────────────────── */
    .header {{
      background: linear-gradient(160deg, #1a0000 0%, #2e0000 45%, #1a0000 100%);
      border-bottom: 3px solid #cc0000;
      padding: 2rem 1.5rem 1.75rem;
      text-align: center;
    }}
    .header-badge {{
      display: inline-block;
      background: #cc0000;
      color: #fff;
      font-size: 0.7rem;
      font-weight: 700;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      padding: 0.25rem 0.85rem;
      border-radius: 999px;
      margin-bottom: 0.75rem;
    }}
    .header h1 {{
      font-size: 2rem;
      font-weight: 800;
      color: #fff;
      letter-spacing: -0.02em;
    }}
    .header h1 span {{ color: #ff6666; }}
    .header .subtitle {{
      margin-top: 0.5rem;
      font-size: 0.95rem;
      color: #ccc;
    }}
    .header .subtitle strong {{ color: #fff; }}
    .header .header-lead {{ margin: 0; }}
    .header .header-desc,
    .header .header-note {{
      margin: 0.55rem auto 0;
      max-width: 44rem;
      font-size: 0.88rem;
      line-height: 1.45;
      color: #c8c8c8;
    }}
    .header .header-note {{ margin-top: 0.45rem; }}
    .header .header-filter-hint {{ font-weight: 400; color: #aaa; }}

    .og-row--hidden {{ display: none !important; }}

    .stat-card--wide {{ grid-column: 1 / -1; }}
    .stat-card--text-left {{ text-align: left; }}
    .stat-card--filter-tile {{ padding: 1rem 1.1rem; }}
    .commentary-coverage-note {{
      padding: 0.65rem 1rem;
      text-align: left;
      font-size: 0.78rem;
      line-height: 1.45;
      color: #555;
      background: #faf8f4;
      border-style: dashed;
      border-color: #cfc8bc;
    }}

    .competition-slicer--tile {{
      margin: 0;
      padding: 0;
      max-width: none;
      background: transparent;
      border: none;
      text-align: left;
    }}
    .stat-card--filter-tile .slicer-head {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 0.5rem;
      margin-bottom: 0.6rem;
    }}
    .stat-card--filter-tile .slicer-title {{
      font-size: 0.72rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: #cc0000;
    }}
    .stat-card--filter-tile .slicer-actions {{ display: inline-flex; gap: 0.35rem; }}
    .stat-card--filter-tile .slicer-btn {{
      font: inherit;
      cursor: pointer;
      font-size: 0.7rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      padding: 0.3rem 0.55rem;
      border-radius: 6px;
      border: 1px solid #ddd8d0;
      background: #faf8f5;
      color: #333;
    }}
    .stat-card--filter-tile .slicer-btn:hover {{
      border-color: #cc0000;
      background: #fff0f0;
      color: #990000;
    }}
    .stat-card--filter-tile .slicer-chips {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.45rem 0.65rem;
      justify-content: flex-start;
    }}
    .stat-card--filter-tile .slicer-chip {{
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      font-size: 0.78rem;
      color: #333;
      cursor: pointer;
      user-select: none;
    }}
    .stat-card--filter-tile .slicer-chip input {{ accent-color: #cc0000; width: 1rem; height: 1rem; }}
    .stat-card--filter-tile .slicer-chip span {{ line-height: 1.25; }}

    .date-filter-tile {{
      text-align: left;
    }}
    .date-filter-head {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 0.6rem;
      margin-bottom: 0.65rem;
    }}
    .date-filter-title {{
      font-size: 0.72rem;
      font-weight: 700;
      color: #888;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .date-filter-presets {{ display: flex; flex-wrap: wrap; gap: 0.35rem; }}
    .date-preset-btn {{
      font: inherit;
      cursor: pointer;
      font-size: 0.72rem;
      font-weight: 600;
      padding: 0.35rem 0.6rem;
      border-radius: 6px;
      border: 1px solid #ddd8d0;
      background: #faf8f5;
      color: #333;
    }}
    .date-preset-btn:hover {{ border-color: #cc0000; color: #cc0000; }}
    .date-preset-btn.is-active {{
      border-color: #cc0000;
      background: #fff0f0;
      color: #990000;
    }}
    .date-filter-custom {{
      display: flex;
      flex-wrap: wrap;
      align-items: flex-end;
      gap: 1rem;
    }}
    .date-filter-label {{
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
      font-size: 0.72rem;
      font-weight: 600;
      color: #555;
    }}
    .date-filter-label input[type="date"] {{
      font: inherit;
      padding: 0.35rem 0.5rem;
      border: 1px solid #ccc;
      border-radius: 6px;
      background: #fff;
      color: #111;
    }}
    .date-filter-hint {{
      margin-top: 0.55rem;
      font-size: 0.78rem;
      color: #666;
      line-height: 1.35;
    }}
    .date-filter-muted {{ font-size: 0.82rem; color: #888; margin-top: 0.35rem; }}

    /* ── Stats ────────────────────────────────────────── */
    .stats-section {{
      max-width: 1100px;
      margin: 2rem auto 0;
      padding: 0 1rem;
    }}
    .stats-grid {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 0.75rem;
      align-items: start;
    }}
    .stat-card {{
      background: #ffffff;
      border: 1px solid #ddd8d0;
      border-radius: 10px;
      padding: 1rem 0.75rem;
      text-align: center;
      transition: border-color 0.2s, box-shadow 0.2s;
      box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}
    .stat-card:hover {{ border-color: #cc0000; box-shadow: 0 2px 8px rgba(204,0,0,0.12); }}
    .stat-card--correct {{ border-color: #b8ddb8; background: #f4fbf4; }}
    .stat-card--correct:hover {{ border-color: #2a7a2a; box-shadow: 0 2px 8px rgba(42,122,42,0.12); }}
    .stat-card--incorrect {{ border-color: #f0c0c0; background: #fff5f5; }}
    .stat-card--incorrect:hover {{ border-color: #cc0000; box-shadow: 0 2px 8px rgba(204,0,0,0.15); }}
    .stat-number {{
      font-size: 1.9rem;
      font-weight: 800;
      color: #cc0000;
      line-height: 1;
    }}
    .stat-number--correct  {{ color: #1a7a1a; }}
    .stat-number--incorrect {{ color: #cc0000; }}
    .stat-label {{
      margin-top: 0.35rem;
      font-size: 0.72rem;
      color: #888;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .stat-sub {{
      display: block;
      color: #333;
      font-size: 0.78rem;
      font-weight: 600;
      text-transform: none;
      letter-spacing: 0;
      margin-top: 0.1rem;
    }}
    .stat-sub--muted {{ color: #888; font-weight: 400; font-style: italic; }}

    /* ── Table section ────────────────────────────────── */
    .table-section {{
      max-width: 100%;
      margin: 1.75rem auto 0;
      padding: 0 1rem;
    }}
    .table-header-row {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 0.6rem;
      flex-wrap: wrap;
      gap: 0.4rem;
    }}
    .table-title {{
      font-size: 0.85rem;
      font-weight: 700;
      color: #cc0000;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .sort-hint {{
      font-size: 0.72rem;
      color: #999;
      max-width: 36rem;
      text-align: right;
      line-height: 1.35;
    }}
    .table-wrap {{
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      border-radius: 8px;
      border: 1px solid #ddd8d0;
      box-shadow: 0 1px 4px rgba(0,0,0,0.07);
    }}

    /* ── Table ────────────────────────────────────────── */
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.78rem;
      table-layout: fixed;
    }}
    col.c-num        {{ width: 2%; }}
    col.c-comp       {{ width: 11%; }}
    col.c-title      {{ width: 13%; }}
    col.c-matchdate  {{ width: 7%; }}
    col.c-matchid    {{ width: 10%; }}
    col.c-recorded   {{ width: 5%; }}
    col.c-recordingid {{ width: 9%; }}
    col.c-scorer     {{ width: 10%; }}
    col.c-playerid   {{ width: 8%; }}
    col.c-min        {{ width: 4%; }}
    col.c-team       {{ width: 9%; }}
    col.c-score      {{ width: 5%; }}
    col.c-final      {{ width: 5%; }}
    col.c-ogmention  {{ width: 5%; }}
    col.c-commentary {{ width: 16%; }}

    thead tr:first-child {{
      background: #1a0000;
      border-bottom: 2px solid #cc0000;
    }}
    thead tr:first-child th {{
      padding: 0.55rem 0.4rem;
      text-align: left;
      font-size: 0.68rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: #ffaaaa;
      white-space: nowrap;
      overflow: hidden;
      cursor: pointer;
      user-select: none;
    }}
    thead tr:first-child th:hover {{ background: #2e0000; color: #ffd0d0; }}
    thead tr:first-child th.sorted-asc::after  {{ content: " ▲"; font-size: 0.6rem; color: #ff8888; }}
    thead tr:first-child th.sorted-desc::after {{ content: " ▼"; font-size: 0.6rem; color: #ff8888; }}
    thead tr:first-child th.num {{ text-align: center; cursor: default; }}
    thead tr:first-child th.center {{ text-align: center; }}
    thead tr.og-col-filters th {{
      background: #2a1515;
      cursor: default;
      padding: 0.3rem 0.35rem;
      border-bottom: 2px solid #cc0000;
      text-transform: none;
      font-weight: 400;
      letter-spacing: normal;
    }}
    tbody tr {{ background: #ffffff; transition: background 0.12s; }}
    tbody tr:nth-child(even) {{ background: #faf7f2; }}
    tbody tr:hover {{ background: #fff0f0; }}

    td {{
      padding: 0.45rem 0.4rem;
      text-align: center;
      vertical-align: middle;
      border-bottom: 1px solid #e8e2d8;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    td.num {{ text-align: center; color: #bbb; font-size: 0.75rem; }}
    td.minute {{
      text-align: center;
      font-size: 0.9rem;
      font-weight: 700;
      color: #c07000;
    }}
    td.score {{
      text-align: center;
      font-variant-numeric: tabular-nums;
      font-weight: 600;
      color: #555;
    }}
    td.score.final {{ color: #111; font-weight: 700; }}

    .match-name {{
      font-weight: 600;
      color: #111;
      text-align: center;
      white-space: normal;
      overflow: visible;
    }}
    .player-name {{
      font-weight: 600;
      color: #222;
      text-align: center;
      white-space: normal;
      overflow: visible;
    }}
    .meta {{
      font-size: 0.68rem;
      color: #999;
      margin-top: 0.1rem;
      text-align: center;
      white-space: normal;
    }}
    .team-badge {{
      display: inline-block;
      background: #fff0f0;
      border: 1px solid #ffcccc;
      color: #cc0000;
      border-radius: 4px;
      padding: 0.2rem 0.5rem;
      font-size: 0.72rem;
      font-weight: 600;
      white-space: nowrap;
    }}
    .id-cell code {{
      font-family: 'Cascadia Code', 'Consolas', 'Courier New', monospace;
      font-size: 0.68rem;
      color: #555;
      background: #f0ede8;
      border: 1px solid #ddd8d0;
      border-radius: 3px;
      padding: 0.15rem 0.35rem;
      display: block;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }}
    .match-id-cell code {{
      font-size: 0.62rem;
      overflow: visible;
      text-overflow: clip;
    }}
    td.og-yes {{ text-align: center; color: #1a7a1a; font-weight: 700; font-size: 0.82rem; }}
    td.og-no  {{ text-align: center; color: #cc0000; font-weight: 700; font-size: 0.82rem; }}
    td.recorded-true {{
      text-align: center;
      font-family: 'Cascadia Code', 'Consolas', 'Courier New', monospace;
      font-size: 0.78rem;
      font-weight: 700;
      color: #1a5c1a;
    }}
    td.recorded-false {{
      text-align: center;
      font-family: 'Cascadia Code', 'Consolas', 'Courier New', monospace;
      font-size: 0.78rem;
      font-weight: 600;
      color: #666;
    }}
    td.recorded-unknown {{
      text-align: center;
      font-family: 'Cascadia Code', 'Consolas', 'Courier New', monospace;
      font-size: 0.78rem;
      color: #999;
    }}
    .commentary-cell {{ white-space: normal; max-width: 260px; }}
    .commentary {{
      font-size: 0.75rem;
      color: #555;
      font-style: italic;
      line-height: 1.4;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }}

    /* ── Branding ─────────────────────────────────────── */
    .branding {{
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 1rem;
      margin-top: 2.5rem;
      padding: 1.25rem 1rem;
      border-top: 1px solid #ddd8d0;
      flex-wrap: wrap;
    }}
    .branding-label {{
      font-size: 0.72rem;
      color: #aaa;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      white-space: nowrap;
    }}
    .branding-logo {{
      height: 120px;
      display: flex;
      align-items: center;
    }}
    .branding-logo svg {{
      height: 120px;
      width: auto;
    }}

    /* ── Footer ───────────────────────────────────────── */
    .footer {{
      text-align: center;
      margin-top: 1rem;
      color: #aaa;
      font-size: 0.72rem;
    }}
    .footer a {{ color: #cc0000; text-decoration: none; }}

    /* ── Mobile card view (portrait phones) ───────────── */
    @media (max-width: 599px) and (orientation: portrait) {{
      .table-wrap {{ border: none; background: transparent; box-shadow: none; }}
      table, thead, tbody, tr, th, td {{ display: block; }}
      thead {{ display: none; }}
      tbody tr {{
        background: #ffffff;
        border: 1px solid #ddd8d0;
        border-radius: 10px;
        margin-bottom: 0.85rem;
        padding: 0.75rem;
        white-space: normal;
        box-shadow: 0 1px 3px rgba(0,0,0,0.07);
      }}
      tbody tr:nth-child(even) {{ background: #ffffff; }}
      tbody tr:hover {{ background: #fff5f5; }}
      td {{
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        padding: 0.3rem 0;
        border-bottom: 1px solid #e8e2d8;
        white-space: normal;
        font-size: 0.82rem;
      }}
      td:last-child {{ border-bottom: none; }}
      td::before {{
        content: attr(data-label);
        font-size: 0.65rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #cc0000;
        min-width: 90px;
        padding-right: 0.5rem;
        flex-shrink: 0;
      }}
      td.num {{ display: none; }}
      .id-cell, .commentary-cell {{ display: flex; }}
      .id-cell code {{ font-size: 0.65rem; white-space: normal; word-break: break-all; }}
      .commentary {{ -webkit-line-clamp: 3; }}
    }}

    /* ── Tablet / landscape phone ─────────────────────── */
    @media (max-width: 1100px) {{
      .stats-grid {{ grid-template-columns: repeat(4, 1fr); }}
    }}
    @media (max-width: 900px) {{
      .stats-grid {{ grid-template-columns: repeat(3, 1fr); }}
    }}
    @media (max-width: 599px) {{
      .stats-grid {{ grid-template-columns: repeat(2, 1fr); }}
      html {{ font-size: 13px; }}
      .header h1 {{ font-size: 1.5rem; }}
      .table-section {{ padding: 0 0.5rem; }}
    }}

    /* Landscape phone — keep table but hide IDs & commentary */
    @media (max-width: 767px) and (orientation: landscape) {{
      .id-cell, .commentary-cell {{ display: none; }}
      html {{ font-size: 12px; }}
    }}

    /* Portrait tablet — hide only IDs */
    @media (min-width: 600px) and (max-width: 899px) and (orientation: portrait) {{
      .id-cell {{ display: none; }}
    }}
{EXCEL_FILTER_CSS}
{NAV_CSS}
  </style>
</head>
<body>

  <div class="report-sticky-top">
  <div class="header">
    <div class="header-badge">Sportradar Soccer</div>
    <h1><span>Own Goals</span> Tracker</h1>
    <div class="subtitle">
      <p class="header-lead"><strong id="subtitle-og-total">{total}</strong> own goal<span id="subtitle-og-plural">{"s" if total != 1 else ""}</span> <span id="subtitle-filter-scope" class="header-filter-hint">· all competitions</span></p>
      <p class="header-desc">{html.escape(REPORT_BLURB_OWN_GOALS, quote=False)}</p>
      <p class="header-note">{html.escape(REPORT_BLURB_OWN_GOALS_NOTE, quote=False)}</p>
    </div>
  </div>

{navigation_html(REPORT_HTML)}
  </div>

  <div class="stats-section">
    {stats_html}
  </div>

  <div class="table-section">
    <div class="table-header-row">
      <div class="table-title" id="table-filter-title">All Own Goals</div>
      <div class="sort-hint">Click a column header to sort. Use <strong>Values…</strong> for an Excel-style value checklist (search inside the list when needed).</div>
    </div>
    <div class="table-wrap">
      <table id="og-table">
        <colgroup>
          <col class="c-num">
          <col class="c-comp">
          <col class="c-title">
          <col class="c-matchdate">
          <col class="c-matchid">
          <col class="c-recorded">
          <col class="c-recordingid">
          <col class="c-scorer">
          <col class="c-playerid">
          <col class="c-min">
          <col class="c-team">
          <col class="c-score">
          <col class="c-final">
          <col class="c-ogmention">
          <col class="c-commentary">
        </colgroup>
        <thead>
          <tr>
            <th class="num">#</th>
            <th data-col="1">Competition</th>
            <th data-col="2">Title</th>
            <th data-col="3">Date</th>
            <th data-col="4">Match ID</th>
            <th class="center" data-col="5">Recorded</th>
            <th data-col="6">Recording ID</th>
            <th data-col="7">Own Goal Scorer</th>
            <th data-col="8">Player ID</th>
            <th class="center" data-col="9">Min</th>
            <th data-col="10">Benefiting Team</th>
            <th class="center" data-col="11">Score at OG</th>
            <th class="center" data-col="12">Final Score</th>
            <th class="center" data-col="13">Mentions OG?</th>
            <th data-col="14">Commentary</th>
          </tr>
          <tr class="og-col-filters">
            <th class="num"></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="1" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="2" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="3" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="4" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="5" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="6" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="7" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="8" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="9" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="10" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="11" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="12" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="13" title="Pick values like Excel">Values…</button></th>
            <th><button type="button" class="excel-filter-btn" data-table="og-table" data-col="14" title="Pick values like Excel">Values…</button></th>
          </tr>
        </thead>
        <tbody id="og-tbody">
          {table_rows}
        </tbody>
      </table>
    </div>
  </div>

  {logo_section}

  <div class="footer">
    <p>Data sourced from <a href="https://developer.sportradar.com" target="_blank">Sportradar Soccer API</a> &bull; Report generated {generated}</p>
  </div>

  {EXCEL_FILTER_CORE_SCRIPT}
  <script type="application/json" id="report-filter-data">{filter_json}</script>
  {_inline_report_script()}

</body>
</html>"""


def main():
    print("Generating reports…", flush=True)
    pipeline_by_season: dict = {}
    if USE_SUPABASE:
        import db
        print("  Loading own goals from Supabase (may take a bit)…", flush=True)
        rows = db.get_all_own_goals()
        print("  Counting stored timelines (exact count)…", flush=True)
        completed_matches = db.get_completed_timelines_count()
        print("  Pipeline match counts by season…", flush=True)
        pipeline_by_season = db.get_pipeline_stats_by_season_name()
        rows.sort(
            key=lambda r: (
                str(r.get("match_date") or "")[:10],
                int(r["minute"]) if str(r.get("minute") or "").isdigit() else 0,
            )
        )
        rows.reverse()
        print(f"Loaded {len(rows)} own goal records from Supabase")
    else:
        print(f"  Reading {OWN_GOALS_CSV} and cached timelines…", flush=True)
        rows = load_own_goals(OWN_GOALS_CSV)
        rows.sort(
            key=lambda r: (
                str(r.get("match_date") or "")[:10],
                int(r["minute"]) if str(r.get("minute") or "").isdigit() else 0,
            )
        )
        rows.reverse()
        completed_matches = count_completed_matches()
        print(f"Loaded {len(rows)} own goal records from {OWN_GOALS_CSV}")
    print(f"Matches with stored timeline : {completed_matches:,}")
    html = generate_html(rows, completed_matches, pipeline_by_season)
    with open(REPORT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Report written to: {REPORT_HTML}")
    write_legacy_report_redirect()
    print(f"Legacy redirect written to: {REPORT_HTML_LEGACY_REDIRECT} -> {REPORT_HTML}")
    print("Generating companion reports (penalty shootouts, VAR, VAR unpaired, recordings, list of all games)…")
    write_derived_reports()
    import master_games_report

    master_games_report.write_master_games_report()
    print(f"Open {REPORT_HTML} (and linked pages) in your browser to view.")


if __name__ == "__main__":
    main()
