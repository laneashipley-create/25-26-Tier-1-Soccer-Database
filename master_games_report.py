"""
List of All Games HTML — all rows from public."All Games (sr:sport_events)".

Built by generate_report.main(); uses competition slicer + date range + KPI tiles like own_goals.
"""

from __future__ import annotations

import html
import json
from datetime import datetime, timezone

from config import REPORT_BLURB_LIST_OF_ALL_GAMES, REPORT_HTML_MASTER_GAMES, USE_SUPABASE
from report_navigation import NAV_CSS, navigation_html


def build_competition_slicer_mg(names: list[str]) -> str:
    if not names:
        return ""
    chips = []
    for name in names:
        ev = html.escape(name, quote=True)
        disp = html.escape(name, quote=False)
        chips.append(
            f'<label class="slicer-chip"><input type="checkbox" name="mg-comp" value="{ev}" checked />'
            f"<span>{disp}</span></label>"
        )
    chips_html = "\n".join(chips)
    return f"""    <div class="stat-card stat-card--wide stat-card--filter-tile stat-card--text-left" role="region" aria-label="Filter by competition">
      <div class="competition-slicer competition-slicer--tile" role="group">
        <div class="slicer-head">
          <span class="slicer-title">Competitions</span>
          <span class="slicer-actions">
            <button type="button" class="slicer-btn" id="mg-slicer-all">All</button>
            <button type="button" class="slicer-btn" id="mg-slicer-none">None</button>
          </span>
        </div>
        <div class="slicer-chips">{chips_html}</div>
      </div>
    </div>"""


def date_bounds_master(rows: list[dict]) -> tuple[str, str]:
    dates: list[str] = []
    for r in rows:
        d = (r.get("match_date") or "")[:10]
        if len(d) == 10 and d[4] == "-" and d[7] == "-":
            dates.append(d)
    if not dates:
        return "", ""
    return min(dates), max(dates)


def build_date_filter_tile_mg(date_min: str, date_max: str) -> str:
    if not date_min or not date_max:
        return """    <div class="stat-card stat-card--wide stat-card--filter-tile stat-card--text-left date-filter-tile" role="region" aria-label="Kickoff date filter">
      <div class="date-filter-title">Kickoff date range (UTC)</div>
      <p class="date-filter-muted">No kickoff dates in this export yet.</p>
    </div>"""
    dmin = html.escape(date_min, quote=True)
    dmax = html.escape(date_max, quote=True)
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
          <input type="date" id="mg-date-from" min="1990-01-01" max="2099-12-31" value="{dmin}" />
        </label>
        <label class="date-filter-label">To
          <input type="date" id="mg-date-to" min="1990-01-01" max="2099-12-31" value="{dmax}" />
        </label>
      </div>
      <p class="date-filter-hint" id="mg-date-hint"></p>
    </div>"""


def master_dataset_stats(rows: list[dict]) -> tuple[int, int, int, int]:
    """games, distinct teams, distinct leagues (season_name), completed count."""
    teams: set[str] = set()
    leagues: set[str] = set()
    completed = 0
    for r in rows:
        ht = (r.get("home_team") or "").strip()
        at = (r.get("away_team") or "").strip()
        if ht:
            teams.add(ht)
        if at:
            teams.add(at)
        sn = (r.get("season_name") or "").strip()
        if sn:
            leagues.add(sn)
        st = (r.get("status") or "").strip().lower()
        if st in ("closed", "ended"):
            completed += 1
    return len(rows), len(teams), len(leagues), completed


def build_master_table_rows(rows: list[dict]) -> str:
    from generate_report import format_score

    if not rows:
        return '<tr><td colspan="12" style="text-align:center;padding:2rem;color:#666;">No games found.</td></tr>'

    out: list[str] = []
    for i, r in enumerate(rows, 1):
        sn = (r.get("season_name") or "").strip()
        comp_attr = html.escape(sn, quote=True)
        date_raw = (r.get("match_date") or "")[:10]
        date_attr = html.escape(date_raw if len(date_raw) == 10 else "", quote=True)
        ev_id = str(r.get("sport_event_id") or "")
        ev_attr = html.escape(ev_id, quote=True)
        kick = str(r.get("start_time") or "")
        rnd = str(r.get("round") or "")
        hs = r.get("home_score")
        aws = r.get("away_score")
        score_txt = format_score(str(hs) if hs != "" else "", str(aws) if aws != "" else "")
        rid = str(r.get("recording_id") or "")
        title_raw = (r.get("title") or "").strip()
        title_disp = title_raw if title_raw else "—"
        comp_nm = (r.get("competition_name") or "").strip() or "—"
        st = str(r.get("status") or "")
        mst = str(r.get("match_status") or "")
        out.append(f"""
        <tr class="mg-data-row" data-competition="{comp_attr}" data-match-date="{date_attr}" data-event-id="{ev_attr}">
          <td class="num" data-val="{i}" data-label="#">{i}</td>
          <td class="id-cell match-id-cell" data-val="{ev_attr}" data-label="sr_sport_event_id"><code title="{ev_attr}">{html.escape(ev_id, quote=False)}</code></td>
          <td class="id-cell" data-val="{html.escape(rid, quote=True)}" data-label="recording_id"><code>{html.escape(rid or '—', quote=False)}</code></td>
          <td data-val="{html.escape(title_disp, quote=True)}" data-label="Title">{html.escape(title_disp, quote=False)}</td>
          <td data-val="{html.escape(comp_nm, quote=True)}" data-label="Competition Name">{html.escape(comp_nm, quote=False)}</td>
          <td class="mono" data-val="{html.escape(kick, quote=True)}" data-label="Sport Event Start"><code>{html.escape(kick or '—', quote=False)}</code></td>
          <td data-val="{html.escape(rnd, quote=True)}" data-label="Round">{html.escape(rnd or '—', quote=False)}</td>
          <td data-val="{html.escape(str(r.get('home_team') or ''), quote=True)}" data-label="Home">{html.escape(str(r.get('home_team') or ''), quote=False)}</td>
          <td data-val="{html.escape(str(r.get('away_team') or ''), quote=True)}" data-label="Away">{html.escape(str(r.get('away_team') or ''), quote=False)}</td>
          <td class="score" data-val="{html.escape(score_txt, quote=True)}" data-label="Final Score">{html.escape(score_txt, quote=False)}</td>
          <td data-val="{html.escape(st, quote=True)}" data-label="Sport Event Status">{html.escape(st or '—', quote=False)}</td>
          <td data-val="{html.escape(mst, quote=True)}" data-label="Match Status">{html.escape(mst or '—', quote=False)}</td>
        </tr>""")
    return "\n".join(out)


def _inline_master_games_script() -> str:
    return r"""<script>
(function () {
  const tbody = document.getElementById('mg-tbody');
  if (!tbody) return;
  var mgTable = document.getElementById('mg-table');
  if (mgTable) mgTable._excelColSelections = mgTable._excelColSelections || {};
  const filterPayload = (function () {
    const el = document.getElementById('report-filter-data-mg');
    if (!el) return {};
    try { return JSON.parse(el.textContent); } catch (e) { return {}; }
  })();

  const allNames = filterPayload.allSeasonNames || [];
  const dataDateMin = filterPayload.dataDateMin || '';
  const dataDateMax = filterPayload.dataDateMax || '';

  const headers = document.querySelectorAll('#mg-table thead tr:first-child th[data-col]');
  let sortCol = null;
  let sortAsc = true;

  function cellVal(tr, idx) {
    const c = tr.children[idx];
    return c ? (c.getAttribute('data-val') || '') : '';
  }

  function sortTable(colIndex, asc) {
    const all = Array.from(tbody.querySelectorAll('tr.mg-data-row'));
    const visible = all.filter(function (tr) { return !tr.classList.contains('mg-row--hidden'); });
    const hidden = all.filter(function (tr) { return tr.classList.contains('mg-row--hidden'); });
    visible.sort(function (a, b) {
      const aVal = cellVal(a, colIndex);
      const bVal = cellVal(b, colIndex);
      const aNum = parseFloat(aVal);
      const bNum = parseFloat(bVal);
      let cmp;
      if (!isNaN(aNum) && !isNaN(bNum) && aVal !== '' && bVal !== '') cmp = aNum - bNum;
      else cmp = String(aVal).localeCompare(String(bVal), undefined, { numeric: true, sensitivity: 'base' });
      return asc ? cmp : -cmp;
    });
    visible.concat(hidden).forEach(function (r) { tbody.appendChild(r); });
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
    tbody.querySelectorAll('tr.mg-data-row').forEach(function (tr) {
      if (tr.classList.contains('mg-row--hidden')) return;
      const cell = tr.querySelector('td.num');
      if (cell) {
        cell.textContent = String(n);
        cell.setAttribute('data-val', String(n));
        n += 1;
      }
    });
  }

  function fmtInt(x) { return x.toLocaleString('en-US'); }

  function aggMasterVisible() {
    const visible = Array.prototype.slice.call(tbody.querySelectorAll('tr.mg-data-row')).filter(function (tr) {
      return !tr.classList.contains('mg-row--hidden');
    });
    const teams = {};
    const leagues = {};
    let completed = 0;
    visible.forEach(function (tr) {
      const hc = cellVal(tr, 7);
      const ac = cellVal(tr, 8);
      if (hc) teams[hc] = true;
      if (ac) teams[ac] = true;
      const comp = cellVal(tr, 4);
      if (comp) leagues[comp] = true;
      const st = (cellVal(tr, 10) || '').toLowerCase();
      if (st === 'closed' || st === 'ended') completed += 1;
    });
    return {
      games: visible.length,
      teams: Object.keys(teams).length,
      leagues: Object.keys(leagues).length,
      completed: completed,
      upcoming: visible.length - completed
    };
  }

  function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function getSelectedComps() {
    const boxes = document.querySelectorAll('input[name="mg-comp"]');
    if (!boxes.length) return null;
    return Array.prototype.slice.call(document.querySelectorAll('input[name="mg-comp"]:checked')).map(function (cb) { return cb.value; });
  }

  function allCompetitionChipsSelected() {
    const boxes = document.querySelectorAll('input[name="mg-comp"]');
    if (!boxes.length) return true;
    return document.querySelectorAll('input[name="mg-comp"]:checked').length === boxes.length;
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
    const fromEl = document.getElementById('mg-date-from');
    const toEl = document.getElementById('mg-date-to');
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
    var el = document.getElementById('mg-date-hint');
    if (!el) return;
    if (!dr.hasInputs || !dr.lo || !dr.hi) { el.textContent = ''; return; }
    if (dr.from === dr.lo && dr.to === dr.hi) {
      el.textContent = 'Showing all kickoff dates in this report (' + dr.lo + ' to ' + dr.hi + ', UTC calendar dates).';
    } else {
      el.textContent = 'Showing fixtures kicking off ' + dr.from + ' through ' + dr.to + ' (UTC dates, inclusive).';
    }
  }

  function clearPresetActive() {
    document.querySelectorAll('.date-filter-tile .date-preset-btn').forEach(function (b) { b.classList.remove('is-active'); });
  }

  function wireDateFilter() {
    var fromEl = document.getElementById('mg-date-from');
    var toEl = document.getElementById('mg-date-to');
    if (!fromEl || !toEl || !dataDateMin || !dataDateMax) return;
    fromEl.addEventListener('change', function () { clearPresetActive(); applyFilter(); });
    toEl.addEventListener('change', function () { clearPresetActive(); applyFilter(); });
    document.querySelectorAll('.date-filter-tile .date-preset-btn').forEach(function (btn) {
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
    if (!mgTable || !mgTable._excelColSelections) return true;
    var selMap = mgTable._excelColSelections;
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

  function distinctMgColumn(activeCol) {
    var selected = getSelectedComps();
    var dr = getDateRange();
    var selMap = mgTable && mgTable._excelColSelections ? mgTable._excelColSelections : {};
    var s = new Set();
    tbody.querySelectorAll('tr.mg-data-row').forEach(function (tr) {
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

  function updateMgFilterButtons() {
    if (!mgTable) return;
    document.querySelectorAll('#mg-table .excel-filter-btn').forEach(function (btn) {
      var c = String(btn.getAttribute('data-col'));
      var sel = mgTable._excelColSelections[c];
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
    tbody.querySelectorAll('tr.mg-data-row').forEach(function (tr) {
      if (compMatches(tr, selected) && dateMatches(tr, dr) && colFiltersMatch(tr)) tr.classList.remove('mg-row--hidden');
      else tr.classList.add('mg-row--hidden');
    });
    renumberRows();
    var ag = aggMasterVisible();
    setText('kpi-mg-games', fmtInt(ag.games));
    setText('kpi-mg-teams', fmtInt(ag.teams));
    setText('kpi-mg-leagues', fmtInt(ag.leagues));
    setText('kpi-mg-completed', fmtInt(ag.completed));
    setText('kpi-mg-upcoming', fmtInt(ag.upcoming));

    var selArr = selected === null ? allNames : selected;
    var scopeEl = document.getElementById('mg-subtitle-scope');
    if (scopeEl) {
      if (!selArr.length) scopeEl.textContent = '— no competitions selected';
      else if (selArr.length === allNames.length) scopeEl.textContent = '· all competitions';
      else scopeEl.textContent = '· filtered to ' + selArr.length + ' competition' + (selArr.length === 1 ? '' : 's');
    }
    var ttitle = document.getElementById('mg-table-filter-title');
    if (ttitle) {
      if (!selArr.length) ttitle.textContent = 'All fixtures — (none selected)';
      else if (selArr.length === allNames.length) ttitle.textContent = 'All fixtures';
      else ttitle.textContent = 'All fixtures — ' + selArr.join(', ');
    }
    updateDateHint(dr);
  }

  var allBtn = document.getElementById('mg-slicer-all');
  var noneBtn = document.getElementById('mg-slicer-none');
  if (allBtn) {
    allBtn.addEventListener('click', function () {
      document.querySelectorAll('input[name="mg-comp"]').forEach(function (cb) { cb.checked = true; });
      applyFilter();
    });
  }
  if (noneBtn) {
    noneBtn.addEventListener('click', function () {
      document.querySelectorAll('input[name="mg-comp"]').forEach(function (cb) { cb.checked = false; });
      applyFilter();
    });
  }
  document.querySelectorAll('input[name="mg-comp"]').forEach(function (cb) {
    cb.addEventListener('change', applyFilter);
  });

  if (mgTable) {
    document.querySelectorAll('#mg-table .excel-filter-btn').forEach(function (btn) {
      btn.addEventListener('click', function (ev) {
        ev.stopPropagation();
        var col = parseInt(btn.getAttribute('data-col'), 10);
        var dist = distinctMgColumn(col);
        var key = String(col);
        var cur = mgTable._excelColSelections[key];
        var initial = cur == null ? null : new Set(cur);
        if (!window.ReportExcelFilter) return;
        window.ReportExcelFilter.open({
          anchor: btn,
          title: 'Filter column',
          distinctSnapshot: dist,
          selectedSet: initial,
          onApply: function (set) {
            if (set == null) delete mgTable._excelColSelections[key];
            else if (set.size === 0) mgTable._excelColSelections[key] = new Set();
            else mgTable._excelColSelections[key] = set;
            updateMgFilterButtons();
            applyFilter();
          }
        });
      });
    });
    updateMgFilterButtons();
  }
  wireDateFilter();
  applyFilter();
})();
</script>"""


def generate_master_games_html(rows: list[dict]) -> str:
    from generate_report import EXCEL_FILTER_CORE_SCRIPT, EXCEL_FILTER_CSS, load_svg

    generated = datetime.now(timezone.utc).strftime("%d %B %Y, %H:%M UTC")
    total = len(rows)
    ng, nt, nl, nc = master_dataset_stats(rows)
    logo_svg = load_svg("lanes_sportsdata.svg")

    all_season_names = sorted(
        {(r.get("season_name") or "").strip() for r in rows if (r.get("season_name") or "").strip()}
    )
    competition_tile = build_competition_slicer_mg(all_season_names)
    dmin, dmax = date_bounds_master(rows)
    date_tile = build_date_filter_tile_mg(dmin, dmax)
    filter_payload = {
        "allSeasonNames": all_season_names,
        "dataDateMin": dmin,
        "dataDateMax": dmax,
    }
    filter_json = json.dumps(filter_payload, ensure_ascii=False)

    stats_cards_html = f"""
          <div class="mg-kpi-row-games">
          <div class="stat-card">
            <div class="stat-number" id="kpi-mg-games">{ng:,}</div>
            <div class="stat-label">Total # of Games</div>
          </div>
          <div class="stat-card">
            <div class="stat-number" id="kpi-mg-completed">{nc:,}</div>
            <div class="stat-label"># of Games Played</div>
          </div>
          <div class="stat-card">
            <div class="stat-number" id="kpi-mg-upcoming">{total - nc:,}</div>
            <div class="stat-label"># of Games still to go</div>
          </div>
          </div>
          <div class="stat-card">
            <div class="stat-number" id="kpi-mg-teams">{nt:,}</div>
            <div class="stat-label">Total # of Teams</div>
          </div>
          <div class="stat-card">
            <div class="stat-number" id="kpi-mg-leagues">{nl:,}</div>
            <div class="stat-label">Competition seasons</div>
          </div>"""

    stats_html = (
        '<div class="stats-grid">\n'
        + competition_tile
        + "\n"
        + date_tile
        + "\n"
        + stats_cards_html
        + "\n</div>"
    )

    table_rows = build_master_table_rows(rows)

    logo_section = ""
    if logo_svg:
        logo_section = f"""
  <div class="branding">
    <span class="branding-label">Brought to you by</span>
    <div class="branding-logo">{logo_svg}</div>
  </div>"""

    filter_btns = ['<th class="num"></th>']
    for col in range(1, 12):
        filter_btns.append(
            f'<th><button type="button" class="excel-filter-btn" data-table="mg-table" data-col="{col}" '
            f'title="Pick values like Excel">Values…</button></th>'
        )
    filter_row = "\n".join(filter_btns)

    nav = navigation_html("report_master_games.html")

    body_main = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Tier 1 Soccer Leagues — List of All Games</title>
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
    .mg-row--hidden {{ display: none !important; }}
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
    .header h1 {{ font-size: clamp(1.05rem, 2.8vw, 1.85rem); font-weight: 800; color: #fff; letter-spacing: -0.02em; line-height: 1.25; max-width: 52rem; margin-left: auto; margin-right: auto; }}
    .header h1 .mg-head-accent {{ color: #ff9966; }}
    .header .subtitle {{ margin-top: 0.5rem; font-size: 0.95rem; color: #ccc; }}
    .header .header-lead {{ margin: 0; }}
    .header .header-desc {{
      margin: 0.55rem auto 0;
      max-width: 44rem;
      font-size: 0.88rem;
      line-height: 1.45;
      color: #c8c8c8;
    }}
    .header .header-filter-hint {{ font-weight: 400; color: #aaa; }}
    .stat-card--wide {{ grid-column: 1 / -1; }}
    .stat-card--text-left {{ text-align: left; }}
    .stat-card--filter-tile {{ padding: 1rem 1.1rem; }}
    .competition-slicer--tile {{ margin: 0; padding: 0; max-width: none; background: transparent; border: none; text-align: left; }}
    .stat-card--filter-tile .slicer-head {{ display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 0.5rem; margin-bottom: 0.6rem; }}
    .stat-card--filter-tile .slicer-title {{ font-size: 0.72rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; color: #cc0000; }}
    .stat-card--filter-tile .slicer-actions {{ display: inline-flex; gap: 0.35rem; }}
    .stat-card--filter-tile .slicer-btn {{
      font: inherit; cursor: pointer; font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
      letter-spacing: 0.06em; padding: 0.3rem 0.55rem; border-radius: 6px; border: 1px solid #ddd8d0;
      background: #faf8f5; color: #333;
    }}
    .stat-card--filter-tile .slicer-btn:hover {{ border-color: #cc0000; background: #fff0f0; color: #990000; }}
    .stat-card--filter-tile .slicer-chips {{ display: flex; flex-wrap: wrap; gap: 0.45rem 0.65rem; justify-content: flex-start; }}
    .stat-card--filter-tile .slicer-chip {{ display: inline-flex; align-items: center; gap: 0.35rem; font-size: 0.78rem; color: #333; cursor: pointer; user-select: none; }}
    .stat-card--filter-tile .slicer-chip input {{ accent-color: #cc0000; width: 1rem; height: 1rem; }}
    .date-filter-tile {{ text-align: left; }}
    .date-filter-head {{ display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 0.6rem; margin-bottom: 0.65rem; }}
    .date-filter-title {{ font-size: 0.72rem; font-weight: 700; color: #888; text-transform: uppercase; letter-spacing: 0.08em; }}
    .date-filter-presets {{ display: flex; flex-wrap: wrap; gap: 0.35rem; }}
    .date-preset-btn {{ font: inherit; cursor: pointer; font-size: 0.72rem; font-weight: 600; padding: 0.35rem 0.6rem; border-radius: 6px; border: 1px solid #ddd8d0; background: #faf8f5; color: #333; }}
    .date-preset-btn:hover {{ border-color: #cc0000; color: #cc0000; }}
    .date-preset-btn.is-active {{ border-color: #cc0000; background: #fff0f0; color: #990000; }}
    .date-filter-custom {{ display: flex; flex-wrap: wrap; align-items: flex-end; gap: 1rem; }}
    .date-filter-label {{ display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.72rem; font-weight: 600; color: #555; }}
    .date-filter-label input[type="date"] {{ font: inherit; padding: 0.35rem 0.5rem; border: 1px solid #ccc; border-radius: 6px; background: #fff; color: #111; }}
    .date-filter-hint {{ margin-top: 0.55rem; font-size: 0.78rem; color: #666; line-height: 1.35; }}
    .date-filter-muted {{ font-size: 0.82rem; color: #888; margin-top: 0.35rem; }}
    .stats-section {{ max-width: 1100px; margin: 2rem auto 0; padding: 0 1rem; }}
    .stats-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.75rem; align-items: start; }}
    .mg-kpi-row-games {{
      grid-column: 1 / -1;
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 0.75rem;
      align-items: stretch;
    }}
    .stat-card {{
      background: #ffffff; border: 1px solid #ddd8d0; border-radius: 10px; padding: 1rem 0.75rem; text-align: center;
      transition: border-color 0.2s, box-shadow 0.2s; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}
    .stat-card:hover {{ border-color: #cc0000; box-shadow: 0 2px 8px rgba(204,0,0,0.12); }}
    .stat-number {{ font-size: 1.75rem; font-weight: 800; color: #cc0000; line-height: 1; }}
    .stat-label {{ margin-top: 0.35rem; font-size: 0.72rem; color: #888; text-transform: uppercase; letter-spacing: 0.06em; }}
    .table-section {{ max-width: 100%; margin: 1.75rem auto 0; padding: 0 1rem; }}
    .table-header-row {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.6rem; flex-wrap: wrap; gap: 0.4rem; }}
    .table-title {{ font-size: 0.85rem; font-weight: 700; color: #cc0000; text-transform: uppercase; letter-spacing: 0.08em; }}
    .sort-hint {{ font-size: 0.72rem; color: #999; max-width: 38rem; text-align: right; line-height: 1.35; }}
    .table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; border-radius: 8px; border: 1px solid #ddd8d0; box-shadow: 0 1px 4px rgba(0,0,0,0.07); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.76rem; table-layout: fixed; }}
    thead tr:first-child {{ background: #1a0000; border-bottom: 2px solid #cc0000; }}
    thead tr:first-child th {{
      padding: 0.55rem 0.35rem; text-align: left; font-size: 0.65rem; font-weight: 700; text-transform: uppercase;
      letter-spacing: 0.05em; color: #ffaaaa; white-space: nowrap; overflow: hidden; cursor: pointer; user-select: none;
    }}
    thead tr:first-child th:hover {{ background: #2e0000; color: #ffd0d0; }}
    thead tr:first-child th.sorted-asc::after {{ content: " ▲"; font-size: 0.55rem; color: #ff8888; }}
    thead tr:first-child th.sorted-desc::after {{ content: " ▼"; font-size: 0.55rem; color: #ff8888; }}
    thead tr:first-child th.num {{ text-align: center; cursor: default; }}
    thead tr.og-col-filters th {{
      background: #2a1515; cursor: default; padding: 0.28rem 0.3rem; border-bottom: 2px solid #cc0000;
      text-transform: none; font-weight: 400; letter-spacing: normal;
    }}
    tbody tr {{ background: #ffffff; transition: background 0.12s; }}
    tbody tr:nth-child(even) {{ background: #faf7f2; }}
    tbody tr:hover {{ background: #fff0f0; }}
    td {{
      padding: 0.38rem 0.35rem; text-align: center; vertical-align: middle; border-bottom: 1px solid #e8e2d8;
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    }}
    td.num {{ text-align: center; color: #bbb; font-size: 0.74rem; }}
    td.score {{ font-variant-numeric: tabular-nums; font-weight: 600; color: #444; }}
    .match-name {{ font-weight: 600; color: #111; text-align: center; white-space: normal; overflow: visible; }}
    .meta {{ font-size: 0.66rem; color: #999; margin-top: 0.08rem; text-align: center; white-space: normal; }}
    .id-cell code {{
      font-family: 'Cascadia Code', 'Consolas', 'Courier New', monospace; font-size: 0.64rem; color: #555;
      background: #f0ede8; border: 1px solid #ddd8d0; border-radius: 3px; padding: 0.12rem 0.28rem;
      display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }}
    .match-id-cell code {{ font-size: 0.59rem; }}
    td.recorded-true {{ font-weight: 700; color: #1a5c1a; }}
    td.recorded-false {{ font-weight: 600; color: #666; }}
    td.recorded-unknown {{ color: #999; }}
    .branding {{ display: flex; align-items: center; justify-content: center; gap: 1rem; margin-top: 2.5rem; padding: 1.25rem 1rem; border-top: 1px solid #ddd8d0; flex-wrap: wrap; }}
    .branding-label {{ font-size: 0.72rem; color: #aaa; text-transform: uppercase; letter-spacing: 0.1em; white-space: nowrap; }}
    .branding-logo {{ height: 120px; display: flex; align-items: center; }}
    .branding-logo svg {{ height: 120px; width: auto; }}
    .footer {{ text-align: center; margin-top: 1rem; color: #aaa; font-size: 0.72rem; }}
    @media (max-width: 900px) {{ .stats-grid {{ grid-template-columns: repeat(3, 1fr); }} }}
    @media (max-width: 720px) {{ .mg-kpi-row-games {{ grid-template-columns: 1fr; }} }}
    @media (max-width: 599px) {{ .stats-grid {{ grid-template-columns: repeat(2, 1fr); }} html {{ font-size: 13px; }} }}
    {EXCEL_FILTER_CSS}
    {NAV_CSS}
  </style>
</head>
<body>
  <div class="report-sticky-top">
  <div class="header">
    <div class="header-badge">Sportradar Soccer</div>
    <h1>Tier 1 Soccer Leagues <span class="mg-head-accent">// List of All Games</span></h1>
    <div class="subtitle">
      <p class="header-lead"><strong>{total:,}</strong> fixtures <span id="mg-subtitle-scope" class="header-filter-hint">· all competitions</span></p>
      <p class="header-desc">{html.escape(REPORT_BLURB_LIST_OF_ALL_GAMES, quote=False)}</p>
    </div>
  </div>
{nav}
  </div>

  <div class="stats-section">
    {stats_html}
  </div>

  <div class="table-section">
    <div class="table-header-row">
      <div class="table-title" id="mg-table-filter-title">All fixtures</div>
      <div class="sort-hint">Click a column header to sort. Use <strong>Values…</strong> for Excel-style filters (combine with competition + date tiles).</div>
    </div>
    <div class="table-wrap">
      <table id="mg-table">
        <thead>
          <tr>
            <th class="num">#</th>
            <th data-col="1">sr_sport_event_id</th>
            <th data-col="2">recording_id</th>
            <th data-col="3">Title</th>
            <th data-col="4">Competition Name</th>
            <th data-col="5">Sport Event Start</th>
            <th data-col="6">Round</th>
            <th data-col="7">Home</th>
            <th data-col="8">Away</th>
            <th data-col="9">Final Score</th>
            <th data-col="10">Sport Event Status</th>
            <th data-col="11">Match Status</th>
          </tr>
          <tr class="og-col-filters">
            {filter_row}
          </tr>
        </thead>
        <tbody id="mg-tbody">
          {table_rows}
        </tbody>
      </table>
    </div>
  </div>

  {logo_section}

  <div class="footer">
    <p>Data from Supabase <code>All Games (sr:sport_events)</code> · Generated {html.escape(generated, quote=False)}</p>
  </div>

  <script type="application/json" id="report-filter-data-mg">{filter_json}</script>
"""

    return body_main + EXCEL_FILTER_CORE_SCRIPT + _inline_master_games_script() + "\n</body>\n</html>"


def write_master_games_report() -> None:
    """Write REPORT_HTML_MASTER_GAMES from Supabase or a stub."""

    if not USE_SUPABASE:
        nav = navigation_html("report_master_games.html")
        stub = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>Tier 1 Soccer Leagues — List of All Games</title>
<style>body{{font-family:system-ui;padding:2rem;background:#f5f2ec;}}{NAV_CSS}</style></head>
<body><div class="report-sticky-top">{nav}</div>
<p>Set Supabase credentials to build this report from <code>All Games</code>.</p></body></html>"""
        with open(REPORT_HTML_MASTER_GAMES, "w", encoding="utf-8") as f:
            f.write(stub)
        print(f"  Wrote stub: {REPORT_HTML_MASTER_GAMES}")
        return

    import db

    rows = db.get_all_master_games_report_rows()
    rows.sort(key=lambda r: (r.get("match_date") or "", r.get("sport_event_id") or ""))
    doc = generate_master_games_html(rows)

    with open(REPORT_HTML_MASTER_GAMES, "w", encoding="utf-8") as f:
        f.write(doc)
    print(f"  Wrote {REPORT_HTML_MASTER_GAMES} ({len(rows)} rows)")
