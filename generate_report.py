"""
STEP 5 — Generate the EPL Own Goals HTML report.

Reads data/own_goals.csv and produces a self-contained report.html.
Can be re-run at any time to refresh the report from the latest CSV.
"""

import csv
import html
import json
import os
from datetime import datetime, timezone

from config import OWN_GOALS_CSV, REPORT_HTML, SEASON_NAME, SEASON_LABEL, TIMELINES_DIR, USE_SUPABASE

ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")


def count_completed_matches() -> int:
    """Number of cached timeline files = completed matches reviewed."""
    if not os.path.isdir(TIMELINES_DIR):
        return 0
    return sum(1 for f in os.listdir(TIMELINES_DIR) if f.endswith(".json"))


def count_timeline_events() -> int:
    """Total number of individual timeline events across all cached matches."""
    if not os.path.isdir(TIMELINES_DIR):
        return 0
    total = 0
    for filename in os.listdir(TIMELINES_DIR):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(TIMELINES_DIR, filename)
        try:
            with open(filepath, encoding="utf-8") as f:
                data = json.load(f)
            total += len(data.get("timeline", []))
        except Exception:
            pass
    return total


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


def build_table_rows(rows: list[dict]) -> str:
    if not rows:
        return '<tr><td colspan="12" style="text-align:center;padding:2rem;color:#666;">No own goals found yet.</td></tr>'

    html_rows = []
    for i, r in enumerate(rows, 1):
        minute_display = format_minute(r["minute"], r["stoppage_time"])
        score_at_og = format_score(r["home_score_after"], r["away_score_after"])
        final_score = format_score(r["final_home_score"], r["final_away_score"])
        match_label = f"{r['home_team']} vs {r['away_team']}"
        round_label = f"GW{r['round']}" if r.get("round") else "—"
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

        comp = (r.get("season_name") or "").strip()
        comp_attr = html.escape(comp, quote=True)
        ev_attr = html.escape(str(r.get("sport_event_id", "")), quote=True)
        pl_attr = html.escape(str(r.get("og_player", "")), quote=True)
        tm_attr = html.escape(str(r.get("og_player_team", "")), quote=True)
        match_date_raw = (r.get("match_date") or "")[:10]
        date_attr = html.escape(match_date_raw if len(match_date_raw) == 10 else "", quote=True)
        html_rows.append(f"""
        <tr class="og-data-row" data-competition="{comp_attr}" data-match-date="{date_attr}" data-event-id="{ev_attr}" data-og-player="{pl_attr}" data-og-team="{tm_attr}">
          <td class="num" data-val="{i}" data-label="#">{i}</td>
          <td data-val="{r.get('season_name','')}" data-label="Competition">
            <div class="match-name">{r.get('season_name','—')}</div>
            <div class="meta">{r.get('competition_id','')}</div>
          </td>
          <td data-val="{r['match_date']}" data-label="Match">
            <div class="match-name">{match_label}</div>
            <div class="meta">{r['match_date']} &bull; {round_label}</div>
          </td>
          <td class="id-cell match-id-cell" data-val="{r['sport_event_id']}" data-label="Match ID"><code title="{r['sport_event_id']}">{r['sport_event_id']}</code></td>
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
  const filterPayload = (function () {
    const el = document.getElementById('report-filter-data');
    if (!el) return {};
    try { return JSON.parse(el.textContent); } catch (e) { return {}; }
  })();

  const allNames = filterPayload.allSeasonNames || [];
  const pb = filterPayload.pipelineBySeason || {};
  const pg = filterPayload.pipelineGlobal || { matches: 0, events: 0 };
  const scopeDefault = filterPayload.scopeLabelDefault || '';
  const dataDateMin = filterPayload.dataDateMin || '';
  const dataDateMax = filterPayload.dataDateMax || '';

  const headers = document.querySelectorAll('thead th[data-col]');
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
      const aNum = parseFloat(aVal);
      const bNum = parseFloat(bVal);
      let cmp;
      if (!isNaN(aNum) && !isNaN(bNum) && aVal !== '' && bVal !== '') {
        cmp = aNum - bNum;
      } else {
        cmp = aVal.localeCompare(bVal, undefined, { numeric: true, sensitivity: 'base' });
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
      return { matches: pg.matches || 0, events: pg.events || 0 };
    }
    if (selected.length === 0) return { matches: 0, events: 0 };
    let m = 0;
    let e = 0;
    selected.forEach(function (sn) {
      const row = pb[sn];
      if (row) {
        m += row.matches_reviewed || 0;
        e += row.timeline_events || 0;
      }
    });
    return { matches: m, events: e };
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
    visible.forEach(function (tr) {
      const evId = tr.getAttribute('data-event-id') || '';
      if (evId) eventsSeen[evId] = true;
      const pl = tr.getAttribute('data-og-player') || '';
      const tm = tr.getAttribute('data-og-team') || '';
      if (pl) pc[pl] = (pc[pl] || 0) + 1;
      if (tm) tc[tm] = (tc[tm] || 0) + 1;
      const rowSays = tr.querySelector('td.og-yes, td.og-no');
      if (rowSays && rowSays.classList.contains('og-yes')) corr += 1;
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
      incorr: total - corr
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

  function applyFilter() {
    var selected = getSelectedComps();
    var dr = getDateRange();
    tbody.querySelectorAll('tr.og-data-row').forEach(function (tr) {
      if (compMatches(tr, selected) && dateMatches(tr, dr)) tr.classList.remove('og-row--hidden');
      else tr.classList.add('og-row--hidden');
    });
    renumberRows();

    var pipeKeys = selected === null ? (allNames.length ? allNames : Object.keys(pb)) : selected;
    // Summing pipelineBySeason misses timelines that do not map to a configured season / closed game
    // (or empty JSON rows still counted globally). When every comp chip is on, match the printed totals.
    var st = allCompetitionChipsSelected()
      ? { matches: pg.matches || 0, events: pg.events || 0 }
      : pipelineStats(pipeKeys);
    setText('kpi-matches', fmtInt(st.matches));
    setText('kpi-events', fmtInt(st.events));

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
    var scopeEl = document.getElementById('subtitle-scope');
    if (scopeEl) {
      if (!selArr.length) scopeEl.textContent = '— no competitions selected';
      else if (selArr.length === allNames.length) scopeEl.textContent = 'scored across ' + scopeDefault;
      else scopeEl.textContent = 'across ' + selArr.length + ' selected competition' + (selArr.length === 1 ? '' : 's');
    }
    var ttitle = document.getElementById('table-filter-title');
    if (ttitle) {
      if (!selArr.length) ttitle.textContent = 'All Own Goals — (none selected)';
      else if (selArr.length === allNames.length) ttitle.textContent = 'All Own Goals — ' + scopeDefault;
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
  wireDateFilter();
  applyFilter();
})();
</script>"""


def generate_html(
    rows: list[dict],
    completed_matches: int,
    timeline_events: int,
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
        "pipelineGlobal": {"matches": completed_matches, "events": timeline_events},
        "allSeasonNames": all_season_names,
        "scopeLabelDefault": SEASON_LABEL,
        "dataDateMin": data_date_min,
        "dataDateMax": data_date_max,
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

        events_display = f"{timeline_events:,}"

        commentary_correct = sum(1 for r in rows if "own goal" in r.get("commentary", "").lower())
        commentary_incorrect = total - commentary_correct
        games_with_og = len(set(r["sport_event_id"] for r in rows))

        stats_cards_html = f"""
          <div class="stat-card">
            <div class="stat-number" id="kpi-matches">{completed_matches:,}</div>
            <div class="stat-label">Matches Reviewed</div>
          </div>
          <div class="stat-card">
            <div class="stat-number" id="kpi-events">{events_display}</div>
            <div class="stat-label">Timeline Events Reviewed</div>
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
            <div class="stat-label">Commentary Correct<br><span class="stat-sub stat-sub--muted">Mentions "own goal"</span></div>
          </div>
          <div class="stat-card stat-card--incorrect">
            <div class="stat-number stat-number--incorrect" id="kpi-commentary-no">{commentary_incorrect}</div>
            <div class="stat-label">Commentary Incorrect<br><span class="stat-sub stat-sub--muted">No "own goal" mention</span></div>
          </div>"""
    else:
        stats_cards_html = ""

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

    .og-row--hidden {{ display: none !important; }}

    .stat-card--wide {{ grid-column: 1 / -1; }}
    .stat-card--text-left {{ text-align: left; }}
    .stat-card--filter-tile {{ padding: 1rem 1.1rem; }}

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
    col.c-comp       {{ width: 12%; }}
    col.c-match      {{ width: 12%; }}
    col.c-matchid    {{ width: 12%; }}
    col.c-scorer     {{ width: 10%; }}
    col.c-playerid   {{ width: 8%; }}
    col.c-min        {{ width: 4%; }}
    col.c-team       {{ width: 9%; }}
    col.c-score      {{ width: 5%; }}
    col.c-final      {{ width: 5%; }}
    col.c-ogmention  {{ width: 5%; }}
    col.c-commentary {{ width: 25%; }}

    thead tr {{
      background: #1a0000;
      border-bottom: 2px solid #cc0000;
    }}
    thead th {{
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
    thead th:hover {{ background: #2e0000; color: #ffd0d0; }}
    thead th.sorted-asc::after  {{ content: " ▲"; font-size: 0.6rem; color: #ff8888; }}
    thead th.sorted-desc::after {{ content: " ▼"; font-size: 0.6rem; color: #ff8888; }}
    thead th.num {{ text-align: center; cursor: default; }}
    thead th.center {{ text-align: center; }}

    tbody tr {{ background: #ffffff; transition: background 0.12s; }}
    tbody tr:nth-child(even) {{ background: #faf7f2; }}
    tbody tr:hover {{ background: #fff0f0; }}

    td {{
      padding: 0.45rem 0.4rem;
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
      white-space: normal;
      overflow: visible;
    }}
    .player-name {{
      font-weight: 600;
      color: #222;
      white-space: normal;
      overflow: visible;
    }}
    .meta {{
      font-size: 0.68rem;
      color: #999;
      margin-top: 0.1rem;
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
  </style>
</head>
<body>

  <div class="header">
    <div class="header-badge">Sportradar Soccer</div>
    <h1><span>Own Goals</span> Tracker</h1>
    <div class="subtitle">
      <strong id="subtitle-og-total">{total}</strong> own goal<span id="subtitle-og-plural">{"s" if total != 1 else ""}</span> <span id="subtitle-scope">scored across {SEASON_LABEL}</span>
    </div>
  </div>

  <div class="stats-section">
    {stats_html}
  </div>

  <div class="table-section">
    <div class="table-header-row">
      <div class="table-title" id="table-filter-title">All Own Goals &mdash; {SEASON_LABEL}</div>
      <div class="sort-hint">Click any column header to sort</div>
    </div>
    <div class="table-wrap">
      <table id="og-table">
        <colgroup>
          <col class="c-num">
          <col class="c-comp">
          <col class="c-match">
          <col class="c-matchid">
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
            <th data-col="2">Match</th>
            <th data-col="3">Match ID</th>
            <th data-col="4">Own Goal Scorer</th>
            <th data-col="5">Player ID</th>
            <th class="center" data-col="6">Min</th>
            <th data-col="7">Benefiting Team</th>
            <th class="center" data-col="8">Score at OG</th>
            <th class="center" data-col="9">Final Score</th>
            <th class="center" data-col="10">Mentions OG?</th>
            <th data-col="11">Commentary</th>
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

  <script type="application/json" id="report-filter-data">{filter_json}</script>
  {_inline_report_script()}

</body>
</html>"""


def main():
    pipeline_by_season: dict = {}
    if USE_SUPABASE:
        import db
        rows = db.get_all_own_goals()
        completed_matches, timeline_events = db.get_report_stats()
        pipeline_by_season = db.get_pipeline_stats_by_season_name()
        rows.sort(key=lambda r: (r["match_date"], int(r["minute"]) if str(r["minute"]).isdigit() else 0))
        print(f"Loaded {len(rows)} own goal records from Supabase")
    else:
        rows = load_own_goals(OWN_GOALS_CSV)
        rows.sort(key=lambda r: (r["match_date"], int(r["minute"]) if str(r["minute"]).isdigit() else 0))
        completed_matches = count_completed_matches()
        timeline_events = count_timeline_events()
        print(f"Loaded {len(rows)} own goal records from {OWN_GOALS_CSV}")
    print(f"Completed matches reviewed : {completed_matches}")
    print(f"Total timeline events      : {timeline_events:,}")
    html = generate_html(rows, completed_matches, timeline_events, pipeline_by_season)
    with open(REPORT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Report written to: {REPORT_HTML}")
    print(f"Open it in your browser to view.")


if __name__ == "__main__":
    main()
