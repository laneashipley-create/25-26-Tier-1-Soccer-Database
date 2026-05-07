from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from report_navigation import NAV_CSS, navigation_html

CSV_PATH = ROOT / "wc2026_schedule.csv"
CSV_FALLBACK = ROOT / "wc2026_schedule.with_groups.csv"
HTML_PATH = ROOT / "wc2026_schedule.html"


def resolve_csv_path() -> pathlib.Path:
    if CSV_PATH.exists():
        header_line = CSV_PATH.read_text(encoding="utf-8").splitlines()[:1]
        if header_line and "stage" in header_line[0]:
            return CSV_PATH
    if CSV_FALLBACK.exists():
        return CSV_FALLBACK
    return CSV_PATH


def main() -> None:
    csv_path = resolve_csv_path()
    csv_text = csv_path.read_text(encoding="utf-8").replace("`", "\\`")
    nav_block = navigation_html("wc2026_schedule.html")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>FIFA World Cup 2026 Schedule — Lane&apos;s SportsData</title>
  <style>
""" + NAV_CSS + f"""
    *, *::before, *::after {{ box-sizing: border-box; }}
    html {{ font-size: 14px; }}
    html, body {{ margin: 0; padding: 0; }}
    body {{
      font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
      color: #1a1a2a;
      background: #f5f2ec;
      min-height: 100vh;
      padding-bottom: 3rem;
    }}
    .wc-wrap {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 0 1rem;
    }}
    .wc-hero.header {{
      background: linear-gradient(160deg, #1a0000 0%, #2e0000 45%, #1a0000 100%);
      border-bottom: 3px solid #cc0000;
      padding: 1.75rem 1.25rem 1.5rem;
      text-align: center;
      margin: 0 -1rem 1.25rem;
      padding-left: 1rem;
      padding-right: 1rem;
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
    .wc-hero h1 {{
      margin: 0;
      font-size: clamp(1.5rem, 4vw, 2rem);
      font-weight: 800;
      color: #fff;
      letter-spacing: -0.02em;
    }}
    .wc-hero .wc-accent {{ color: #ff9966; }}
    .wc-hero .subtitle {{
      margin-top: 0.5rem;
      font-size: 0.95rem;
      color: #ccc;
    }}
    .wc-hero .header-lead {{ margin: 0; }}
    .wc-hero .header-lead strong {{ color: #fff; }}
    .wc-hero .header-desc {{
      margin: 0.5rem auto 0;
      max-width: 40rem;
      font-size: 0.88rem;
      line-height: 1.45;
      color: #c8c8c8;
    }}
    .wc-hero-tools {{
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.65rem;
      flex-wrap: wrap;
      margin-top: 1rem;
    }}
    .wc-meta {{ font-size: 0.82rem; color: #aaa; }}
    .wc-live {{ font-size: 0.74rem; font-weight: 700; color: #ffcccc; }}
    .btn {{
      font: inherit;
      cursor: pointer;
      border: 1px solid #ddd8d0;
      background: #faf8f5;
      color: #333;
      border-radius: 8px;
      padding: 0.4rem 0.75rem;
      font-size: 0.78rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }}
    .btn:hover {{ border-color: #cc0000; background: #fff0f0; color: #990000; }}
    .filters {{
      display: flex;
      flex-direction: column;
      gap: 0.65rem;
      margin-bottom: 1.25rem;
    }}
    .filters-row-top {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0.65rem;
      align-items: stretch;
    }}
    .filters-row-top .filter-card {{ min-width: 0; }}
    .filters-row-top .group-grid {{
      grid-template-columns: repeat(3, minmax(0, 1fr));
      max-width: none;
      margin: 0;
    }}
    .filter-card {{
      border: 1px solid #ddd8d0;
      background: #ffffff;
      border-radius: 10px;
      padding: 0.85rem 0.9rem;
      box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}
    .section-head {{
      display: flex;
      justify-content: flex-start;
      align-items: center;
      gap: 8px;
      margin-bottom: 0.5rem;
    }}
    .section-title {{
      color: #cc0000;
      font-size: 0.72rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .toggle-arrow {{
      border: 1px solid #ccc4bc;
      background: #faf8f5;
      color: #333;
      border-radius: 6px;
      width: 32px;
      height: 28px;
      font-size: 0.95rem;
      line-height: 1;
      font-weight: 800;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 0;
    }}
    .toggle-arrow:hover {{ border-color: #cc0000; color: #990000; }}
    .section-body.hidden {{ display: none; }}
    .group-filters {{ margin-bottom: 6px; }}
    .filter-top {{ display: flex; justify-content: space-between; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }}
    .summary {{ color: #444; font-size: 0.88rem; min-height: 1.2rem; }}
    .pill-grid {{ display: flex; flex-wrap: wrap; gap: 6px; max-height: 170px; overflow-y: auto; }}
    .group-grid {{
      display: grid;
      grid-template-columns: repeat(6, minmax(110px, 1fr));
      gap: 6px;
      max-width: 860px;
      margin: 0 auto 6px;
      justify-content: center;
    }}
    .stage-pills {{ display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }}
    .stage-sub-wrap {{
      margin-top: 8px;
      padding-top: 8px;
      border-top: 1px dashed #cfc8bc;
    }}
    .stage-sub-wrap.hidden {{ display: none; }}
    .stage-sub-label {{
      font-size: 0.7rem;
      font-weight: 700;
      color: #888;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }}
    .pill {{
      border: 1px solid #ddd8d0;
      background: #faf8f5;
      color: #333;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 0.78rem;
      font-weight: 600;
      cursor: pointer;
    }}
    .pill span {{ color: #888; margin-left: 6px; font-size: 0.72rem; font-weight: 700; }}
    .pill.active {{
      background: #cc0000;
      border-color: #a30000;
      color: #fff;
      box-shadow: 0 2px 6px rgba(204,0,0,0.25);
    }}
    .pill.active span {{ color: #ffe0e0; }}
    .pill.related {{
      background: #fff0f0;
      border-color: #cc0000;
      color: #660000;
    }}
    .pill.related span {{ color: #993333; }}
    .timeline {{ padding-right: 4px; padding-bottom: 2rem; min-height: 40vh; }}
    .day-block {{ margin: 1rem 0 1.35rem; }}
    .day-header {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin: 0 0 8px;
      border-left: 4px solid #cc0000;
      padding: 8px 10px;
      position: sticky;
      top: 2.85rem;
      z-index: 50;
      background: linear-gradient(to bottom, rgba(245,242,236,.98), rgba(245,242,236,.93));
      box-shadow: 0 4px 12px rgba(0,0,0,.08);
    }}
    .day-title {{ font-size: 0.95rem; font-weight: 700; color: #1a1a2a; }}
    .day-count {{ color: #666; font-size: 0.82rem; }}
    .match-card {{
      display: grid;
      grid-template-columns: 220px 1fr 150px;
      gap: 10px;
      background: #ffffff;
      border: 1px solid #ddd8d0;
      border-radius: 10px;
      padding: 10px;
      margin-bottom: 8px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}
    .match-card:hover {{ border-color: #cc0000; box-shadow: 0 2px 8px rgba(204,0,0,0.1); }}
    .match-card.live {{ border-color: #cc0000; box-shadow: 0 0 0 2px rgba(204,0,0,.2); }}
    .match-card.completed {{ opacity: 0.82; }}
    .stage {{ color: #888; font-weight: 600; font-size: 0.8rem; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.04em; }}
    .kickoff {{ font-weight: 700; color: #1a1a2a; }}
    .venue-time {{ color: #666; font-size: 0.76rem; margin-top: 3px; }}
    .center {{ display: flex; align-items: center; justify-content: center; gap: 8px; text-align: center; }}
    .team {{ font-size: 1rem; font-weight: 800; color: #1a1a2a; }}
    .score, .sep {{ min-width: 56px; font-weight: 800; color: #cc0000; }}
    .right {{ text-align: right; color: #666; font-size: 0.78rem; align-self: center; }}
    .knockout {{
      margin: 1.35rem 0 1rem;
      color: #cc0000;
      font-weight: 700;
      font-size: 0.85rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      display: flex;
      gap: 10px;
      align-items: center;
    }}
    .knockout:before, .knockout:after {{
      content: "";
      flex: 1;
      height: 1px;
      background: linear-gradient(to right, transparent, #cc8888, transparent);
    }}
    .empty {{ padding: 1.75rem; color: #666; text-align: center; font-size: 0.9rem; }}
    @media (max-width: 900px) {{
      .match-card {{ grid-template-columns: 1fr; }}
      .center, .right {{ justify-content: flex-start; text-align: left; }}
      .group-grid {{ grid-template-columns: repeat(3, minmax(110px, 1fr)); }}
      .filters-row-top {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="report-sticky-top">
{nav_block}
  </div>

  <div class="wc-wrap">
    <header class="wc-hero header" aria-label="World Cup 2026 schedule header">
      <div class="header-badge">Sportradar Soccer</div>
      <h1>FIFA World Cup <span class="wc-accent">2026</span> <span class="wc-trophy" aria-hidden="true">🏆</span></h1>
      <div class="subtitle">
        <p class="header-lead">June 11 – July 19, 2026 · <strong>104</strong> matches · USA / Canada / Mexico</p>
        <p class="header-desc" id="tzNote">All times are shown in your local timezone.</p>
      </div>
      <div class="wc-hero-tools">
        <div id="liveBadge" class="wc-live"></div>
        <div id="lastUpdated" class="wc-meta">Last updated: --</div>
        <button id="refreshBtn" class="btn" type="button">Refresh</button>
      </div>
    </header>

    <div class="filters">
      <div class="filters-row-top">
        <div class="filter-card">
          <div class="section-head">
            <button id="toggleGroupsBtn" class="toggle-arrow" type="button" aria-label="Collapse Group Filter" aria-expanded="true">▾</button>
            <div class="section-title">Group Filter</div>
          </div>
          <div id="groupSection" class="section-body">
            <div class="filter-top group-filters">
              <div id="groupSummary" class="summary">Group filter: All groups</div>
              <button id="clearGroupBtn" class="btn" type="button">Clear Groups</button>
            </div>
            <div id="groupPills" class="group-grid"></div>
          </div>
        </div>

        <div class="filter-card">
          <div class="section-head">
            <button id="toggleStageBtn" class="toggle-arrow" type="button" aria-label="Collapse Stage Filter" aria-expanded="true">▾</button>
            <div class="section-title">Stage Filter</div>
          </div>
          <div id="stageSection" class="section-body">
            <div class="filter-top">
              <div id="stageSummary" class="summary">Stage: All stages</div>
            </div>
            <div id="stagePills" class="stage-pills"></div>
            <div id="stageSubWrap" class="stage-sub-wrap hidden">
              <div id="stageSubLabel" class="stage-sub-label">Knockout round</div>
              <div id="stageSubPills" class="stage-pills"></div>
            </div>
          </div>
        </div>
      </div>

      <div class="filter-card">
        <div class="section-head">
          <button id="toggleTeamsBtn" class="toggle-arrow" type="button" aria-label="Collapse Team Filter" aria-expanded="true">▾</button>
          <div class="section-title">Team Filter</div>
        </div>
        <div id="teamSection" class="section-body">
          <div class="filter-top">
            <div id="summary" class="summary"></div>
            <button id="clearBtn" class="btn" type="button">Clear All</button>
          </div>
          <div id="pills" class="pill-grid"></div>
        </div>
      </div>
    </div>

    <main id="timeline" class="timeline"></main>
  </div>

  <script>
    const RAW_CSV = `{csv_text}`;
    const GROUP_STAGE_END = "2026-06-28";
    const TOURNAMENT_END = "2026-07-19";
    const VIEWER_TZ = Intl.DateTimeFormat().resolvedOptions().timeZone || "Local Time";
    const selectedTeams = new Set();
    const selectedGroups = new Set();
    let selectedStage = "";
    let selectedKnockoutRound = "";

    function pageScrollY() {{
      return window.scrollY || document.documentElement.scrollTop || 0;
    }}
    function setPageScrollY(y) {{
      window.scrollTo(0, y);
    }}

    const dom = {{
      timeline: document.getElementById("timeline"),
      groupPills: document.getElementById("groupPills"),
      groupSummary: document.getElementById("groupSummary"),
      clearGroupBtn: document.getElementById("clearGroupBtn"),
      groupSection: document.getElementById("groupSection"),
      toggleGroupsBtn: document.getElementById("toggleGroupsBtn"),
      pills: document.getElementById("pills"),
      summary: document.getElementById("summary"),
      clearBtn: document.getElementById("clearBtn"),
      teamSection: document.getElementById("teamSection"),
      toggleTeamsBtn: document.getElementById("toggleTeamsBtn"),
      stageSection: document.getElementById("stageSection"),
      stagePills: document.getElementById("stagePills"),
      stageSummary: document.getElementById("stageSummary"),
      stageSubWrap: document.getElementById("stageSubWrap"),
      stageSubLabel: document.getElementById("stageSubLabel"),
      stageSubPills: document.getElementById("stageSubPills"),
      toggleStageBtn: document.getElementById("toggleStageBtn"),
      refreshBtn: document.getElementById("refreshBtn"),
      lastUpdated: document.getElementById("lastUpdated"),
      liveBadge: document.getElementById("liveBadge"),
      tzNote: document.getElementById("tzNote"),
    }};

    function parseCsv(text) {{
      const lines = (text || "").trim().split(/\\r?\\n/);
      if (lines.length < 2) return [];
      const headers = lines[0].split(",");
      const out = [];
      for (let i = 1; i < lines.length; i++) {{
        const cells = [];
        let cur = "", inQuotes = false;
        for (let j = 0; j < lines[i].length; j++) {{
          const ch = lines[i][j];
          if (ch === '"') {{
            if (inQuotes && lines[i][j + 1] === '"') {{ cur += '"'; j++; }}
            else inQuotes = !inQuotes;
          }} else if (ch === "," && !inQuotes) {{
            cells.push(cur); cur = "";
          }} else cur += ch;
        }}
        cells.push(cur);
        const row = {{}};
        headers.forEach((h, idx) => row[h.trim()] = (cells[idx] ?? "").trim());
        out.push(row);
      }}
      return out;
    }}

    function statusKind(m) {{
      const s = String(m.match_status || m.status || "").toLowerCase();
      if (s.includes("in_progress") || s.includes("live")) return "LIVE";
      if (s.includes("ended") || s.includes("closed") || s.includes("finished")) return "COMPLETED";
      return "SCHEDULED";
    }}

    function normalizedStage(m) {{
      const s = String(m.stage || "").trim().toLowerCase();
      if (s === "league" || s === "cup") return s;
      const d = (m.start_time || "").slice(0, 10);
      if (d && d <= GROUP_STAGE_END) return "league";
      return "cup";
    }}

    function knockoutRoundKey(m) {{
      if (m.stageNorm !== "cup") return null;
      const n = m.match_number;
      if (n <= 88) return "r32";
      if (n <= 96) return "r16";
      if (n <= 100) return "qf";
      if (n <= 102) return "sf";
      if (n === 103) return "3rd";
      return "final";
    }}

    function stageLabel(m, idx) {{
      const st = m.stageNorm;
      const day = (m.start_time || "").slice(0, 10);
      if (st === "league" || (st !== "cup" && day <= GROUP_STAGE_END)) {{
        if (m.round && m.group) return `Group ${{m.group}} · Matchday ${{m.round}}`;
        return "Group Stage";
      }}
      const n = idx + 1;
      if (n <= 72) return "Group Stage";
      if (n <= 88) return "Round of 32";
      if (n <= 96) return "Round of 16";
      if (n <= 100) return "Quarterfinal";
      if (n <= 102) return "Semifinal";
      if (n === 103) return "Third Place";
      return "Final";
    }}

    function localDayKey(iso) {{
      return new Intl.DateTimeFormat("en-CA", {{ year:"numeric", month:"2-digit", day:"2-digit" }}).format(new Date(iso));
    }}
    function localDayTitle(key) {{
      return new Intl.DateTimeFormat("en-US", {{ weekday:"long", month:"long", day:"numeric" }}).format(new Date(`${{key}}T12:00:00`));
    }}
    function localTime(iso) {{
      return new Intl.DateTimeFormat("en-US", {{ hour:"numeric", minute:"2-digit", hour12:true, timeZoneName:"short" }}).format(new Date(iso));
    }}

    const matches = parseCsv(RAW_CSV)
      .sort((a,b) => String(a.start_time||"").localeCompare(String(b.start_time||"")))
      .map((m, i) => {{
        const stageNorm = normalizedStage(m);
        return {{
          ...m,
          home_team: m.home_team || "TBD",
          away_team: m.away_team || "TBD",
          group: (m.group || "").toUpperCase(),
          stageNorm,
          match_number: i + 1
        }};
      }});
    const groupToTeams = new Map();
    const teamToGroups = new Map();
    matches.forEach((m) => {{
      if (!m.group) return;
      if (!groupToTeams.has(m.group)) groupToTeams.set(m.group, new Set());
      groupToTeams.get(m.group).add(m.home_team);
      groupToTeams.get(m.group).add(m.away_team);
      if (!teamToGroups.has(m.home_team)) teamToGroups.set(m.home_team, new Set());
      if (!teamToGroups.has(m.away_team)) teamToGroups.set(m.away_team, new Set());
      teamToGroups.get(m.home_team).add(m.group);
      teamToGroups.get(m.away_team).add(m.group);
    }});

    function filteredMatches() {{
      return matches.filter((m) => {{
        const teamOk = !selectedTeams.size || selectedTeams.has(m.home_team) || selectedTeams.has(m.away_team);
        const groupOk = !selectedGroups.size || selectedGroups.has(m.group);
        const stageOk = !selectedStage || m.stageNorm === selectedStage;
        const cupRoundOk =
          selectedStage !== "cup" ||
          !selectedKnockoutRound ||
          knockoutRoundKey(m) === selectedKnockoutRound;
        return teamOk && groupOk && stageOk && cupRoundOk;
      }});
    }}

    const KNOCKOUT_ROUND_OPTIONS = [
      {{ key: "", label: "All knockout rounds" }},
      {{ key: "r32", label: "Round of 32" }},
      {{ key: "r16", label: "Round of 16" }},
      {{ key: "qf", label: "Quarterfinals" }},
      {{ key: "sf", label: "Semifinals" }},
      {{ key: "3rd", label: "Third place" }},
      {{ key: "final", label: "Final" }},
    ];

    function renderStagePills() {{
      const stages = [
        {{ key: "", label: "All stages" }},
        {{ key: "league", label: "Group stage" }},
        {{ key: "cup", label: "Knockout" }},
      ];
      dom.stagePills.innerHTML = "";
      stages.forEach(({{ key, label }}) => {{
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "pill" + (selectedStage === key ? " active" : "");
        btn.textContent = label;
        btn.onclick = () => {{
          const y = pageScrollY();
          selectedStage = key;
          if (key !== "cup") selectedKnockoutRound = "";
          renderStagePills();
          render();
          setPageScrollY(y);
        }};
        dom.stagePills.appendChild(btn);
      }});

      const cupMatches = matches.filter((m) => m.stageNorm === "cup");
      const showKnockoutSub = selectedStage === "cup" && cupMatches.length > 0;
      dom.stageSubWrap.classList.toggle("hidden", !showKnockoutSub);
      dom.stageSubLabel.textContent = "Knockout round";
      dom.stageSubPills.innerHTML = "";
      if (showKnockoutSub) {{
        KNOCKOUT_ROUND_OPTIONS.forEach(({{ key, label }}) => {{
          const count = cupMatches.filter(
            (m) => !key || knockoutRoundKey(m) === key
          ).length;
          const btn = document.createElement("button");
          btn.type = "button";
          btn.className = "pill" + (selectedKnockoutRound === key ? " active" : "");
          btn.innerHTML = `${{label}}<span>${{count}}</span>`;
          btn.onclick = () => {{
            const y = pageScrollY();
            selectedKnockoutRound = key;
            renderStagePills();
            render();
            setPageScrollY(y);
          }};
          dom.stageSubPills.appendChild(btn);
        }});
      }}

      const roundHuman = KNOCKOUT_ROUND_OPTIONS.find((o) => o.key === selectedKnockoutRound);
      dom.stageSummary.textContent = !selectedStage
        ? "Stage: All stages"
        : selectedStage === "league"
          ? "Stage: Group stage only"
          : selectedKnockoutRound && roundHuman
            ? `Stage: Knockout — ${{roundHuman.label}}`
            : "Stage: Knockout only";
    }}

    function renderGroupPills() {{
      const groupCounts = new Map();
      matches.forEach((m) => {{
        if (!m.group) return;
        groupCounts.set(m.group, (groupCounts.get(m.group) || 0) + 1);
      }});

      dom.groupPills.innerHTML = "";
      [...groupCounts.keys()].sort((a,b)=>a.localeCompare(b)).forEach((group) => {{
        const btn = document.createElement("button");
        const relatedToSelectedTeam = selectedTeams.size && [...selectedTeams].some((team) => teamToGroups.get(team)?.has(group));
        btn.className = "pill"
          + (selectedGroups.has(group) ? " active" : "")
          + (!selectedGroups.has(group) && relatedToSelectedTeam ? " related" : "");
        btn.innerHTML = `Group ${{group}}<span>${{groupCounts.get(group)}}</span>`;
        btn.onclick = () => {{
          const y = pageScrollY();
          selectedGroups.has(group) ? selectedGroups.delete(group) : selectedGroups.add(group);
          renderPills();
          renderGroupPills();
          render();
          setPageScrollY(y);
        }};
        dom.groupPills.appendChild(btn);
      }});

      dom.groupSummary.textContent = selectedGroups.size
        ? `Group filter: ${{[...selectedGroups].sort((a,b)=>a.localeCompare(b)).map((g)=>`Group ${{g}}`).join(", ")}}`
        : "Group filter: All groups";
    }}

    function renderPills() {{
      const counts = new Map();
      matches.forEach(m => {{
        counts.set(m.home_team, (counts.get(m.home_team)||0)+1);
        counts.set(m.away_team, (counts.get(m.away_team)||0)+1);
      }});
      dom.pills.innerHTML = "";
      const isPlaceholderTeam = (name) => /^[0-9]/.test(name || "") || /^W/i.test(name || "");
      [...counts.keys()].sort((a,b)=> {{
        const aPlaceholder = isPlaceholderTeam(a);
        const bPlaceholder = isPlaceholderTeam(b);
        if (aPlaceholder !== bPlaceholder) return aPlaceholder ? 1 : -1;
        return a.localeCompare(b);
      }}).forEach(team => {{
        const btn = document.createElement("button");
        const relatedToSelectedGroup = selectedGroups.size && [...selectedGroups].some((g) => groupToTeams.get(g)?.has(team));
        btn.className = "pill"
          + (selectedTeams.has(team) ? " active" : "")
          + (!selectedTeams.has(team) && relatedToSelectedGroup ? " related" : "");
        btn.innerHTML = `${{team}}<span>${{counts.get(team)}}</span>`;
        btn.onclick = () => {{
          const y = pageScrollY();
          selectedTeams.has(team) ? selectedTeams.delete(team) : selectedTeams.add(team);
          renderPills();
          renderGroupPills();
          render();
          setPageScrollY(y);
        }};
        dom.pills.appendChild(btn);
      }});
    }}

    function render() {{
      const rows = filteredMatches();
      const teamPart = selectedTeams.size
        ? `teams: ${{[...selectedTeams].sort((a,b)=>a.localeCompare(b)).join(", ")}}`
        : "teams: all";
      const groupPart = selectedGroups.size
        ? `groups: ${{[...selectedGroups].sort((a,b)=>a.localeCompare(b)).join(", ")}}`
        : "groups: all";
      const stagePart = !selectedStage
        ? "stage: all"
        : selectedStage === "league"
          ? "stage: group"
          : selectedKnockoutRound
            ? `stage: knockout (${{selectedKnockoutRound}})`
            : "stage: knockout";
      dom.summary.textContent = `Showing ${{rows.length}} matches (${{teamPart}}; ${{groupPart}}; ${{stagePart}})`;

      const live = rows.filter(m => statusKind(m)==="LIVE").length;
      dom.liveBadge.textContent = live ? `${{live}} LIVE` : "";
      dom.lastUpdated.textContent = "Last updated: " + new Intl.DateTimeFormat("en-US", {{
        month:"short", day:"numeric", hour:"numeric", minute:"2-digit", timeZoneName:"short"
      }}).format(new Date());
      dom.tzNote.textContent = `All times are shown in your local timezone (${{VIEWER_TZ}}).`;

      const grouped = new Map();
      rows.forEach(m => {{
        const k = localDayKey(m.start_time);
        if (!grouped.has(k)) grouped.set(k, []);
        grouped.get(k).push(m);
      }});

      const keys = [...grouped.keys()].sort((a,b)=>a.localeCompare(b));
      dom.timeline.innerHTML = "";
      let knockoutShown = false;

      for (let i = 0; i < keys.length; i++) {{
        const dayKey = keys[i];
        const dayMatches = grouped.get(dayKey);
        const dayHasCup = dayMatches.some((m) => m.stageNorm === "cup");
        const priorLeague = keys.slice(0, i).some((k) =>
          grouped.get(k).some((m) => m.stageNorm === "league")
        );
        if (!knockoutShown && dayHasCup && (priorLeague || i === 0)) {{
          const sep = document.createElement("div");
          sep.className = "knockout";
          sep.textContent = "⚽ Knockout Stage";
          dom.timeline.appendChild(sep);
          knockoutShown = true;
        }}

        const block = document.createElement("section");
        block.className = "day-block";
        block.innerHTML = `<div class="day-header"><div class="day-title">${{localDayTitle(dayKey)}}</div><div class="day-count">${{dayMatches.length}} matches</div></div>`;

        dayMatches.forEach((m) => {{
          const kind = statusKind(m);
          const card = document.createElement("article");
          card.className = "match-card " + (kind==="LIVE" ? "live" : kind==="COMPLETED" ? "completed" : "");
          const hasScore = kind === "COMPLETED" && m.home_score !== "" && m.away_score !== "";
          const center = hasScore ? `<div class="score">${{m.home_score}} - ${{m.away_score}}</div>` : `<div class="sep">vs</div>`;
          card.innerHTML = `
            <div>
              <div class="stage">${{stageLabel(m, m.match_number - 1)}}</div>
              <div class="kickoff">${{localTime(m.start_time)}}</div>
              <div class="venue-time">${{kind}}</div>
            </div>
            <div class="center">
              <div class="team">${{m.home_team}}</div>
              ${{center}}
              <div class="team">${{m.away_team}}</div>
            </div>
            <div class="right">Match ${{m.match_number}} of ${{matches.length}}</div>
          `;
          block.appendChild(card);
        }});

        dom.timeline.appendChild(block);
      }}

      if (!rows.length) dom.timeline.innerHTML = `<div class="empty">No matches for current filter.</div>`;
    }}

    dom.clearBtn.onclick = () => {{
      selectedTeams.clear();
      selectedGroups.clear();
      selectedStage = "";
      selectedKnockoutRound = "";
      renderStagePills();
      renderPills();
      renderGroupPills();
      render();
    }};
    dom.clearGroupBtn.onclick = () => {{ selectedGroups.clear(); renderGroupPills(); render(); }};
    dom.toggleGroupsBtn.onclick = () => {{
      const hidden = dom.groupSection.classList.toggle("hidden");
      dom.toggleGroupsBtn.textContent = hidden ? "▸" : "▾";
      dom.toggleGroupsBtn.setAttribute("aria-expanded", hidden ? "false" : "true");
    }};
    dom.toggleTeamsBtn.onclick = () => {{
      const hidden = dom.teamSection.classList.toggle("hidden");
      dom.toggleTeamsBtn.textContent = hidden ? "▸" : "▾";
      dom.toggleTeamsBtn.setAttribute("aria-expanded", hidden ? "false" : "true");
    }};
    dom.toggleStageBtn.onclick = () => {{
      const hidden = dom.stageSection.classList.toggle("hidden");
      dom.toggleStageBtn.textContent = hidden ? "▸" : "▾";
      dom.toggleStageBtn.setAttribute("aria-expanded", hidden ? "false" : "true");
    }};
    dom.refreshBtn.onclick = () => render();

    renderGroupPills();
    renderPills();
    renderStagePills();
    render();
  </script>
</body>
</html>
"""

    HTML_PATH.write_text(html, encoding="utf-8")
    print(f"Wrote {HTML_PATH.name} from {csv_path.name}")


if __name__ == "__main__":
    main()
