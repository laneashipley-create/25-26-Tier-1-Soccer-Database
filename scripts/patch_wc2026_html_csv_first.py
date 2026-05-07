from __future__ import annotations

import json
import pathlib
import re


ROOT = pathlib.Path(__file__).resolve().parents[1]
HTML_PATH = ROOT / "wc2026_schedule.html"
CSV_PATH = ROOT / "wc2026_schedule.csv"


def main() -> None:
    html = HTML_PATH.read_text(encoding="utf-8")
    csv_text = CSV_PATH.read_text(encoding="utf-8")

    const_block = f"""    const SPORTRADAR_SEASON_ID = "sr:season:101177";
    const TOURNAMENT_START = "2026-06-11";
    const GROUP_STAGE_END = "2026-06-28";
    const CSV_PATH = "./wc2026_schedule.csv";
    const EMBEDDED_CSV = {json.dumps(csv_text)};

    const SUPABASE_URL = "https://yoesorfzvtbdmvrdtqoo.supabase.co";
    const SUPABASE_PUBLISHABLE_KEY = "sb_publishable_x9tYEDwV7sVCpoPRFBjN3w_2TYhKm8J";
    const LIVE_SUPABASE_ENABLED = false;
    const supabase = (window.supabase && LIVE_SUPABASE_ENABLED) ? window.supabase.createClient(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY) : null;"""

    html = re.sub(
        r'const SPORTRADAR_SEASON_ID = "sr:season:101177";[\s\S]*?const supabase = window\.supabase\.createClient\(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY\);',
        const_block,
        html,
        count=1,
    )

    new_fetch = """    // -----------------------------
    // CSV + optional Supabase loading
    // -----------------------------
    function parseCsvText(csvText) {
      const lines = (csvText || "").trim().split(/\\r?\\n/);
      if (lines.length < 2) return [];
      const headers = lines[0].split(",").map((h) => h.trim());
      const rows = [];
      for (let i = 1; i < lines.length; i += 1) {
        const line = lines[i];
        if (!line) continue;
        const cells = [];
        let cur = "";
        let inQuotes = false;
        for (let j = 0; j < line.length; j += 1) {
          const ch = line[j];
          if (ch === '"') {
            if (inQuotes && line[j + 1] === '"') { cur += '"'; j += 1; }
            else inQuotes = !inQuotes;
          } else if (ch === ',' && !inQuotes) {
            cells.push(cur);
            cur = "";
          } else {
            cur += ch;
          }
        }
        cells.push(cur);
        const row = {};
        headers.forEach((h, idx) => { row[h] = (cells[idx] ?? "").trim(); });
        rows.push(row);
      }
      return rows;
    }

    function hydrateRows(rawRows) {
      return rawRows
        .filter((r) => r.season_id === SPORTRADAR_SEASON_ID)
        .sort((a, b) => String(a.start_time || "").localeCompare(String(b.start_time || "")))
        .map((m, idx) => ({
          id: m.id || null,
          season_id: m.season_id || null,
          sport_event_id: m.sport_event_id || null,
          start_time: m.start_time || null,
          round: m.round || null,
          home_team: normalizeTeamName(m.home_team),
          away_team: normalizeTeamName(m.away_team),
          status: m.status || null,
          match_status: m.match_status || null,
          home_score: m.home_score !== "" ? Number(m.home_score) : null,
          away_score: m.away_score !== "" ? Number(m.away_score) : null,
          venue_name: "Venue TBD",
          city_name: "City TBD",
          match_number: idx + 1
        }));
    }

    async function fetchFromCsvFile() {
      if (location.protocol === "file:") return [];
      const resp = await fetch(CSV_PATH, { cache: "no-store" });
      if (!resp.ok) throw new Error(`CSV fetch failed (${resp.status})`);
      const csvText = await resp.text();
      return hydrateRows(parseCsvText(csvText));
    }

    async function fetchFromSupabaseLive() {
      if (!supabase) return [];
      const { data: seasonRows, error: seasonErr } = await supabase
        .from("Seasons (current sr:season:ID)")
        .select("id,sportradar_season_id")
        .eq("sportradar_season_id", SPORTRADAR_SEASON_ID)
        .limit(1);
      if (seasonErr) throw new Error(`Failed to fetch season UUID: ${seasonErr.message}`);
      const seasonUuid = seasonRows?.[0]?.id;
      if (!seasonUuid) throw new Error(`Season ${SPORTRADAR_SEASON_ID} not found`);

      const selectCols = "id,season_id,sport_event_id,start_time,round,home_team,away_team,status,match_status,home_score,away_score";
      const { data, error } = await supabase
        .from("All Games (sr:sport_events)")
        .select(selectCols)
        .eq("season_id", seasonUuid)
        .order("start_time", { ascending: true, nullsFirst: false });
      if (error) throw new Error(`Failed to fetch matches: ${error.message}`);
      return hydrateRows(data || []);
    }

    async function loadData() {
      setVisible(dom.loadingState, true);
      setVisible(dom.errorState, false);
      setVisible(dom.emptyState, false);
      dom.timeline.innerHTML = "";

      try {
        let matches = [];
        try {
          matches = await fetchFromCsvFile();
        } catch (_) {
          matches = [];
        }

        if (!matches.length) {
          matches = hydrateRows(parseCsvText(EMBEDDED_CSV));
        }

        if (!matches.length && LIVE_SUPABASE_ENABLED) {
          matches = await fetchFromSupabaseLive();
        }

        state.allMatches = matches;

        if (state.allMatches.length === 0) {
          setVisible(dom.loadingState, false);
          setVisible(dom.emptyState, true);
          return;
        }

        renderTeamPills();
        renderTimeline();
        formatLastUpdated();
        setVisible(dom.loadingState, false);
      } catch (err) {
        setVisible(dom.loadingState, false);
        setVisible(dom.errorState, true);
        dom.errorText.textContent = err?.message || "Unable to load schedule data.";
      }
    }

"""

    html = re.sub(
        r"\s*// -----------------------------\n\s*// Supabase data fetching[\s\S]*?\n\s*// -----------------------------\n\s*// Filtering \+ rendering",
        "\n" + new_fetch + "    // -----------------------------\n    // Filtering + rendering",
        html,
        count=1,
    )

    HTML_PATH.write_text(html, encoding="utf-8")
    print("Updated wc2026_schedule.html")


if __name__ == "__main__":
    main()
