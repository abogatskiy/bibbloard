#!/usr/bin/env python3
"""
Billboard H-Index Calculator
==============================
Computes two h-index variants for every artist in the Billboard Hot 100 history
(1958–present) and for six genre charts, using open datasets.

H-Index definitions (by analogy with the academic h-index):
  - Weeks h-index  : artist has h songs each appearing on the chart for ≥ h weeks
  - Peak h-index   : artist has h songs each with chart score ≥ h
                     (chart score = 100 − peak_position, so #1 → 99, #100 → 0)

Data sources:
  Hot 100:  https://github.com/mhollingshead/billboard-hot-100
  Genres:   https://github.com/pdp2600/chartscraper
"""

import csv
import json
import urllib.request
import urllib.parse
from collections import defaultdict
from datetime import date
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

HOT100_URL   = "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/all.json"
HOT100_CACHE = Path("data/all.json")

CHARTSCRAPER_BASE = "https://raw.githubusercontent.com/pdp2600/chartscraper/master/ChartScraper_data/"
GENRE_CHARTS = {
    "Country":          "All_Country_Songs_from_2011-04-09_to_2018-12-31.csv",
    "Hip-Hop":          "All_Hip_Hop_Songs_from_1958-10-20_to_2018-12-31.csv",
    "Latin":            "All_Latin_Songs_from_1986-09-20_to_2018-12-31.csv",
    "Pop":              "All_Pop_Songs_from_1992-10-03_to_2018-12-31.csv",
    "Rock":             "All_Rock_Songs_from_2009-06-20_to_2018-12-31.csv",
    "Dance/Electronic": "All_Dance_Electronic_Songs_from_2013-01-26_to_2018-12-31.csv",
}

# URL-safe key for each chart (used in data/<key>[_<period>].json filenames)
CHART_KEYS = {
    "Hot 100":          "hot100",
    "Country":          "country",
    "Hip-Hop":          "hiphop",
    "Latin":            "latin",
    "Pop":              "pop",
    "Rock":             "rock",
    "Dance/Electronic": "dance_electronic",
}

# (period_key, years_back) — None means all-time
PERIODS = [
    ("all", None),
    ("5y",   5),
    ("10y", 10),
    ("15y", 15),
    ("20y", 20),
    ("25y", 25),
    ("30y", 30),
]

DATA_DIR  = Path("data")
TOP_N     = 100   # artists printed in console ranking
TOP_PLOT  = 30    # artists included in chart JSON (client filters to 10/20/30)
TOP_TABLE = 200   # rows in HTML ranking tables

COLORS = [
    '#4e79a7','#f28e2b','#e15759','#76b7b2','#59a14f',
    '#edc948','#b07aa1','#ff9da7','#9c755f','#bab0ac',
    '#1f77b4','#aec7e8','#ffbb78','#2ca02c','#98df8a',
    '#d62728','#ff9896','#9467bd','#c5b0d5','#8c564b',
    '#c49c94','#e377c2','#f7b6d2','#7f7f7f','#c7c7c7',
    '#bcbd22','#dbdb8d','#17becf','#9edae5','#393b79',
]

# ── Download helpers ───────────────────────────────────────────────────────────

def _reporthook(label):
    def hook(count, block_size, total_size):
        if total_size > 0:
            pct = min(count * block_size * 100 // total_size, 100)
            print(f"\r  {label}: {pct}%", end="", flush=True)
    return hook

def load_hot100() -> list:
    if HOT100_CACHE.exists():
        print("Loading cached Hot 100 data …", flush=True)
        with open(HOT100_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    print("Downloading Hot 100 data (~50 MB) …", flush=True)
    urllib.request.urlretrieve(HOT100_URL, HOT100_CACHE, _reporthook("Hot 100"))
    print()
    with open(HOT100_CACHE, "r", encoding="utf-8") as f:
        return json.load(f)

# ── Row parsing (flat lists, one entry per chart-week per song) ────────────────

def parse_hot100_rows(charts: list) -> list:
    """Flatten all weekly chart entries into a list of dicts with date."""
    rows = []
    for chart in charts:
        chart_date = chart.get("date", "")
        for entry in chart.get("data", []):
            artist = entry.get("artist", "").strip()
            song   = entry.get("song",   "").strip()
            if not artist or not song:
                continue
            peak = entry.get("peak_position") or 101
            rows.append({"artist": artist, "song": song,
                         "peak": peak, "date": chart_date})
    return rows

def parse_genre_rows(genre_name: str, filename: str) -> list:
    """Download (if needed) and parse a chartscraper CSV into a flat row list."""
    cache = DATA_DIR / filename
    if not cache.exists() or cache.stat().st_size < 1024:
        url = CHARTSCRAPER_BASE + urllib.parse.quote(filename)
        print(f"Downloading {genre_name} data …", flush=True)
        urllib.request.urlretrieve(url, cache, _reporthook(genre_name))
        print()

    print(f"Parsing {genre_name} …", flush=True)
    rows = []
    with open(cache, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            artist = (row.get("artist") or "").strip()
            title  = (row.get("title")  or "").strip()
            if not artist or not title:
                continue
            try:
                peak       = int(row.get("peak_position")  or 101)
                chart_date = row.get("chart_date", "")
            except (ValueError, TypeError):
                continue
            if peak <= 0:
                peak = 101
            rows.append({"artist": artist, "song": title,
                         "peak": peak, "date": chart_date})
    return rows

def max_row_date(rows: list) -> str:
    return max((r["date"] for r in rows if r["date"]), default="")

# ── Deduplication with optional date filter ───────────────────────────────────

def deduplicate_rows(rows: list, since: str = None) -> dict:
    """
    Aggregate rows into artist_songs dict: artist -> [{song, weeks, peak}].

    weeks = number of chart-week appearances in the date window (each row = 1 week).
    peak  = best (lowest numbered) peak_position seen in the window.
    """
    raw = {}   # (artist, song) -> [count, peak]
    for r in rows:
        if since and r["date"] < since:
            continue
        key = (r["artist"], r["song"])
        if key not in raw:
            raw[key] = [1, r["peak"]]
        else:
            raw[key][0] += 1
            raw[key][1]  = min(raw[key][1], r["peak"])

    artist_songs = defaultdict(list)
    for (artist, song), (weeks, peak) in raw.items():
        artist_songs[artist].append({"song": song, "weeks": weeks, "peak": peak})
    return dict(artist_songs)

# ── H-Index ────────────────────────────────────────────────────────────────────

def hindex_weeks(songs: list) -> int:
    """Largest h s.t. artist has h songs each on chart for ≥ h weeks."""
    counts = sorted((s["weeks"] for s in songs), reverse=True)
    h = 0
    for i, w in enumerate(counts, start=1):
        if w >= i: h = i
        else: break
    return h

def hindex_peak(songs: list, chart_size: int = 100) -> int:
    """Largest h s.t. h songs have chart score ≥ h  (score = chart_size − peak)."""
    peaks = sorted(s["peak"] for s in songs)
    h = 0
    for i, p in enumerate(peaks, start=1):
        if p <= chart_size - i: h = i
        else: break
    return h

def compute_chart_size(artist_songs: dict) -> int:
    """Infer chart depth from the data (max peak position seen)."""
    return max(
        s["peak"] for songs in artist_songs.values() for s in songs if s["peak"] > 0
    )

def hh_index(ranking) -> int:
    """Largest h s.t. h artists have h-index ≥ h."""
    values = sorted((r[1] for r in ranking), reverse=True)
    h = 0
    for i, v in enumerate(values, start=1):
        if v >= i: h = i
        else: break
    return h

# ── Rankings ───────────────────────────────────────────────────────────────────

def compute_rankings(artist_songs: dict, chart_size: int = 100):
    """Returns (weeks_ranking, peak_ranking) — sorted lists of (artist, h, n_songs)."""
    weeks_ranking = []
    peak_ranking  = []
    for artist, songs in artist_songs.items():
        hw = hindex_weeks(songs)
        hp = hindex_peak(songs, chart_size)
        n  = len(songs)
        weeks_ranking.append((artist, hw, n))
        peak_ranking.append( (artist, hp, n))
    weeks_ranking.sort(key=lambda x: (-x[1], -x[2]))
    peak_ranking.sort( key=lambda x: (-x[1], -x[2]))
    return weeks_ranking, peak_ranking

# ── Date arithmetic ───────────────────────────────────────────────────────────

def compute_since_date(max_date_str: str, years_back) -> str:
    if years_back is None or not max_date_str:
        return None
    d = date.fromisoformat(max_date_str[:10])
    try:
        return d.replace(year=d.year - years_back).isoformat()
    except ValueError:                          # Feb 29 in non-leap year
        return d.replace(year=d.year - years_back, day=28).isoformat()

# ── Console output ─────────────────────────────────────────────────────────────

def print_ranking(title: str, ranking, top_n: int):
    print(f"\n{'═'*62}\n  {title}\n{'═'*62}")
    print(f"  {'#':>4}  {'H':>4}  {'Songs':>6}  Artist")
    print(f"  {'-'*4}  {'-'*4}  {'-'*6}  {'-'*40}")
    for rank, (artist, h, n) in enumerate(ranking[:top_n], 1):
        print(f"  {rank:>4}  {h:>4}  {n:>6}  {artist}")

def save_csv(filename: str, ranking, header_h: str):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"rank,{header_h},total_songs,artist\n")
        for rank, (artist, h, n) in enumerate(ranking, 1):
            safe = artist.replace('"', '""')
            f.write(f'{rank},{h},{n},"{safe}"\n')
    print(f"  Saved → {filename}")

# ── Data JSON ──────────────────────────────────────────────────────────────────

def curve_values(artist: str, artist_songs: dict, metric: str, chart_size: int = 100):
    songs = artist_songs[artist]
    if metric == "weeks":
        pairs = sorted(((s["weeks"], s["song"]) for s in songs), reverse=True)
    else:
        pairs = sorted(
            ((chart_size - s["peak"], s["song"]) for s in songs
             if s.get("peak") and 0 < s["peak"] <= chart_size),
            reverse=True
        )
    pairs = [(v, nm) for v, nm in pairs if v >= 1][:160]
    return [v for v, _ in pairs], [nm for _, nm in pairs]

def build_chart_payload(weeks_ranking, peak_ranking, artist_songs, hhw, hhp, chart_size: int = 100) -> dict:
    def plot_data(ranking, metric):
        out = []
        for i, (a, h, n) in enumerate(ranking[:TOP_PLOT]):
            vals, names = curve_values(a, artist_songs, metric, chart_size)
            out.append({"artist": a, "h": h, "n": n,
                        "color": COLORS[i], "values": vals, "songs": names})
        return out

    return {
        "hhw": hhw, "hhp": hhp,
        "chart_size": chart_size,
        "weeks":      plot_data(weeks_ranking, "weeks"),
        "peak":       plot_data(peak_ranking,  "peak"),
        "weeksTable": [[r, a, h, n] for r, (a, h, n) in enumerate(weeks_ranking[:TOP_TABLE], 1)],
        "peakTable":  [[r, a, h, n] for r, (a, h, n) in enumerate(peak_ranking[:TOP_TABLE],  1)],
    }

def save_chart_data(payload: dict, path: Path):
    path.parent.mkdir(exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))
    size_kb = path.stat().st_size // 1024
    print(f"    → {path}  ({size_kb} KB)")

# ── HTML template ──────────────────────────────────────────────────────────────

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Billboard H-Index</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:Georgia,'Times New Roman',serif;background:#f7f7f5;color:#222;line-height:1.55}
.wrap{max-width:1300px;margin:0 auto;padding:2.5rem 2rem 4rem}
h1{font-size:2rem;letter-spacing:-.02em;margin-bottom:.3rem}
.byline{font-size:.88rem;color:#888;margin-bottom:2.2rem}
.byline a{color:#4e79a7;text-decoration:none}

/* Genre ranking table */
.section-title{font-size:1rem;font-weight:bold;margin-bottom:.7rem;letter-spacing:-.01em}

/* Chart selector */
.selector-bar{display:flex;align-items:center;gap:.7rem;margin-bottom:1.5rem;flex-wrap:wrap}
.selector-bar label{font-size:.88rem;color:#555}
.selector-bar select{font-family:inherit;font-size:.9rem;padding:.35rem .7rem;
  border:1px solid #ccc;border-radius:3px;background:#fff;cursor:pointer;color:#222}
.selector-bar select:focus{outline:2px solid #4e79a7;outline-offset:1px}
.selector-bar .sep{color:#ccc;margin:0 .1rem}
#loading{font-size:.82rem;color:#aaa;display:none}

/* H-H-Index box */
.hhbox{display:flex;gap:0;background:#fff;border:1px solid #ddd;
       border-left:5px solid #4e79a7;border-radius:3px;margin-bottom:2.2rem;overflow:hidden}
.hhstat{padding:1.1rem 2rem;border-right:1px solid #eee}
.hhstat .val{font-size:2rem;font-weight:bold;line-height:1;color:#222}
.hhstat .lbl{font-size:.78rem;color:#888;margin-top:.25rem}
.hhdesc{padding:1.1rem 1.5rem;font-size:.82rem;color:#888;
        align-self:center;line-height:1.6;flex:1}

/* Plots */
.plots{display:grid;grid-template-columns:repeat(auto-fit,minmax(520px,1fr));gap:1.5rem;margin-bottom:2.2rem}
.plot-card{background:#fff;border:1px solid #ddd;border-radius:3px;padding:.75rem .75rem .5rem}
.plot-card h2{font-size:.95rem;padding:0 .25rem .15rem;margin-bottom:.1rem}
.plot-card .desc{font-size:.77rem;color:#999;padding:0 .25rem .4rem}
.plot-box{width:100%;aspect-ratio:1}

/* Tables */
.tables{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem}
.tbl-card{background:#fff;border:1px solid #ddd;border-radius:3px;padding:1rem}
.tbl-card h2{font-size:.9rem;margin-bottom:.7rem}
table{width:100%;border-collapse:collapse;font-size:.8rem}
thead th{background:#222;color:#fff;padding:.3rem .6rem;font-weight:normal;
         font-size:.71rem;text-transform:uppercase;letter-spacing:.06em;text-align:left}
tbody td{padding:.26rem .6rem;border-bottom:1px solid #f2f2f0;vertical-align:middle}
tbody tr:hover{background:#fafaf8}
.tr{text-align:right}.dim{color:#bbb}.bld{font-weight:bold}

/* Load more button */
.load-more-wrap{text-align:center;margin:1rem 0 2rem}
#load-more-btn{font-family:inherit;font-size:.82rem;padding:.4rem 1.2rem;
  border:1px solid #ccc;border-radius:3px;background:#fff;cursor:pointer;color:#555}
#load-more-btn:hover{background:#f5f5f3}

footer{margin-top:2.5rem;font-size:.74rem;color:#bbb;
       border-top:1px solid #e8e8e5;padding-top:1rem;line-height:1.7}
@media(max-width:860px){.plots,.tables{grid-template-columns:1fr}.hhbox{flex-wrap:wrap}}
</style>
</head>
<body>
<div class="wrap">
  <h1>Billboard H-Index</h1>
  <p class="byline">
    Hot&nbsp;100: 3,531 weekly charts (1958&ndash;2026) &middot;
    Data: <a href="https://github.com/mhollingshead/billboard-hot-100">mhollingshead/billboard-hot-100</a>
    &nbsp;&middot;&nbsp;
    Genre charts: <a href="https://github.com/pdp2600/chartscraper">pdp2600/chartscraper</a>
  </p>

  <!-- Genre h-h-index ranking -->
  <div class="section-title">Genre H-H-Index Ranking</div>
  <div class="tables" style="margin-bottom:2.2rem">
    <div class="tbl-card">
      <table><thead><tr>
        <th style="width:2rem" class="tr">#</th>
        <th>Genre</th>
        <th style="width:5rem" class="tr">Weeks H-H</th>
        <th style="width:5rem" class="tr">Peak H-H</th>
      </tr></thead><tbody id="genre-tbody"></tbody></table>
    </div>
    <div class="tbl-card" style="font-size:.8rem;color:#888;padding:1rem 1.2rem;line-height:1.7">
      <p style="margin-bottom:.6rem">
        The <strong style="color:#222">h-h-index</strong> of a chart is the largest <em>h</em>
        such that <em>h</em> artists each have an h-index&nbsp;&ge;&nbsp;<em>h</em> on that chart.
      </p>
      <p>
        <strong style="color:#222">Weeks h-index</strong> for an artist: sort their songs by
        weeks-on-chart descending; h&nbsp;= the largest rank where weeks&nbsp;&ge;&nbsp;rank.<br>
        <strong style="color:#222">Peak h-index</strong>: sort by chart score
        (100&nbsp;&minus;&nbsp;peak position) descending; h&nbsp;= largest rank where
        score&nbsp;&ge;&nbsp;rank.
      </p>
    </div>
  </div>

  <!-- Chart + period selectors -->
  <div class="selector-bar">
    <label for="chart-select">View chart:</label>
    <select id="chart-select">
      <option value="hot100">Billboard Hot 100</option>
      <option value="country">Country</option>
      <option value="hiphop">Hip-Hop</option>
      <option value="latin">Latin</option>
      <option value="pop">Pop</option>
      <option value="rock">Rock</option>
      <option value="dance_electronic">Dance / Electronic</option>
    </select>
    <span class="sep">&middot;</span>
    <label for="period-select">History:</label>
    <select id="period-select">
      <option value="all">All time</option>
      <option value="5y">Last 5 years</option>
      <option value="10y">Last 10 years</option>
      <option value="15y">Last 15 years</option>
      <option value="20y">Last 20 years</option>
      <option value="25y">Last 25 years</option>
      <option value="30y">Last 30 years</option>
    </select>
    <span class="sep">&middot;</span>
    <label for="n-select">Artists in plot:</label>
    <select id="n-select">
      <option value="10">Top 10</option>
      <option value="20">Top 20</option>
      <option value="30">Top 30</option>
    </select>
    <span id="loading">Loading&hellip;</span>
  </div>

  <!-- H-H-Index stats (updated per selection) -->
  <div class="hhbox">
    <div class="hhstat">
      <div class="val" id="hhw-val">&mdash;</div>
      <div class="lbl">Weeks h-h-index</div>
    </div>
    <div class="hhstat">
      <div class="val" id="hhp-val">&mdash;</div>
      <div class="lbl">Peak h-h-index</div>
    </div>
    <div class="hhdesc">
      The <strong>h-h-index</strong> is the h-index of h-indices &mdash;
      the largest <em>h</em> such that <em>h</em> artists each have an h-index &ge; <em>h</em>.
    </div>
  </div>

  <!-- Plots -->
  <div class="plots">
    <div class="plot-card">
      <h2>Weeks H-Index &mdash; Top <span class="n-label">10</span></h2>
      <div class="desc"><em>h</em> songs spent &ge;&thinsp;<em>h</em> weeks on chart &middot; hover for details</div>
      <div id="weeks-plot" class="plot-box"></div>
    </div>
    <div class="plot-card">
      <h2>Peak H-Index &mdash; Top <span class="n-label">10</span></h2>
      <div class="desc" id="peak-desc"><em>h</em> songs have chart score &ge;&thinsp;<em>h</em> &middot; score&nbsp;=&nbsp;100&thinsp;&minus;&thinsp;peak position &middot; hover for details</div>
      <div id="peak-plot" class="plot-box"></div>
    </div>
  </div>

  <!-- Artist ranking tables -->
  <div class="tables">
    <div class="tbl-card">
      <h2>Weeks H-Index &mdash; Top 50</h2>
      <table><thead><tr>
        <th style="width:2rem" class="tr">#</th>
        <th style="width:2rem" class="tr">H</th>
        <th style="width:3.2rem" class="tr">Songs</th>
        <th>Artist</th>
      </tr></thead><tbody id="weeks-tbody"></tbody></table>
    </div>
    <div class="tbl-card">
      <h2>Peak H-Index &mdash; Top 50</h2>
      <table><thead><tr>
        <th style="width:2rem" class="tr">#</th>
        <th style="width:2rem" class="tr">H</th>
        <th style="width:3.2rem" class="tr">Songs</th>
        <th>Artist</th>
      </tr></thead><tbody id="peak-tbody"></tbody></table>
    </div>
  </div>

  <!-- Load more -->
  <div class="load-more-wrap">
    <button id="load-more-btn" onclick="loadMore()">Load more</button>
  </div>

  <footer>
    <strong>Weeks h-index:</strong> sort songs by weeks-on-chart descending;
    h&nbsp;= largest rank where weeks&nbsp;&ge;&nbsp;rank. &nbsp;&middot;&nbsp;
    <strong>Peak h-index:</strong> sort songs by chart score (100&nbsp;&minus;&nbsp;peak position) descending;
    h&nbsp;= largest rank where score&nbsp;&ge;&nbsp;rank. &nbsp;&middot;&nbsp;
    Each (artist,&nbsp;song) pair is deduplicated; weeks = chart appearances in window, peak = best position in window. &nbsp;&middot;&nbsp;
    Genre data covers 2009&ndash;2018 (Rock, Dance/Electronic), 2011&ndash;2018 (Country),
    1986&ndash;2018 (Latin), 1992&ndash;2018 (Pop), 1958&ndash;2018 (Hip-Hop).
  </footer>
</div>

<script>
// Genre summary inlined (tiny — 6 genres × 7 periods)
const GENRE_SUMMARY = __GENRE_SUMMARY__;

function selectChart(key) {
  document.getElementById('chart-select').value = key;
  loadChart();
  document.getElementById('chart-select').scrollIntoView({behavior: 'smooth', block: 'center'});
}

function updateGenreTable(period) {
  const rows = GENRE_SUMMARY.map(g => {
    const p = g.periods[period] || g.periods['all'];
    return {genre: g.genre, key: g.key, hhw: p.hhw, hhp: p.hhp};
  }).sort((a, b) => b.hhw - a.hhw || b.hhp - a.hhp);
  const tbody = document.getElementById('genre-tbody');
  tbody.innerHTML = '';
  rows.forEach((g, i) => {
    tbody.insertAdjacentHTML('beforeend',
      `<tr onclick="selectChart('${g.key}')" style="cursor:pointer" title="View ${g.genre} chart">` +
      `<td class="tr dim">${i+1}</td><td>${g.genre}</td>` +
      `<td class="tr bld">${g.hhw}</td><td class="tr bld">${g.hhp}</td></tr>`);
  });
}
updateGenreTable('all');

// ── State ─────────────────────────────────────────────────────────────────────
let _currentData = null;
let _tableOffset = 50;
const TABLE_STEP = 50;

// ── Label spreading (greedy top-down) ────────────────────────────────────────
function spreadLabels(pts, minGap, yMin, yMax) {
  const n = pts.length;
  const avail = yMax - yMin;
  if (minGap * (n - 1) > avail) minGap = avail / Math.max(n - 1, 1);
  pts[0].yl = Math.min(pts[0].y, yMax);
  for (let i = 1; i < n; i++)
    pts[i].yl = Math.min(pts[i].y, pts[i-1].yl - minGap);
  if (pts[n-1].yl < yMin) {
    const shift = yMin - pts[n-1].yl;
    pts.forEach(p => p.yl += shift);
  }
}

// ── Build one Plotly chart ────────────────────────────────────────────────────
function createPlot(divId, curves, yLabel, chartSize) {
  Plotly.purge(divId);
  if (!curves.length) return;
  const traces = [];
  const annotations = [];

  const maxY  = Math.max(...curves.map(c => c.values[0] ?? 0));
  const maxX  = Math.max(...curves.map(c => c.values.length));
  // chartSize (peak chart only): the chart's total positions, used to set x range
  // so both axes span 0..chartSize and the diagonal runs at a true 45°.
  const scoreRange = chartSize || maxY;
  const xClip  = maxX < scoreRange * 0.4 ? scoreRange + 5 : Math.min(maxX + 4, scoreRange + 5);
  const diagEnd = xClip;

  // Diagonal y = x
  traces.push({
    x: [0, diagEnd], y: [0, diagEnd],
    mode: 'lines',
    line: {color: 'rgba(60,60,60,0.28)', dash: 'dot', width: 1.5},
    hoverinfo: 'skip', showlegend: false
  });

  // Artist curves
  for (const c of curves) {
    const x = c.values.map((_, i) => i + 1);
    traces.push({
      x, y: c.values,
      mode: 'lines',
      line: {color: c.color, width: 2.2},
      showlegend: false,
      customdata: c.songs,
      hovertemplate: '<b>' + c.artist + '</b><br><i>%{customdata}</i><br>' +
                     '#%{x} \u00b7 ' + yLabel + ': %{y}<extra></extra>'
    });
  }

  // H-index markers on diagonal — one trace per artist so hover is correct
  for (const c of curves) {
    if (c.h > 0) {
      traces.push({
        x: [c.h], y: [c.h],
        mode: 'markers',
        marker: {color: c.color, size: 10, symbol: 'circle',
                 line: {color: 'white', width: 2}},
        showlegend: false,
        hovertemplate: '<b>' + c.artist + '</b><br>h&#8209;index\u00a0=\u00a0' + c.h + '<extra></extra>'
      });
    }
  }

  // Left-edge labels with spreading
  const labelPts = curves
    .map(c => ({y: c.values[0] ?? 0, yl: c.values[0] ?? 0, artist: c.artist, color: c.color}))
    .sort((a, b) => b.y - a.y);
  spreadLabels(labelPts, maxY * 0.105, maxY * 0.02, maxY * 0.98);

  for (const lp of labelPts) {
    const needsArrow = Math.abs(lp.yl - lp.y) > 0.5;
    annotations.push({
      x: 0, xref: 'paper',
      y: lp.yl, yref: 'y',
      text: lp.artist,
      showarrow: needsArrow,
      ax: needsArrow ? 1    : undefined, axref: needsArrow ? 'x' : undefined,
      ay: needsArrow ? lp.y : undefined, ayref: needsArrow ? 'y' : undefined,
      arrowhead: 0, arrowwidth: 0.9, arrowcolor: lp.color,
      xanchor: 'right', yanchor: 'middle',
      font: {color: lp.color, size: 11, family: 'Georgia, serif'}
    });
  }

  Plotly.newPlot(divId, traces, {
    xaxis: {
      title: {text: 'Songs ranked by metric (best \u2192 worst)', standoff: 8},
      range: [1, xClip],
      showgrid: true, gridcolor: '#f0f0f0',
      zeroline: false, showline: true, linecolor: '#ddd'
    },
    yaxis: {
      title: {text: yLabel, standoff: 8},
      range: [0, maxY * 1.07],
      showgrid: true, gridcolor: '#f0f0f0',
      zeroline: false, showline: true, linecolor: '#ddd'
    },
    annotations,
    hovermode: 'closest',
    plot_bgcolor: 'white', paper_bgcolor: 'white',
    margin: {t: 15, b: 58, l: 210, r: 15},
    font: {family: 'Georgia, serif', size: 11}
  }, {responsive: true, displayModeBar: false});
}

// ── Table rendering ───────────────────────────────────────────────────────────
function fillTableRows(id, rows) {
  const tbody = document.getElementById(id);
  tbody.innerHTML = '';
  for (const [rank, artist, h, n] of rows) {
    tbody.insertAdjacentHTML('beforeend',
      `<tr><td class="tr dim">${rank}</td><td class="tr bld">${h}</td>` +
      `<td class="tr dim">${n}</td><td>${artist}</td></tr>`);
  }
}

function renderTables() {
  fillTableRows('weeks-tbody', _currentData.weeksTable.slice(0, _tableOffset));
  fillTableRows('peak-tbody',  _currentData.peakTable.slice(0, _tableOffset));
  const total = Math.max(_currentData.weeksTable.length, _currentData.peakTable.length);
  const btn = document.getElementById('load-more-btn');
  if (_tableOffset >= total) {
    btn.style.display = 'none';
  } else {
    btn.style.display = '';
    btn.textContent = `Load more (showing ${Math.min(_tableOffset, total)} of ${total})`;
  }
}

function loadMore() {
  _tableOffset += TABLE_STEP;
  renderTables();
}

// ── Render a full payload ─────────────────────────────────────────────────────
function renderChart(data) {
  _currentData = data;
  _tableOffset = TABLE_STEP;
  document.getElementById('hhw-val').textContent = data.hhw;
  document.getElementById('hhp-val').textContent = data.hhp;
  const n  = parseInt(document.getElementById('n-select').value);
  const cs = data.chart_size || 100;
  document.getElementById('peak-desc').innerHTML =
    '<em>h</em> songs have chart score &ge;&thinsp;<em>h</em> &middot; score&nbsp;=&nbsp;' +
    cs + '&thinsp;&minus;&thinsp;peak position &middot; hover for details';
  createPlot('weeks-plot', data.weeks.slice(0, n), 'Weeks on chart');
  createPlot('peak-plot',  data.peak.slice(0, n),
             'Chart score (' + cs + '\u2212peak position)', cs);
  renderTables();
}

// ── Fetch and render ──────────────────────────────────────────────────────────
function getDataFile() {
  const chart  = document.getElementById('chart-select').value;
  const period = document.getElementById('period-select').value;
  return period === 'all' ? `data/${chart}.json` : `data/${chart}_${period}.json`;
}

async function loadChart() {
  const loading = document.getElementById('loading');
  loading.style.display = 'inline';
  try {
    const resp = await fetch(getDataFile());
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    renderChart(await resp.json());
  } catch (e) {
    console.error('Failed to load chart data:', e);
    alert('Could not load: ' + getDataFile());
  } finally {
    loading.style.display = 'none';
  }
}

// ── Wiring ────────────────────────────────────────────────────────────────────
document.getElementById('chart-select').addEventListener('change', loadChart);
document.getElementById('period-select').addEventListener('change', () => {
  updateGenreTable(document.getElementById('period-select').value);
  loadChart();
});

document.getElementById('n-select').addEventListener('change', () => {
  if (!_currentData) return;
  const n  = parseInt(document.getElementById('n-select').value);
  const cs = _currentData.chart_size || 100;
  document.querySelectorAll('.n-label').forEach(el => el.textContent = n);
  createPlot('weeks-plot', _currentData.weeks.slice(0, n), 'Weeks on chart');
  createPlot('peak-plot',  _currentData.peak.slice(0, n),
             'Chart score (' + cs + '\u2212peak position)', cs);
});

loadChart();
</script>
</body>
</html>"""


def generate_html(genre_summary: list):
    """Write the lightweight HTML shell (no chart data inline)."""
    genre_json = json.dumps(genre_summary, separators=(",", ":"))
    out = _HTML.replace("__GENRE_SUMMARY__", genre_json)
    with open("billboard_hindex.html", "w", encoding="utf-8") as f:
        f.write(out)
    print("  Saved → billboard_hindex.html")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    DATA_DIR.mkdir(exist_ok=True)

    # ── Hot 100 ──────────────────────────────────────────────────────────────
    charts = load_hot100()
    print(f"Loaded {len(charts):,} weekly charts.")

    print("Parsing Hot 100 rows …", flush=True)
    hot100_rows = parse_hot100_rows(charts)
    hot100_max  = max_row_date(hot100_rows)
    print(f"  {len(hot100_rows):,} entries, max date: {hot100_max}")

    # All-time data for CSV export and console ranking
    hot100_all        = deduplicate_rows(hot100_rows)
    hot100_size       = compute_chart_size(hot100_all)
    wr_all, pr_all    = compute_rankings(hot100_all, hot100_size)
    print_ranking("HOT 100 — WEEKS H-INDEX (all time)", wr_all, TOP_N)
    print_ranking("HOT 100 — PEAK H-INDEX  (all time)", pr_all, TOP_N)
    print("\nSaving Hot 100 CSVs …")
    save_csv("billboard_hindex_weeks.csv", wr_all, "weeks_hindex")
    save_csv("billboard_hindex_peak.csv",  pr_all, "peak_hindex")

    print(f"\nGenerating Hot 100 data files … (chart_size={hot100_size})")
    hot100_periods = {}
    for period_key, years_back in PERIODS:
        sd           = compute_since_date(hot100_max, years_back)
        artist_songs = deduplicate_rows(hot100_rows, sd)
        cs           = compute_chart_size(artist_songs)
        wr, pr       = compute_rankings(artist_songs, cs)
        hhw          = hh_index(wr)
        hhp          = hh_index(pr)
        label = "All time" if years_back is None else f"Last {years_back}y"
        print(f"  {label:12s}: {len(artist_songs):5,} artists  chart_size={cs}  Weeks HH={hhw}  Peak HH={hhp}")
        hot100_periods[period_key] = {"hhw": hhw, "hhp": hhp}
        fname = "hot100.json" if period_key == "all" else f"hot100_{period_key}.json"
        save_chart_data(build_chart_payload(wr, pr, artist_songs, hhw, hhp, cs), DATA_DIR / fname)

    # ── Genre charts ─────────────────────────────────────────────────────────
    genre_summary = []

    for genre_name, csv_filename in GENRE_CHARTS.items():
        print(f"\n── {genre_name} ──────────────────────────────────────────────")
        genre_rows = parse_genre_rows(genre_name, csv_filename)
        genre_max  = max_row_date(genre_rows)
        key        = CHART_KEYS[genre_name]
        genre_periods = {}   # period_key -> {hhw, hhp}

        for period_key, years_back in PERIODS:
            sd           = compute_since_date(genre_max, years_back)
            artist_songs = deduplicate_rows(genre_rows, sd)
            cs           = compute_chart_size(artist_songs)
            wr, pr       = compute_rankings(artist_songs, cs)
            hhw          = hh_index(wr)
            hhp          = hh_index(pr)
            label = "All time" if years_back is None else f"Last {years_back}y"
            print(f"  {label:12s}: {len(artist_songs):5,} artists  chart_size={cs}  Weeks HH={hhw}  Peak HH={hhp}")
            genre_periods[period_key] = {"hhw": hhw, "hhp": hhp}
            fname = f"{key}.json" if period_key == "all" else f"{key}_{period_key}.json"
            save_chart_data(build_chart_payload(wr, pr, artist_songs, hhw, hhp, cs), DATA_DIR / fname)

        genre_summary.append({"genre": genre_name, "key": key, "periods": genre_periods})

    # ── Genre summary ─────────────────────────────────────────────────────────
    genre_summary.append({"genre": "Hot 100", "key": "hot100", "periods": hot100_periods})
    genre_summary.sort(key=lambda g: (-g["periods"]["all"]["hhw"], -g["periods"]["all"]["hhp"]))
    print(f"\n{'═'*52}\n  GENRE H-H-INDEX RANKING (all time)\n{'═'*52}")
    print(f"  {'Genre':<22}  {'Weeks HH':>9}  {'Peak HH':>8}")
    print(f"  {'-'*22}  {'-'*9}  {'-'*8}")
    for i, g in enumerate(genre_summary, 1):
        hhw = g["periods"]["all"]["hhw"]
        hhp = g["periods"]["all"]["hhp"]
        print(f"  {i}. {g['genre']:<20}  {hhw:>9}  {hhp:>8}")

    print("\nGenerating HTML …")
    generate_html(genre_summary)
    print("\nDone. Open: http://localhost:7433/billboard_hindex.html")


if __name__ == "__main__":
    main()
