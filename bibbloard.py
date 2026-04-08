#!/usr/bin/env python3
"""
Bibbloard — Billboard H-Index Calculator
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
import os
import re
import sys
import time
import urllib.request
import urllib.parse
from collections import defaultdict
from datetime import date
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────

HOT100_URL   = "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/all.json"
RAW_DIR      = Path("raw")
HOT100_CACHE = RAW_DIR / "all.json"

# Genre chart CSV filenames (in raw/).
# Core charts (from chartscraper + fetch_genre_updates.py backfill):
GENRE_CHARTS_CORE = [
    ("Country",          "country.csv"),
    ("Hip-Hop",          "hiphop.csv"),
    ("Latin",            "latin.csv"),
    ("Pop",              "pop.csv"),
    ("Rock",             "rock.csv"),
    ("Dance/Electronic", "dance_electronic.csv"),
]
# Optional charts — included only if CSV exists in raw/
GENRE_CHARTS_OPTIONAL = [
    ("Adult Contemporary", "adult_contemporary.csv"),
    ("Adult Pop",          "adult_pop.csv"),
    ("Country Airplay",    "country_airplay.csv"),
    ("Gospel",             "gospel.csv"),
    ("Jazz",               "jazz.csv"),
    ("Alternative",        "alternative.csv"),
]

# URL-safe key for each chart (used in data/<key>[_<period>].json filenames)
CHART_KEYS = {
    "Hot 100":            "hot100",
    "Country":            "country",
    "Hip-Hop":            "hiphop",
    "Latin":              "latin",
    "Pop":                "pop",
    "Rock":               "rock",
    "Dance/Electronic":   "dance_electronic",
    "Adult Contemporary": "adult_contemporary",
    "Adult Pop":          "adult_pop",
    "Country Airplay":    "country_airplay",
    "Gospel":             "gospel",
    "Jazz":               "jazz",
    "Alternative":        "alternative",
}

# (period_key, since_iso) — since_iso=None means all-time
# Using absolute calendar years so cross-chart comparisons are fair.
PERIODS = [
    ("all",  None),
    ("2020", "2020-01-01"),
    ("2015", "2015-01-01"),
    ("2010", "2010-01-01"),
    ("2005", "2005-01-01"),
    ("2000", "2000-01-01"),
]

DATA_DIR   = Path("data")
OUTPUT_DIR = Path("output")
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

# ── Progress bar ──────────────────────────────────────────────────────────────

class Progress:
    """Single-line live progress bar with ETA.  Thread-unsafe but sufficient here."""
    _FULL  = '█'
    _EMPTY = '░'

    def __init__(self, total: int, bar_width: int = 32):
        self.total     = max(total, 1)
        self.done      = 0
        self.start     = time.perf_counter()
        self.desc      = ''
        self._bar_w    = bar_width
        self._t_last   = -1.0

    def update(self, n: int = 1, desc: str = None):
        self.done += n
        if desc is not None:
            self.desc = desc
        t = time.perf_counter()
        if t - self._t_last >= 0.05 or self.done >= self.total:
            self._t_last = t
            self._render()

    def _render(self):
        try:
            cols = os.get_terminal_size().columns
        except OSError:
            cols = 100
        elapsed = time.perf_counter() - self.start
        frac    = min(self.done / self.total, 1.0)
        filled  = int(self._bar_w * frac)
        bar     = self._FULL * filled + self._EMPTY * (self._bar_w - filled)

        if frac >= 1.0:
            time_s = f'done in {elapsed:.1f}s'
        elif frac > 0.005:
            eta = elapsed * (1.0 / frac - 1.0)
            el_s  = f'{elapsed:.0f}s' if elapsed < 60 else f'{elapsed/60:.1f}m'
            if eta < 90:     eta_s = f'{eta:.0f}s'
            elif eta < 5400: eta_s = f'{eta/60:.1f}m'
            else:            eta_s = f'{eta/3600:.1f}h'
            time_s = f'{el_s} elapsed · ETA {eta_s}'
        else:
            time_s = '…'

        right  = f'  {frac*100:4.1f}%  {time_s}'
        prefix = f'\r[{bar}] '
        avail  = cols - len(prefix) - len(right) - 2
        desc   = self.desc if avail > 0 else ''
        if len(desc) > avail:
            desc = desc[:max(avail - 1, 0)] + '…'
        line = prefix + desc.ljust(max(avail, 0)) + right
        sys.stdout.write(line)
        sys.stdout.flush()

    def finish(self, msg: str = 'Done'):
        self.done = self.total
        self.desc = msg
        self._render()
        sys.stdout.write('\n')
        sys.stdout.flush()

# ── Download helpers ───────────────────────────────────────────────────────────

def _download_progress(label: str):
    """Return a urlretrieve reporthook that draws an inline progress bar."""
    bar_w = 28
    start = time.perf_counter()
    def hook(count, block_size, total_size):
        if total_size <= 0:
            return
        done  = min(count * block_size, total_size)
        frac  = done / total_size
        filled = int(bar_w * frac)
        bar   = '█' * filled + '░' * (bar_w - filled)
        elapsed = time.perf_counter() - start
        speed = done / elapsed / 1e6 if elapsed > 0.1 else 0.0
        sys.stdout.write(f'\r  {label}  [{bar}]  {frac*100:4.1f}%  {speed:.1f} MB/s  ')
        sys.stdout.flush()
    return hook

def load_hot100() -> list:
    RAW_DIR.mkdir(exist_ok=True)
    if HOT100_CACHE.exists():
        print("  Hot 100: loading from cache …", flush=True)
        with open(HOT100_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)
    print("  Hot 100: downloading (~50 MB) …")
    urllib.request.urlretrieve(HOT100_URL, HOT100_CACHE, _download_progress("Hot 100"))
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

def parse_genre_rows(genre_name: str, filename: str):
    """Parse a genre CSV from raw/. Returns None if the file doesn't exist."""
    csv_path = RAW_DIR / filename
    if not csv_path.exists():
        return None

    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
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

def min_row_date(rows: list) -> str:
    return min((r["date"] for r in rows if r["date"]), default="")

def max_row_date(rows: list) -> str:
    return max((r["date"] for r in rows if r["date"]), default="")

# ── Per-date chart size map ───────────────────────────────────────────────────

def build_chart_size_map(rows: list) -> dict:
    """
    Return {date_str: int} — the number of entries on each chart week
    (= max peak_position seen on that date).
    """
    weekly: dict = {}
    for r in rows:
        d = r.get("date", "")
        p = r.get("peak", 0)
        if d and isinstance(p, int) and p > 0:
            if d not in weekly or p > weekly[d]:
                weekly[d] = p
    return weekly

# ── Deduplication with optional date filter ───────────────────────────────────

def deduplicate_rows(rows: list, since: str = None, until: str = None,
                     chart_sizes: dict = None) -> dict:
    """
    Aggregate rows into artist_songs dict:
      artist -> [{song, weeks, peak_score, first_year}]

    weeks      = chart-week appearances in the date window.
    peak_score = best (highest) score in the window, where
                 score = chart_size_on_that_date - peak_position.
                 Using per-week chart sizes ensures fair comparison across
                 eras when the chart length changed (e.g. AC: 30→40 in 2004).
    first_year = calendar year of first chart appearance in the window.
    """
    raw = {}   # (artist, song) -> [count, best_peak_score, first_date]
    for r in rows:
        if since and r["date"] < since:
            continue
        if until and r["date"] > until:
            continue
        peak = r.get("peak", 0)
        if not isinstance(peak, int) or peak <= 0:
            continue
        # Per-week chart size → score for this appearance
        cs    = chart_sizes.get(r["date"], 0) if chart_sizes else 0
        score = max(cs - peak, 0)
        key   = (r["artist"], r["song"])
        if key not in raw:
            raw[key] = [1, score, r["date"]]
        else:
            raw[key][0] += 1
            if score > raw[key][1]:
                raw[key][1] = score  # keep best (highest) score

    artist_songs = defaultdict(list)
    for (artist, song), (weeks, peak_score, first_date) in raw.items():
        first_year = int(first_date[:4]) if first_date and len(first_date) >= 4 else None
        artist_songs[artist].append({
            "song": song, "weeks": weeks,
            "peak_score": peak_score, "first_year": first_year,
        })
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

def hindex_peak(songs: list) -> int:
    """Largest h s.t. h songs have peak_score ≥ h."""
    scores = sorted((s["peak_score"] for s in songs), reverse=True)
    h = 0
    for i, s in enumerate(scores, start=1):
        if s >= i: h = i
        else: break
    return h

def compute_chart_size(chart_sizes: dict, since: str = None) -> int:
    """
    Return the maximum chart entries per week within the period (for axis labels
    and scaling).  Uses the per-date chart-size map, not the artist_songs dict.
    """
    if not chart_sizes:
        return 100
    vals = [v for d, v in chart_sizes.items() if not since or d >= since]
    return max(vals) if vals else 100

def hh_index(ranking) -> int:
    """Largest h s.t. h artists have h-index ≥ h."""
    values = sorted((r[1] for r in ranking), reverse=True)
    h = 0
    for i, v in enumerate(values, start=1):
        if v >= i: h = i
        else: break
    return h

# ── Rankings ───────────────────────────────────────────────────────────────────

def compute_rankings(artist_songs: dict):
    """Returns (weeks_ranking, peak_ranking) — sorted lists of (artist, h, n_songs)."""
    weeks_ranking = []
    peak_ranking  = []
    for artist, songs in artist_songs.items():
        hw = hindex_weeks(songs)
        hp = hindex_peak(songs)
        n  = len(songs)
        weeks_ranking.append((artist, hw, n))
        peak_ranking.append( (artist, hp, n))
    weeks_ranking.sort(key=lambda x: (-x[1], -x[2]))
    peak_ranking.sort( key=lambda x: (-x[1], -x[2]))
    return weeks_ranking, peak_ranking

# ── Artist timelines ──────────────────────────────────────────────────────────

def compute_artist_timelines(artists: list, rows: list, metric: str,
                             period_since: str, latest_date: str,
                             chart_sizes: dict = None, tick=None) -> dict:
    """
    Walk each artist's chart appearances chronologically, recording (date, h) only
    when the h-index changes.  Gives weekly precision with minimal storage.

    Returns: {artist: {"d": [date_str, ...], "h": [int, ...]}}
    Both arrays are parallel; "d" holds the ISO date of each h-index change.

    chart_sizes: {date: int} per-week chart depth map for correct peak scoring.
    """
    # Group rows by artist, pre-filtered by period_since
    rows_by_artist: dict = defaultdict(list)
    for r in rows:
        if period_since and r["date"] < period_since:
            continue
        rows_by_artist[r["artist"]].append(r)

    result = {}

    for artist in artists:
        artist_rows = sorted(rows_by_artist.get(artist, []), key=lambda r: r["date"])
        if not artist_rows:
            if tick: tick(artist)
            continue

        song_weeks: dict = defaultdict(int)   # song -> cumulative weeks
        song_score: dict = {}                 # song -> best peak_score so far
        change_dates: list = []
        change_h:     list = []
        prev_h = -1

        for r in artist_rows:
            song = r["song"]
            if metric == "weeks":
                song_weeks[song] += 1
                counts = sorted(song_weeks.values(), reverse=True)
                h = 0
                for i, w in enumerate(counts, 1):
                    if w >= i: h = i
                    else:      break
            else:
                cs    = chart_sizes.get(r["date"], 0) if chart_sizes else 0
                pk    = r.get("peak", 0)
                score = max(cs - pk, 0) if isinstance(pk, int) and 0 < pk <= cs else 0
                old   = song_score.get(song, -1)
                if score > old:
                    song_score[song] = score
                scores = sorted(song_score.values(), reverse=True)
                h = 0
                for i, s in enumerate(scores, 1):
                    if s >= i: h = i
                    else:      break

            if h != prev_h:
                change_dates.append(r["date"])
                change_h.append(h)
                prev_h = h

        if change_dates:
            result[artist] = {"d": change_dates, "h": change_h}
        if tick:
            tick(artist)

    return result


# ── Date arithmetic ───────────────────────────────────────────────────────────
# (No computation needed — periods are absolute ISO date strings or None.)

# ── Console output ─────────────────────────────────────────────────────────────

def print_ranking(title: str, ranking, top_n: int):
    print(f"\n{'═'*62}\n  {title}\n{'═'*62}")
    print(f"  {'#':>4}  {'H':>4}  {'Songs':>6}  Artist")
    print(f"  {'-'*4}  {'-'*4}  {'-'*6}  {'-'*40}")
    for rank, (artist, h, n) in enumerate(ranking[:top_n], 1):
        print(f"  {rank:>4}  {h:>4}  {n:>6}  {artist}")

def save_csv(filename, ranking, header_h: str):
    """Write CSV atomically."""
    path = Path(filename)
    tmp  = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(f"rank,{header_h},total_songs,artist\n")
            for rank, (artist, h, n) in enumerate(ranking, 1):
                safe = artist.replace('"', '""')
                f.write(f'{rank},{h},{n},"{safe}"\n')
        os.replace(tmp, path)
    except:
        tmp.unlink(missing_ok=True)
        raise

# ── Data JSON ──────────────────────────────────────────────────────────────────

def curve_values(artist: str, artist_songs: dict, metric: str):
    songs = artist_songs[artist]
    if metric == "weeks":
        triples = sorted(
            ((s["weeks"], s["song"], s.get("first_year")) for s in songs),
            reverse=True
        )
    else:
        triples = sorted(
            ((s["peak_score"], s["song"], s.get("first_year")) for s in songs
             if s.get("peak_score", 0) >= 1),
            reverse=True
        )
    triples = [(v, nm, yr) for v, nm, yr in triples if v >= 1][:160]
    return ([v for v, _, _ in triples],
            [nm for _, nm, _ in triples],
            [yr for _, _, yr in triples])

def build_chart_payload(weeks_ranking, peak_ranking, artist_songs, hhw, hhp,
                        chart_size: int = 100, rows: list = None, latest_date: str = "",
                        period_since: str = None, tick_w=None, tick_p=None,
                        chart_sizes: dict = None) -> dict:
    def plot_data(ranking, metric):
        out = []
        for i, (a, h, n) in enumerate(ranking[:TOP_PLOT]):
            vals, names, years = curve_values(a, artist_songs, metric)
            out.append({"artist": a, "h": h, "n": n,
                        "color": COLORS[i], "values": vals, "songs": names, "years": years})
        return out

    # ── Timelines + velocity for all table artists ────────────────────────────
    table_w = weeks_ranking[:TOP_TABLE]
    table_p = peak_ranking[:TOP_TABLE]
    all_artists = list({a for a, _, _ in table_w} | {a for a, _, _ in table_p})

    timelines: dict = {}
    if rows:
        w_tl = compute_artist_timelines(all_artists, rows, "weeks", period_since, latest_date,
                                        chart_sizes=chart_sizes, tick=tick_w)
        p_tl = compute_artist_timelines(all_artists, rows, "peak",  period_since, latest_date,
                                        chart_sizes=chart_sizes, tick=tick_p)
        for artist in all_artists:
            entry = {}
            if artist in w_tl: entry["w"] = w_tl[artist]
            if artist in p_tl: entry["p"] = p_tl[artist]
            if entry:
                timelines[artist] = entry

    # Derive velocity from weekly change-point timelines
    # velocity = h_now - h_one_year_before_latest_date
    from datetime import date as _date, timedelta as _td
    def _one_year_ago(latest: str) -> str:
        try:
            d = _date.fromisoformat(latest)
            return str(d - _td(days=365))
        except Exception:
            return ""

    one_year_ago = _one_year_ago(latest_date)

    def get_velocity(artist, tl_dict):
        tl = tl_dict.get(artist)
        if not tl:
            return 0
        dates = tl["d"]
        hs    = tl["h"]
        if not dates:
            return 0
        h_now = hs[-1]
        # Find h-value at one_year_ago (last change-point <= one_year_ago)
        h_ago = 0
        for d, h in zip(dates, hs):
            if d <= one_year_ago:
                h_ago = h
            else:
                break
        return max(0, h_now - h_ago)

    w_vel = {a: get_velocity(a, w_tl) for a, _, _ in table_w} if rows else {}
    p_vel = {a: get_velocity(a, p_tl) for a, _, _ in table_p} if rows else {}

    return {
        "hhw": hhw, "hhp": hhp,
        "chart_size": chart_size,
        "latest_date": latest_date,
        "weeks":      plot_data(weeks_ranking, "weeks"),
        "peak":       plot_data(peak_ranking,  "peak"),
        "weeksTable": [[r, a, h, n, w_vel.get(a, 0)] for r, (a, h, n) in enumerate(table_w, 1)],
        "peakTable":  [[r, a, h, n, p_vel.get(a, 0)] for r, (a, h, n) in enumerate(table_p, 1)],
        "timelines":  timelines,
    }

def save_chart_data(payload: dict, path: Path):
    """Write JSON atomically: write to .tmp then rename, so Ctrl-C can't corrupt."""
    path.parent.mkdir(exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"))
        os.replace(tmp, path)
    except:
        tmp.unlink(missing_ok=True)
        raise

# ── HTML patching ──────────────────────────────────────────────────────────────

def update_html_genre_summary(genre_summary: list):
    """Patch the GENRE_SUMMARY constant in bibbloard.html in-place."""
    html_path = Path(__file__).parent / "bibbloard.html"
    if not html_path.exists():
        print(f"  ⚠ {html_path} not found — skipping HTML update")
        return
    text = html_path.read_text(encoding="utf-8")
    genre_json = json.dumps(genre_summary, separators=(",", ":"))
    lines = text.splitlines(keepends=True)
    replaced = False
    for i, line in enumerate(lines):
        if re.match(r"\s*const GENRE_SUMMARY\s*=", line):
            lines[i] = f"const GENRE_SUMMARY = {genre_json};\n"
            replaced = True
            break
    if not replaced:
        print("  ⚠ GENRE_SUMMARY not found in bibbloard.html — no changes made")
    else:
        # Atomic write: write to .tmp then rename
        tmp = html_path.with_suffix(".tmp")
        try:
            tmp.write_text("".join(lines), encoding="utf-8")
            os.replace(tmp, html_path)
        except:
            tmp.unlink(missing_ok=True)
            raise



# ── Main ───────────────────────────────────────────────────────────────────────

def _count_artists(rows: list, since: str) -> int:
    """Count unique artists in rows, respecting the period filter."""
    seen = set()
    for r in rows:
        if since and r["date"] < since:
            continue
        seen.add(r["artist"])
    return min(len(seen), TOP_TABLE)


def main():
    DATA_DIR.mkdir(exist_ok=True)

    # ── Load data ─────────────────────────────────────────────────────────────
    print("Loading data …")
    charts      = load_hot100()
    hot100_rows = parse_hot100_rows(charts)
    hot100_max  = max_row_date(hot100_rows)
    hot100_min  = min_row_date(hot100_rows)
    print(f"  Hot 100: {len(hot100_rows):,} entries  ({hot100_min} → {hot100_max})")

    # Parse all genre CSVs upfront so we can count total work below
    all_genre_charts = GENRE_CHARTS_CORE + GENRE_CHARTS_OPTIONAL
    loaded_genres: list = []   # [(genre_name, key, rows, min_date, max_date)]
    skipped: list = []
    for genre_name, csv_filename in all_genre_charts:
        rows = parse_genre_rows(genre_name, csv_filename)
        if rows is None or len(rows) == 0:
            skipped.append(genre_name)
            continue
        key = CHART_KEYS[genre_name]
        loaded_genres.append((genre_name, key, rows, min_row_date(rows), max_row_date(rows)))

    present = [g[0] for g in loaded_genres]
    print(f"  Genre CSVs: {', '.join(present) or 'none'}")
    if skipped:
        print(f"  Skipped:    {', '.join(skipped)}")

    # ── Build per-week chart-size maps (used for fair peak scoring) ──────────
    hot100_chart_sizes = build_chart_size_map(hot100_rows)
    genre_chart_sizes  = {key: build_chart_size_map(rows)
                          for _, key, rows, _, _ in loaded_genres}

    # ── Export all-time Hot 100 CSVs (quick, before progress bar) ────────────
    hot100_all     = deduplicate_rows(hot100_rows, chart_sizes=hot100_chart_sizes)
    hot100_size    = compute_chart_size(hot100_chart_sizes)
    wr_all, pr_all = compute_rankings(hot100_all)
    OUTPUT_DIR.mkdir(exist_ok=True)
    save_csv(OUTPUT_DIR / "bibbloard_weeks.csv", wr_all, "weeks_hindex")
    save_csv(OUTPUT_DIR / "bibbloard_peak.csv",  pr_all, "peak_hindex")

    # ── Pre-count total timeline-artist ticks for accurate ETA ───────────────
    total_ticks = 0
    for _, period_since in PERIODS:
        total_ticks += 2 * _count_artists(hot100_rows, period_since)
    for _, _, rows, _, _ in loaded_genres:
        for _, period_since in PERIODS:
            total_ticks += 2 * _count_artists(rows, period_since)

    # ── Main computation loop with progress bar ───────────────────────────────
    progress = Progress(total_ticks)
    # Shared label state — updated just before each compute_artist_timelines call
    _lbl = {"chart": "", "period": "", "metric": ""}

    def tick_w(artist: str):
        _lbl["metric"] = "weeks"
        progress.update(1, f"{_lbl['chart']} · {_lbl['period']} · weeks · {artist}")

    def tick_p(artist: str):
        _lbl["metric"] = "peak"
        progress.update(1, f"{_lbl['chart']} · {_lbl['period']} · peak  · {artist}")

    try:
        # ── Hot 100 ──────────────────────────────────────────────────────────
        hot100_periods: dict = {}
        _lbl["chart"] = "Hot 100"
        for period_key, period_since in PERIODS:
            _lbl["period"] = period_key
            artist_songs = deduplicate_rows(hot100_rows, period_since,
                                            chart_sizes=hot100_chart_sizes)
            if not artist_songs:
                continue
            cs       = compute_chart_size(hot100_chart_sizes, period_since)
            wr, pr   = compute_rankings(artist_songs)
            hhw      = hh_index(wr)
            hhp      = hh_index(pr)
            hot100_periods[period_key] = {"hhw": hhw, "hhp": hhp}
            fname = "hot100.json" if period_key == "all" else f"hot100_{period_key}.json"
            save_chart_data(
                build_chart_payload(wr, pr, artist_songs, hhw, hhp, cs,
                                    rows=hot100_rows, latest_date=hot100_max,
                                    period_since=period_since,
                                    tick_w=tick_w, tick_p=tick_p,
                                    chart_sizes=hot100_chart_sizes),
                DATA_DIR / fname)

        # ── Genre charts ─────────────────────────────────────────────────────
        genre_summary: list = []
        for genre_name, key, genre_rows, genre_min, genre_max in loaded_genres:
            _lbl["chart"] = genre_name
            genre_periods: dict = {}
            gchart_sizes  = genre_chart_sizes[key]
            for period_key, period_since in PERIODS:
                _lbl["period"] = period_key
                artist_songs = deduplicate_rows(genre_rows, period_since,
                                               chart_sizes=gchart_sizes)
                if not artist_songs:
                    continue
                cs     = compute_chart_size(gchart_sizes, period_since)
                wr, pr = compute_rankings(artist_songs)
                hhw    = hh_index(wr)
                hhp    = hh_index(pr)
                genre_periods[period_key] = {"hhw": hhw, "hhp": hhp}
                fname  = f"{key}.json" if period_key == "all" else f"{key}_{period_key}.json"
                save_chart_data(
                    build_chart_payload(wr, pr, artist_songs, hhw, hhp, cs,
                                        rows=genre_rows, latest_date=genre_max,
                                        period_since=period_since,
                                        tick_w=tick_w, tick_p=tick_p,
                                        chart_sizes=gchart_sizes),
                    DATA_DIR / fname)
            genre_summary.append({
                "genre": genre_name, "key": key, "periods": genre_periods,
                "earliest": genre_min, "latest": genre_max,
            })

    except KeyboardInterrupt:
        sys.stdout.write('\n')
        print("\nInterrupted — partial files cleaned up (atomic writes).")
        sys.exit(1)

    progress.finish("All chart files written")

    # ── Genre summary ─────────────────────────────────────────────────────────
    genre_summary.append({
        "genre": "Hot 100", "key": "hot100", "periods": hot100_periods,
        "earliest": hot100_min, "latest": hot100_max,
    })
    genre_summary.sort(key=lambda g: (-g["periods"]["all"]["hhw"], -g["periods"]["all"]["hhp"]))

    print(f"\n{'═'*56}")
    print(f"  {'Genre':<22}  {'Coverage':>14}  {'Weeks HH':>9}  {'Peak HH':>8}")
    print(f"  {'-'*22}  {'-'*14}  {'-'*9}  {'-'*8}")
    for i, g in enumerate(genre_summary, 1):
        hhw  = g["periods"]["all"]["hhw"]
        hhp  = g["periods"]["all"]["hhp"]
        span = f"{g['earliest'][:4]}–{g['latest'][:4]}"
        print(f"  {i}. {g['genre']:<20}  {span:>14}  {hhw:>9}  {hhp:>8}")

    update_html_genre_summary(genre_summary)
    print("\nDone ✓  open: http://localhost:7433/bibbloard.html")


if __name__ == "__main__":
    main()
