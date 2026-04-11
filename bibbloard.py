#!/usr/bin/env python3
"""
Bibbloard — Billboard H-Index Calculator
==============================
Computes three h-index variants for every artist in the Billboard Hot 100 history
(1958–present) and for twelve genre charts, using open datasets.

H-Index definitions (by analogy with the academic h-index):
  - Weeks h-index      : artist has h songs each appearing on the chart for ≥ h weeks
  - Peak h-index       : artist has h songs each with chart score ≥ h
                         (chart score = chart_size − peak_position, so Hot 100 #1 → 99)
  - Integrated h-index : artist has h songs each with ≥ h peak-equivalent weeks on chart
                         (sum of (chart_size − position)/chart_size per charting week;
                          normalised per week so chart size changes don't distort scores)

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
                woc        = int(row.get("weeks_on_chart") or 1)
            except (ValueError, TypeError):
                continue
            if peak <= 0:
                peak = 101
            if woc < 1:
                woc = 1
            rows.append({"artist": artist, "song": title,
                         "peak": peak, "date": chart_date, "woc": woc})
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
                     chart_sizes: dict = None, unified_cs: int = None) -> dict:
    """
    Aggregate rows into artist_songs dict:
      artist -> [{song, weeks, peak_score, first_year}]

    weeks      = chart-week appearances in the date window.
    peak_score = best (highest) score in the window, where
                 score = eff_cs - peak_position.
    integrated_score = cumulative (eff_cs − pos) / eff_cs per week.
    eff_cs     = unified_cs if set, else the actual per-week chart size.
                 Setting unified_cs=100 enables fair cross-chart comparison.
    first_year = calendar year of first chart appearance in the window.
    """
    raw = {}   # (artist, song) -> [count, best_peak_score, integrated_score, first_date]
    for r in rows:
        if since and r["date"] < since:
            continue
        if until and r["date"] > until:
            continue
        peak = r.get("peak", 0)
        if not isinstance(peak, int) or peak <= 0:
            continue
        cs = chart_sizes.get(r["date"], 0) if chart_sizes else 0
        if cs == 0 or peak > cs:       # skip entries with no valid chart-size data
            continue
        eff_cs = unified_cs if unified_cs is not None else cs
        score  = max(eff_cs - peak, 0)
        key    = (r["artist"], r["song"])
        norm   = score / eff_cs        # normalized: (eff_cs−pos)/eff_cs per week
        if key not in raw:
            raw[key] = [1, score, norm, r["date"]]
        else:
            raw[key][0] += 1
            if score > raw[key][1]:
                raw[key][1] = score  # keep best (highest) peak score
            raw[key][2] += norm      # accumulate normalized integrated score

    artist_songs = defaultdict(list)
    for (artist, song), (weeks, peak_score, integrated_score, first_date) in raw.items():
        first_year = int(first_date[:4]) if first_date and len(first_date) >= 4 else None
        artist_songs[artist].append({
            "song": song, "weeks": weeks,
            "peak_score": peak_score, "integrated_score": integrated_score,
            "first_year": first_year,
        })
    return dict(artist_songs)

def deduplicate_snapshot(rows: list, chart_sizes: dict = None, unified_cs: int = None) -> dict:
    """
    Build artist_songs from a single-week snapshot using weeks_on_chart + peak_position.

    weeks h-index and peak h-index are exact.
    integrated_score is estimated as woc * peak_score / eff_cs
    (assumes each week was at peak position — an overestimate but good enough for ranking).
    """
    cs = max(chart_sizes.values()) if chart_sizes else max(
        (r.get("peak", 0) for r in rows), default=50)
    eff_cs = unified_cs if unified_cs is not None else cs

    from datetime import date as _d, timedelta as _td
    artist_songs: dict = defaultdict(list)
    seen: set = set()
    for r in rows:
        key = (r["artist"], r["song"])
        if key in seen:
            continue
        seen.add(key)
        peak = r.get("peak", 0)
        woc  = r.get("woc", 1)
        if not isinstance(peak, int) or peak <= 0 or peak > cs:
            continue
        score      = max(eff_cs - peak, 0)
        integrated = woc * score / eff_cs if eff_cs > 0 else 0
        # Approximate first year from snapshot date minus woc weeks
        first_year = None
        if r.get("date"):
            try:
                snap = _d.fromisoformat(r["date"])
                first_year = (snap - _td(weeks=woc - 1)).year
            except Exception:
                pass
        artist_songs[r["artist"]].append({
            "song": r["song"], "weeks": woc,
            "peak_score": score, "integrated_score": integrated,
            "first_year": first_year,
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

def hindex_integrated(songs: list) -> float:
    """Real-valued h s.t. h songs have integrated_score ≥ h.
    Integer part: standard h-index on float scores.
    Fractional part: linear interpolation of where the score curve crosses y=x,
    i.e. h += excess / (excess + deficit) where excess = score[h] − h and
    deficit = (h+1) − score[h+1].  Equivalent to the bibliometric h-fraction."""
    scores = sorted((s["integrated_score"] for s in songs), reverse=True)
    h = 0
    for i, s in enumerate(scores, start=1):
        if s >= i:
            h = i
        else:
            if h > 0:
                s_h = scores[h - 1]          # score of the last qualifying song
                h += (s_h - h) / (s_h - s + 1)  # interpolate to diagonal crossing
            break
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
    """Returns (weeks_ranking, peak_ranking, integrated_ranking) — sorted lists of (artist, h, n_songs)."""
    weeks_ranking      = []
    peak_ranking       = []
    integrated_ranking = []
    for artist, songs in artist_songs.items():
        hw = hindex_weeks(songs)
        hp = hindex_peak(songs)
        hi = hindex_integrated(songs)
        n  = len(songs)
        weeks_ranking.append((artist, hw, n))
        peak_ranking.append( (artist, hp, n))
        integrated_ranking.append((artist, hi, n))
    weeks_ranking.sort(key=lambda x: (-x[1], -x[2]))
    peak_ranking.sort( key=lambda x: (-x[1], -x[2]))
    integrated_ranking.sort(key=lambda x: (-x[1], -x[2]))
    return weeks_ranking, peak_ranking, integrated_ranking

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

        song_weeks:  dict = defaultdict(int)   # song -> cumulative weeks
        song_score:  dict = {}                 # song -> best peak_score so far
        song_iscore: dict = defaultdict(int)   # song -> cumulative integrated score
        change_dates: list = []
        change_h:     list = []
        change_songs: list = []
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
            elif metric == "integrated":
                cs    = chart_sizes.get(r["date"], 0) if chart_sizes else 0
                pk    = r.get("peak", 0)
                score = max(cs - pk, 0) if isinstance(pk, int) and 0 < pk <= cs else 0
                song_iscore[song] += score / cs if cs > 0 else 0
                scores = sorted(song_iscore.values(), reverse=True)
                h_int = 0
                for i, s in enumerate(scores, 1):
                    if s >= i: h_int = i
                    else:      break
                # Interpolate real-valued h-fraction at the curve/diagonal crossing
                if h_int > 0 and h_int < len(scores):
                    f_h  = scores[h_int - 1]   # score at rank h_int
                    f_h1 = scores[h_int]        # score at rank h_int+1
                    delta = f_h1 - f_h          # ≤ 0 (sorted descending)
                    h = (f_h - h_int * delta) / (1 - delta) if delta != -1 else float(h_int)
                else:
                    h = float(h_int)
            else:  # peak
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
                change_songs.append(song)
                prev_h = h

        # Merge consecutive change-points on the same date: keep the final h,
        # accumulate all triggering songs so the tooltip can list them all.
        if change_dates:
            md, mh, ms = [change_dates[0]], [change_h[0]], [[change_songs[0]]]
            for d, h, s in zip(change_dates[1:], change_h[1:], change_songs[1:]):
                if d == md[-1]:
                    mh[-1] = h          # update to latest h value
                    ms[-1].append(s)    # accumulate song
                else:
                    md.append(d); mh.append(h); ms.append([s])
            result[artist] = {"d": md, "h": mh, "s": ms}
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

def save_csv(filename, weeks_ranking, peak_ranking, int_ranking):
    """Write unified CSV with all three h-indices, atomically."""
    path = Path(filename)
    tmp  = path.with_suffix(".tmp")
    # Build artist → (h, n) maps for peak and integrated
    peak_map = {artist: (h, n) for artist, h, n in peak_ranking}
    int_map  = {artist: (h, n) for artist, h, n in int_ranking}
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write("rank,weeks_hindex,peak_hindex,integrated_hindex,total_songs,artist\n")
            for rank, (artist, wh, n) in enumerate(weeks_ranking, 1):
                ph, _ = peak_map.get(artist, (0, n))
                ih, _ = int_map.get(artist, (0, n))
                safe  = artist.replace('"', '""')
                f.write(f'{rank},{wh},{ph},{ih:.2f},{n},"{safe}"\n')
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
    elif metric == "integrated":
        triples = sorted(
            ((s["integrated_score"], s["song"], s.get("first_year")) for s in songs),
            reverse=True
        )
    else:
        triples = sorted(
            ((s["peak_score"], s["song"], s.get("first_year")) for s in songs),
            reverse=True
        )
    triples = triples[:160]
    return ([v for v, _, _ in triples],
            [nm for _, nm, _ in triples],
            [yr for _, _, yr in triples])

def build_chart_payload(weeks_ranking, peak_ranking, integrated_ranking, artist_songs, hhw, hhp, hhi,
                        chart_size: int = 100, rows: list = None, latest_date: str = "",
                        period_since: str = None, tick_w=None, tick_p=None, tick_i=None,
                        chart_sizes: dict = None, unified_cs: int = None) -> dict:
    def plot_data(ranking, metric, songs=None):
        if songs is None: songs = artist_songs
        out = []
        for i, (a, h, n) in enumerate(ranking[:TOP_PLOT]):
            vals, names, years = curve_values(a, songs, metric)
            out.append({"artist": a, "h": h, "n": n,
                        "color": COLORS[i], "values": vals, "songs": names, "years": years})
        return out

    # ── Per-song weekly positions for integrated mini-chart popups ───────────
    # Cover all artists in the combined table (not just top-30), because any
    # expanded/highlighted artist can be injected into the integrated plot.
    int_song_pos = {}
    if rows:
        i30_set = (
            {a for a, _, _ in integrated_ranking[:TOP_TABLE]} |
            {a for a, _, _ in weeks_ranking[:TOP_TABLE]} |
            {a for a, _, _ in peak_ranking[:TOP_TABLE]}
        )
        i30_needed = {
            a: {s["song"] for s in artist_songs[a]}
            for a in i30_set
        }
        raw_pos = {a: defaultdict(list) for a in i30_set}
        for r in rows:
            a = r["artist"]
            if a not in i30_set: continue
            song = r["song"]
            if song not in i30_needed.get(a, ()): continue
            date = r["date"]
            if period_since and date < period_since: continue
            pos = r.get("peak")
            cs  = chart_sizes.get(date, 0) if chart_sizes else 0
            if not isinstance(pos, int) or pos <= 0 or pos > cs or cs == 0: continue
            raw_pos[a][song].append((date, pos, cs))
        for a in i30_set:
            ad = {}
            for song, wdata in raw_pos[a].items():
                sd = sorted(wdata)
                peak_pos = min(p for _, p, _ in sd)
                scores   = [round((cs - p) / cs, 3) for _, p, cs in sd]
                ad[song] = {"s": scores, "pk": peak_pos}
            if ad:
                int_song_pos[a] = ad

    # ── Timelines + velocity for all table artists ────────────────────────────
    table_w = weeks_ranking[:TOP_TABLE]
    table_p = peak_ranking[:TOP_TABLE]
    table_i = integrated_ranking[:TOP_TABLE]
    all_artists = list({a for a, _, _ in table_w} | {a for a, _, _ in table_p}
                       | {a for a, _, _ in table_i})

    timelines: dict = {}
    if rows:
        w_tl = compute_artist_timelines(all_artists, rows, "weeks",      period_since, latest_date,
                                        chart_sizes=chart_sizes, tick=tick_w)
        p_tl = compute_artist_timelines(all_artists, rows, "peak",       period_since, latest_date,
                                        chart_sizes=chart_sizes, tick=tick_p)
        i_tl = compute_artist_timelines(all_artists, rows, "integrated", period_since, latest_date,
                                        chart_sizes=chart_sizes, tick=tick_i)
        for artist in all_artists:
            entry = {}
            if artist in w_tl: entry["w"] = w_tl[artist]
            if artist in p_tl: entry["p"] = p_tl[artist]
            if artist in i_tl: entry["i"] = i_tl[artist]
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

    # Velocities for all union artists (timelines were computed for all_artists already)
    w_vel = {a: get_velocity(a, w_tl) for a in all_artists} if rows else {}
    p_vel = {a: get_velocity(a, p_tl) for a in all_artists} if rows else {}
    i_vel = {a: get_velocity(a, i_tl) for a in all_artists} if rows else {}

    # Full-ranking lookup maps so cross-table joins are accurate
    wh_map = {a: h for a, h, n in weeks_ranking}
    ph_map = {a: h for a, h, n in peak_ranking}
    ih_map = {a: h for a, h, n in integrated_ranking}
    n_map  = {a: n for a, h, n in weeks_ranking}   # n is identical across all rankings

    # Single combined table covering the union of the three top-200 lists,
    # with correct h-indices from full rankings (not just top-200 lookups).
    union_artists = sorted(
        {a for a, _, _ in table_w} | {a for a, _, _ in table_p} | {a for a, _, _ in table_i},
        key=lambda a: (-ih_map.get(a, 0), -wh_map.get(a, 0), -ph_map.get(a, 0), a)
    )
    combined_table = [
        [r, a,
         ih_map.get(a, 0), i_vel.get(a, 0),
         wh_map.get(a, 0), w_vel.get(a, 0),
         ph_map.get(a, 0), p_vel.get(a, 0),
         n_map.get(a, 0)]
        for r, a in enumerate(union_artists, 1)
    ]

    # Compact curve values for table artists not already in the top-N plot data.
    # These let the frontend draw a curve for any filtered/highlighted artist.
    w_top_set = {a for a, _, _ in weeks_ranking[:TOP_PLOT]}
    p_top_set = {a for a, _, _ in peak_ranking[:TOP_PLOT]}
    i_top_set = {a for a, _, _ in integrated_ranking[:TOP_PLOT]}
    weeks_extra      = {a: dict(zip(("v","s","y"), curve_values(a, artist_songs, "weeks")))
                        for a in union_artists if a not in w_top_set}
    peak_extra       = {a: dict(zip(("v","s","y"), curve_values(a, artist_songs, "peak")))
                        for a in union_artists if a not in p_top_set}
    integrated_extra = {a: dict(zip(("v","s","y"), curve_values(a, artist_songs, "integrated")))
                        for a in union_artists if a not in i_top_set}

    # ── Unified chart size variant (cs=unified_cs for cross-chart comparison) ──
    u: dict = {}
    if unified_cs is not None and rows:
        u_artist_songs = deduplicate_rows(rows, period_since, chart_sizes=chart_sizes,
                                          unified_cs=unified_cs)
        _, u_pr, u_ir = compute_rankings(u_artist_songs)
        u_hhp = hh_index(u_pr)
        u_hhi = hh_index(u_ir)

        u_table_p = u_pr[:TOP_TABLE]
        u_table_i = u_ir[:TOP_TABLE]
        u_ph_map  = {a: h for a, h, n in u_pr}
        u_ih_map  = {a: h for a, h, n in u_ir}
        u_p_top   = {a for a, _, _ in u_pr[:TOP_PLOT]}
        u_i_top   = {a for a, _, _ in u_ir[:TOP_PLOT]}

        u_union = sorted(
            {a for a, _, _ in u_table_p} | {a for a, _, _ in u_table_i}
            | {a for a, _, _ in weeks_ranking[:TOP_TABLE]},
            key=lambda a: (-u_ih_map.get(a, 0), -wh_map.get(a, 0), -u_ph_map.get(a, 0), a)
        )
        u_n_map = {a: len(songs) for a, songs in u_artist_songs.items()}
        u_combined_table = [
            [r, a,
             u_ih_map.get(a, 0), i_vel.get(a, 0),
             wh_map.get(a, 0),   w_vel.get(a, 0),
             u_ph_map.get(a, 0), p_vel.get(a, 0),
             u_n_map.get(a, 0)]
            for r, a in enumerate(u_union, 1)
        ]

        # Extra compact curves for highlighted artists not in top-N unified plots
        u_peak_extra = {a: dict(zip(("v","s","y"), curve_values(a, u_artist_songs, "peak")))
                        for a in u_union if a not in u_p_top and a in u_artist_songs}
        u_int_extra  = {a: dict(zip(("v","s","y"), curve_values(a, u_artist_songs, "integrated")))
                        for a in u_union if a not in u_i_top and a in u_artist_songs}

        # Per-song weekly positions for unified integrated mini-chart popups
        # Cover all u_union artists so injected/highlighted artists work too
        u_i30_set    = set(u_union)
        u_i30_needed = {
            a: {s["song"] for s in u_artist_songs[a]}
            for a in u_i30_set if a in u_artist_songs
        }
        u_raw_pos = {a: defaultdict(list) for a in u_i30_set}
        for r in rows:
            a = r["artist"]
            if a not in u_i30_set: continue
            song = r["song"]
            if song not in u_i30_needed.get(a, ()): continue
            date = r["date"]
            if period_since and date < period_since: continue
            pos = r.get("peak")
            cs  = chart_sizes.get(date, 0) if chart_sizes else 0
            if not isinstance(pos, int) or pos <= 0 or pos > cs or cs == 0: continue
            u_raw_pos[a][song].append((date, pos, cs))
        u_int_song_pos: dict = {}
        for a in u_i30_set:
            ad = {}
            for song, wdata in u_raw_pos.get(a, {}).items():
                sd       = sorted(wdata)
                peak_pos = min(p for _, p, _ in sd)
                # Scores normalised to unified_cs instead of per-week actual cs
                scores   = [round((unified_cs - p) / unified_cs, 3) for _, p, _ in sd]
                ad[song] = {"s": scores, "pk": peak_pos}
            if ad:
                u_int_song_pos[a] = ad

        u = {
            "hhp": u_hhp, "hhi": u_hhi, "chart_size": unified_cs,
            "peak":             plot_data(u_pr, "peak",       u_artist_songs),
            "integrated":       plot_data(u_ir, "integrated", u_artist_songs),
            "combinedTable":    u_combined_table,
            "peakCurves":       u_peak_extra,
            "integratedCurves": u_int_extra,
            "intSongPos":       u_int_song_pos,
        }

    return {
        "hhw": hhw, "hhp": hhp, "hhi": hhi,
        "chart_size": chart_size,
        "latest_date": latest_date,
        "weeks":      plot_data(weeks_ranking,      "weeks"),
        "peak":       plot_data(peak_ranking,        "peak"),
        "integrated": plot_data(integrated_ranking,  "integrated"),
        "combinedTable":    combined_table,
        "weeksCurves":      weeks_extra,
        "peakCurves":       peak_extra,
        "integratedCurves": integrated_extra,
        "intSongPos":       int_song_pos,
        "timelines":  timelines,
        "u":          u,
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
    loaded_genres: list = []   # [(genre_name, key, rows, min_date, max_date, is_snapshot)]
    skipped: list = []
    for genre_name, csv_filename in all_genre_charts:
        rows = parse_genre_rows(genre_name, csv_filename)
        if rows is None or len(rows) == 0:
            skipped.append(genre_name)
            continue
        key = CHART_KEYS[genre_name]
        is_snapshot = min_row_date(rows) == max_row_date(rows)
        if is_snapshot:
            # Accept snapshot if weeks_on_chart data is meaningful (some songs > 1 week)
            if not any(r.get("woc", 1) > 1 for r in rows):
                skipped.append(genre_name + " (single week, no history)")
                continue
        loaded_genres.append((genre_name, key, rows, min_row_date(rows), max_row_date(rows), is_snapshot))

    present = [g[0] for g in loaded_genres]
    print(f"  Genre CSVs: {', '.join(present) or 'none'}")
    if skipped:
        print(f"  Skipped:    {', '.join(skipped)}")

    # ── Build per-week chart-size maps (used for fair peak scoring) ──────────
    hot100_chart_sizes = build_chart_size_map(hot100_rows)
    genre_chart_sizes  = {key: build_chart_size_map(rows)
                          for _, key, rows, _, _, _ in loaded_genres}

    # ── Export all-time Hot 100 CSVs (quick, before progress bar) ────────────
    hot100_all             = deduplicate_rows(hot100_rows, chart_sizes=hot100_chart_sizes)
    hot100_size            = compute_chart_size(hot100_chart_sizes)
    wr_all, pr_all, ir_all = compute_rankings(hot100_all)
    OUTPUT_DIR.mkdir(exist_ok=True)
    save_csv(OUTPUT_DIR / "bibbloard_hindex.csv", wr_all, pr_all, ir_all)

    # ── Pre-count total timeline-artist ticks for accurate ETA ───────────────
    total_ticks = 0
    for _, period_since in PERIODS:
        total_ticks += 3 * _count_artists(hot100_rows, period_since)
    for _, _, rows, _, _, is_snap in loaded_genres:
        if is_snap:
            continue  # snapshots skip timeline computation
        for _, period_since in PERIODS:
            total_ticks += 3 * _count_artists(rows, period_since)

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

    def tick_i(artist: str):
        _lbl["metric"] = "integrated"
        progress.update(1, f"{_lbl['chart']} · {_lbl['period']} · intgr · {artist}")

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
            cs          = compute_chart_size(hot100_chart_sizes, period_since)
            wr, pr, ir  = compute_rankings(artist_songs)
            hhw         = hh_index(wr)
            hhp         = hh_index(pr)
            hhi         = hh_index(ir)
            fname   = "hot100.json" if period_key == "all" else f"hot100_{period_key}.json"
            payload = build_chart_payload(wr, pr, ir, artist_songs, hhw, hhp, hhi, cs,
                                          rows=hot100_rows, latest_date=hot100_max,
                                          period_since=period_since,
                                          tick_w=tick_w, tick_p=tick_p, tick_i=tick_i,
                                          chart_sizes=hot100_chart_sizes, unified_cs=100)
            save_chart_data(payload, DATA_DIR / fname)
            hot100_periods[period_key] = {
                "hhw": hhw, "hhp": hhp, "hhi": hhi,
                "u_hhp": payload["u"].get("hhp", hhp),
                "u_hhi": payload["u"].get("hhi", hhi),
            }

        # ── Genre charts ─────────────────────────────────────────────────────
        genre_summary: list = []
        for genre_name, key, genre_rows, genre_min, genre_max, is_snapshot in loaded_genres:
            _lbl["chart"] = genre_name
            genre_periods: dict = {}
            gchart_sizes  = genre_chart_sizes[key]

            if is_snapshot:
                # Single-week snapshot: compute h-indices from woc + peak_position.
                # Only the all-time period is meaningful; windowed periods are skipped.
                artist_songs = deduplicate_snapshot(genre_rows, chart_sizes=gchart_sizes)
                if not artist_songs:
                    continue
                cs         = compute_chart_size(gchart_sizes)
                wr, pr, ir = compute_rankings(artist_songs)
                hhw        = hh_index(wr)
                hhp        = hh_index(pr)
                hhi        = hh_index(ir)
                # Unified variant
                u_songs = deduplicate_snapshot(genre_rows, chart_sizes=gchart_sizes, unified_cs=100)
                _, u_pr, u_ir = compute_rankings(u_songs)
                u_hhp = hh_index(u_pr)
                u_hhi = hh_index(u_ir)
                payload = build_chart_payload(wr, pr, ir, artist_songs, hhw, hhp, hhi, cs,
                                              rows=None, latest_date=genre_max,
                                              period_since=None,
                                              chart_sizes=gchart_sizes, unified_cs=100)
                save_chart_data(payload, DATA_DIR / f"{key}.json")
                genre_periods["all"] = {
                    "hhw": hhw, "hhp": hhp, "hhi": hhi,
                    "u_hhp": u_hhp, "u_hhi": u_hhi,
                }
            else:
                for period_key, period_since in PERIODS:
                    _lbl["period"] = period_key
                    artist_songs = deduplicate_rows(genre_rows, period_since,
                                                   chart_sizes=gchart_sizes)
                    if not artist_songs:
                        continue
                    cs          = compute_chart_size(gchart_sizes, period_since)
                    wr, pr, ir  = compute_rankings(artist_songs)
                    hhw         = hh_index(wr)
                    hhp         = hh_index(pr)
                    hhi         = hh_index(ir)
                    fname   = f"{key}.json" if period_key == "all" else f"{key}_{period_key}.json"
                    payload = build_chart_payload(wr, pr, ir, artist_songs, hhw, hhp, hhi, cs,
                                                  rows=genre_rows, latest_date=genre_max,
                                                  period_since=period_since,
                                                  tick_w=tick_w, tick_p=tick_p, tick_i=tick_i,
                                                  chart_sizes=gchart_sizes, unified_cs=100)
                    save_chart_data(payload, DATA_DIR / fname)
                    genre_periods[period_key] = {
                        "hhw": hhw, "hhp": hhp, "hhi": hhi,
                        "u_hhp": payload["u"].get("hhp", hhp),
                        "u_hhi": payload["u"].get("hhi", hhi),
                    }

            cs_vals  = list(gchart_sizes.values())
            cs_lo, cs_hi = (min(cs_vals), max(cs_vals)) if cs_vals else (100, 100)
            genre_summary.append({
                "genre": genre_name, "key": key, "periods": genre_periods,
                "earliest": genre_min, "latest": genre_max,
                "cs_lo": cs_lo, "cs_hi": cs_hi,
            })

    except KeyboardInterrupt:
        sys.stdout.write('\n')
        print("\nInterrupted — partial files cleaned up (atomic writes).")
        sys.exit(1)

    progress.finish("All chart files written")

    # ── Genre summary ─────────────────────────────────────────────────────────
    h100_cs = list(hot100_chart_sizes.values())
    h100_lo, h100_hi = (min(h100_cs), max(h100_cs)) if h100_cs else (100, 100)
    genre_summary.append({
        "genre": "Hot 100", "key": "hot100", "periods": hot100_periods,
        "earliest": hot100_min, "latest": hot100_max,
        "cs_lo": h100_lo, "cs_hi": h100_hi,
    })
    genre_summary.sort(key=lambda g: (-g["periods"]["all"]["hhw"], -g["periods"]["all"]["hhp"]))

    print(f"\n{'═'*68}")
    print(f"  {'Genre':<22}  {'Coverage':>14}  {'Weeks HH':>9}  {'Peak HH':>8}  {'Int HH':>7}")
    print(f"  {'-'*22}  {'-'*14}  {'-'*9}  {'-'*8}  {'-'*7}")
    for i, g in enumerate(genre_summary, 1):
        hhw  = g["periods"]["all"]["hhw"]
        hhp  = g["periods"]["all"]["hhp"]
        hhi  = g["periods"]["all"].get("hhi", "?")
        span = f"{g['earliest'][:4]}–{g['latest'][:4]}"
        print(f"  {i}. {g['genre']:<20}  {span:>14}  {hhw:>9}  {hhp:>8}  {hhi:>7}")

    update_html_genre_summary(genre_summary)
    print("\nDone ✓  open: http://localhost:7433/bibbloard.html")


if __name__ == "__main__":
    main()
