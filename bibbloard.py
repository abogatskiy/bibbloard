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
import re
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

# ── Download helpers ───────────────────────────────────────────────────────────

def _reporthook(label):
    def hook(count, block_size, total_size):
        if total_size > 0:
            pct = min(count * block_size * 100 // total_size, 100)
            print(f"\r  {label}: {pct}%", end="", flush=True)
    return hook

def load_hot100() -> list:
    RAW_DIR.mkdir(exist_ok=True)
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

def parse_genre_rows(genre_name: str, filename: str):
    """Parse a genre CSV from raw/. Returns None if the file doesn't exist."""
    csv_path = RAW_DIR / filename
    if not csv_path.exists():
        return None

    print(f"Parsing {genre_name} …", flush=True)
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

# ── HTML patching ──────────────────────────────────────────────────────────────

def update_html_genre_summary(genre_summary: list):
    """Patch the GENRE_SUMMARY constant in bibbloard.html in-place."""
    html_path = Path(__file__).parent / "bibbloard.html"
    if not html_path.exists():
        print(f"  ⚠ {html_path} not found — skipping HTML update")
        return
    text = html_path.read_text(encoding="utf-8")
    genre_json = json.dumps(genre_summary, separators=(",", ":"))
    new_text, n = re.subn(
        r"(const GENRE_SUMMARY\s*=\s*)(\[.*?\])(;)",
        rf"\g<1>{re.escape(genre_json)}\3",
        text,
        flags=re.DOTALL,
    )
    if n == 0:
        print("  ⚠ GENRE_SUMMARY not found in bibbloard.html — no changes made")
    else:
        html_path.write_text(new_text, encoding="utf-8")
        print("  Updated → bibbloard.html (GENRE_SUMMARY)")



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
    OUTPUT_DIR.mkdir(exist_ok=True)
    print("\nSaving Hot 100 CSVs …")
    save_csv(OUTPUT_DIR / "bibbloard_weeks.csv", wr_all, "weeks_hindex")
    save_csv(OUTPUT_DIR / "bibbloard_peak.csv",  pr_all, "peak_hindex")

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
    all_genre_charts = GENRE_CHARTS_CORE + GENRE_CHARTS_OPTIONAL

    for genre_name, csv_filename in all_genre_charts:
        genre_rows = parse_genre_rows(genre_name, csv_filename)
        if genre_rows is None:
            print(f"\n── {genre_name} — skipped (raw/{csv_filename} not found)")
            continue
        if not genre_rows:
            print(f"\n── {genre_name} — skipped (empty CSV)")
            continue

        print(f"\n── {genre_name} ──────────────────────────────────────────────")
        genre_min  = min_row_date(genre_rows)
        genre_max  = max_row_date(genre_rows)
        key        = CHART_KEYS[genre_name]
        genre_periods = {}   # period_key -> {hhw, hhp}

        for period_key, years_back in PERIODS:
            sd           = compute_since_date(genre_max, years_back)
            artist_songs = deduplicate_rows(genre_rows, sd)
            if not artist_songs:
                continue
            cs           = compute_chart_size(artist_songs)
            wr, pr       = compute_rankings(artist_songs, cs)
            hhw          = hh_index(wr)
            hhp          = hh_index(pr)
            label = "All time" if years_back is None else f"Last {years_back}y"
            print(f"  {label:12s}: {len(artist_songs):5,} artists  chart_size={cs}  Weeks HH={hhw}  Peak HH={hhp}")
            genre_periods[period_key] = {"hhw": hhw, "hhp": hhp}
            fname = f"{key}.json" if period_key == "all" else f"{key}_{period_key}.json"
            save_chart_data(build_chart_payload(wr, pr, artist_songs, hhw, hhp, cs), DATA_DIR / fname)

        genre_summary.append({
            "genre": genre_name, "key": key, "periods": genre_periods,
            "earliest": genre_min, "latest": genre_max,
        })

    # ── Genre summary ─────────────────────────────────────────────────────────
    hot100_min = min_row_date(hot100_rows)
    genre_summary.append({
        "genre": "Hot 100", "key": "hot100", "periods": hot100_periods,
        "earliest": hot100_min, "latest": hot100_max,
    })
    genre_summary.sort(key=lambda g: (-g["periods"]["all"]["hhw"], -g["periods"]["all"]["hhp"]))
    print(f"\n{'═'*52}\n  GENRE H-H-INDEX RANKING (all time)\n{'═'*52}")
    print(f"  {'Genre':<22}  {'Earliest':>10}  {'Latest':>10}  {'Weeks HH':>9}  {'Peak HH':>8}")
    print(f"  {'-'*22}  {'-'*10}  {'-'*10}  {'-'*9}  {'-'*8}")
    for i, g in enumerate(genre_summary, 1):
        hhw = g["periods"]["all"]["hhw"]
        hhp = g["periods"]["all"]["hhp"]
        print(f"  {i}. {g['genre']:<20}  {g['earliest']:>10}  {g['latest']:>10}  {hhw:>9}  {hhp:>8}")

    print("\nUpdating HTML …")
    update_html_genre_summary(genre_summary)
    print("\nDone. Open: http://localhost:7433/bibbloard.html")


if __name__ == "__main__":
    main()
