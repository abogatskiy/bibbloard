#!/usr/bin/env python3
"""
fetch_genre_updates.py
======================
Fetches missing Billboard genre chart data using the billboard.py library
(scrapes Billboard.com directly) and appends it to CSVs in raw/.

Usage:
    python3 fetch_genre_updates.py              # interactive menu
    python3 fetch_genre_updates.py --all        # fetch all charts non-interactively
    python3 fetch_genre_updates.py --dry-run    # show plan, fetch nothing
    python3 fetch_genre_updates.py --delay 2    # override starting delay (seconds)

Requirements:
    pip3 install billboard.py
"""

import argparse
import csv
import datetime
import sys
import time
from pathlib import Path

try:
    import billboard
    import requests, requests.adapters
except ImportError:
    sys.exit("Missing dependency — run: pip3 install billboard.py")

# ── Patch billboard.py to use browser headers ──────────────────────────────────
def _patched_session(max_retries):
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.6 Safari/605.1.15"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    session.mount(
        "https://www.billboard.com",
        requests.adapters.HTTPAdapter(max_retries=max_retries),
    )
    return session

billboard._get_session_with_retries = _patched_session

# ── Configuration ──────────────────────────────────────────────────────────────

RAW_DIR = Path(__file__).parent / "raw"

# (display name, billboard.py slug, CSV filename in raw/)
CHARTS = [
    # Core charts
    ("Country",            "country-songs",          "country.csv"),
    ("Hip-Hop",            "r-b-hip-hop-songs",      "hiphop.csv"),
    ("Latin",              "latin-songs",             "latin.csv"),
    ("Pop",                "pop-songs",               "pop.csv"),
    ("Rock",               "hot-rock-songs",          "rock.csv"),
    ("Dance/Electronic",   "dance-electronic-songs",  "dance_electronic.csv"),
    # Optional — fetch from scratch if needed
    ("Adult Contemporary", "adult-contemporary",      "adult_contemporary.csv"),
    ("Adult Pop",          "adult-pop-songs",         "adult_pop.csv"),
    ("Country Airplay",    "country-airplay",         "country_airplay.csv"),
    ("Gospel",             "gospel-songs",            "gospel.csv"),
    ("Jazz",               "jazz-songs",              "jazz.csv"),
    ("Alternative",        "alternative-songs",       "alternative.csv"),
]

# Adaptive delay configuration
DELAY_START   = 0.5    # seconds (fast initial pace)
DELAY_MAX     = 30.0   # cap on backoff
DELAY_BACKOFF = 2.0    # multiply on rate-limit hit
DELAY_RECOVER = 0.85   # multiply on success (gradual recovery)
MAX_RETRIES   = 3
RETRY_DELAY   = 15.0   # seconds before retry on transient error

# Earliest date reachable via billboard.py scraper for each chart
CHART_EARLIEST = {
    "country-songs":         "1958-10-20",
    "r-b-hip-hop-songs":     "1958-10-20",
    "latin-songs":           "1986-09-20",
    "pop-songs":             "1992-10-03",
    "hot-rock-songs":        "2009-06-20",
    "dance-electronic-songs":"2013-01-26",
    "adult-contemporary":    "2000-01-01",
    "adult-pop-songs":       "2000-01-01",
    "country-airplay":       "2000-01-01",
    "gospel-songs":          "2005-03-19",
    "jazz-songs":            "2005-10-22",
    "alternative-songs":     "2000-01-01",
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def get_existing_dates(csv_path: Path) -> set:
    """Return the set of chart_date strings already in the CSV."""
    dates = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            dates.add(row["chart_date"])
    return dates

def get_date_range(csv_path: Path) -> tuple:
    """
    Return (earliest, latest) chart dates in the CSV as datetime.date objects,
    or (None, None) if the file doesn't exist or is empty.
    """
    if not csv_path.exists():
        return None, None
    earliest = latest = None
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d_str = row.get("chart_date", "")
            if not d_str:
                continue
            try:
                d = datetime.date.fromisoformat(d_str)
            except ValueError:
                continue
            if earliest is None or d < earliest:
                earliest = d
            if latest is None or d > latest:
                latest = d
    return earliest, latest

def find_gap_dates(existing_dates: set) -> list:
    """
    Return a list of approximate dates to request for gaps > ~10 days between
    consecutive existing dates. Uses midpoint of each gap interval.
    """
    sorted_dates = sorted(
        datetime.date.fromisoformat(d) for d in existing_dates if d
    )
    gaps = []
    for i in range(len(sorted_dates) - 1):
        d1 = sorted_dates[i]
        d2 = sorted_dates[i + 1]
        # Normal weekly interval is 7 days; gap if > 14 days (more than one week)
        if (d2 - d1).days > 14:
            current = d1 + datetime.timedelta(weeks=1)
            while current < d2:
                gaps.append(current)
                current += datetime.timedelta(weeks=1)
    return gaps

def weeks_between(start: datetime.date, end: datetime.date):
    """Yield weekly dates from start to end inclusive."""
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(weeks=1)

def fetch_with_retry(slug: str, date: datetime.date, retries: int = MAX_RETRIES):
    """Fetch a chart week, retrying on transient errors. Raises on rate-limit (403/429)."""
    for attempt in range(1, retries + 1):
        try:
            return billboard.ChartData(slug, date=date.isoformat())
        except Exception as e:
            msg = str(e)
            # Rate-limiting — don't retry, let caller back off
            if "403" in msg or "429" in msg:
                raise
            if attempt < retries:
                print(f"    ⚠ attempt {attempt} failed ({e}), retrying in {RETRY_DELAY}s…")
                time.sleep(RETRY_DELAY)
            else:
                raise

def write_header(csv_path: Path):
    """Write CSV header if file doesn't exist yet."""
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["", "ranking", "artist", "title",
                         "last_week_rank", "peak_position", "weeks_on_chart", "chart_date"])

def append_rows(csv_path: Path, chart_data, existing_dates: set) -> tuple:
    """
    Append chart entries if their date isn't already present.
    Returns (actual_date_str, entries_written).
    """
    actual_date = chart_data.date
    if actual_date in existing_dates:
        return actual_date, 0

    rows_written = 0
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for entry in chart_data:
            idx = f"{actual_date}_{entry.rank}"
            writer.writerow([
                idx,
                entry.rank,
                entry.artist,
                entry.title,
                entry.lastPos or 0,
                entry.peakPos,
                entry.weeks,
                actual_date,
            ])
            rows_written += 1

    existing_dates.add(actual_date)
    return actual_date, rows_written

def fmt_duration(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h: return f"{h}h {m}m"
    if m: return f"{m}m {s}s"
    return f"{s}s"

# ── Interactive menu ───────────────────────────────────────────────────────────

def print_menu(cutoff: datetime.date):
    """Print chart status and return list of selectable items."""
    print()
    print("  #   Chart                  Status")
    print("  -   -----                  ------")
    items = []
    for i, (name, slug, filename) in enumerate(CHARTS, 1):
        csv_path = RAW_DIR / filename
        earliest_date, last = get_date_range(csv_path)
        if last is None:
            earliest_str = CHART_EARLIEST.get(slug, "unknown")
            status = f"no data — will fetch from {earliest_str}"
        else:
            existing = get_existing_dates(csv_path)
            # New weeks at the end
            new_start = last + datetime.timedelta(weeks=1)
            new_weeks = len(list(weeks_between(new_start, cutoff))) if new_start <= cutoff else 0
            # Gaps within existing data
            gap_dates = find_gap_dates(existing)
            total = new_weeks + len(gap_dates)
            if total == 0:
                status = f"up to date (last: {last})"
            else:
                parts = []
                if new_weeks:
                    parts.append(f"{new_weeks} new weeks")
                if gap_dates:
                    parts.append(f"{len(gap_dates)} gap weeks")
                status = f"last: {last}  →  {', '.join(parts)} to fetch"
        print(f"  {i:<3} {name:<22} {status}")
        items.append((name, slug, filename, earliest_date, last))
    print()
    return items

def ask_selection(items):
    """Prompt user to select charts. Returns list of (name, slug, filename, earliest, last_date)."""
    total = len(items)
    print("Enter chart numbers to fetch (e.g. 1,3,5 or 1-6 or 'all' or 'q' to quit):")
    while True:
        try:
            raw = input("  > ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)

        if raw in ("q", "quit", "exit"):
            sys.exit(0)
        if raw in ("a", "all"):
            return items

        selected = set()
        valid = True
        for part in raw.replace(" ", "").split(","):
            if "-" in part:
                lo, _, hi = part.partition("-")
                try:
                    lo, hi = int(lo), int(hi)
                    if not (1 <= lo <= hi <= total):
                        raise ValueError
                    selected.update(range(lo, hi + 1))
                except ValueError:
                    print(f"  Invalid range: '{part}'")
                    valid = False
                    break
            else:
                try:
                    n = int(part)
                    if not (1 <= n <= total):
                        raise ValueError
                    selected.add(n)
                except ValueError:
                    print(f"  Invalid number: '{part}'")
                    valid = False
                    break

        if valid and selected:
            return [items[i - 1] for i in sorted(selected)]
        if valid:
            print("  Nothing selected — try again or 'q' to quit.")

# ── Fetch one chart ────────────────────────────────────────────────────────────

def fetch_chart(name: str, slug: str, filename: str,
                earliest_existing,
                last_date,
                cutoff: datetime.date, start_delay: float, dry_run: bool) -> int:
    """
    Fetch all missing weeks for one chart (new weeks + any gaps).
    Returns total entries written.
    """
    csv_path = RAW_DIR / filename
    RAW_DIR.mkdir(exist_ok=True)

    # Create file with header if fresh
    if not csv_path.exists():
        write_header(csv_path)

    existing = get_existing_dates(csv_path)

    if last_date is None:
        # Fresh chart — walk every 7 days from known earliest
        earliest_str = CHART_EARLIEST.get(slug)
        start = datetime.date.fromisoformat(earliest_str) if earliest_str else datetime.date(2000, 1, 1)
        week_dates = list(weeks_between(start, cutoff))
    else:
        # Gaps within existing data + new weeks at end
        gap_dates   = find_gap_dates(existing)
        new_start   = last_date + datetime.timedelta(weeks=1)
        new_dates   = list(weeks_between(new_start, cutoff)) if new_start <= cutoff else []
        week_dates  = sorted(set(gap_dates + new_dates))

    if not week_dates:
        print(f"[{name}] Already up to date (last: {last_date})")
        return 0

    est_secs = len(week_dates) * start_delay
    print(f"[{name}] {len(week_dates)} weeks to fetch  "
          f"({week_dates[0]} … {week_dates[-1]})  "
          f"est. {fmt_duration(est_secs)} at {start_delay}s/req")

    if dry_run:
        return 0

    delay = start_delay
    added = skipped = errors = 0
    total_entries = 0

    for i, week_date in enumerate(week_dates):
        try:
            chart_data = fetch_with_retry(slug, week_date)
            actual, n  = append_rows(csv_path, chart_data, existing)

            if n:
                added += 1
                total_entries += n
                pct = f"{(i+1)/len(week_dates)*100:.0f}%"
                print(f"  [{pct:>4}] {actual}  +{n} entries  delay={delay:.2f}s")
                # Gradually recover speed on success
                delay = max(start_delay, delay * DELAY_RECOVER)
            else:
                skipped += 1
                print(f"  [skip] {actual}  (date snapped to existing week)")

        except Exception as e:
            msg = str(e)
            if "403" in msg or "429" in msg:
                delay = min(DELAY_MAX, delay * DELAY_BACKOFF)
                print(f"  [RATE] {week_date}  rate-limited — backing off to {delay:.1f}s")
                time.sleep(delay)
                continue
            else:
                errors += 1
                print(f"  [ERR ] {week_date}: {e}")

        if i < len(week_dates) - 1:
            time.sleep(delay)

    print(f"[{name}] Done — {added} weeks added, {skipped} skipped, {errors} errors\n")
    return total_entries

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--all", action="store_true",
                        help="Fetch all charts without interactive menu")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be fetched, make no requests")
    parser.add_argument("--delay", type=float, default=DELAY_START, metavar="SECS",
                        help=f"Starting delay between requests (default: {DELAY_START})")
    args = parser.parse_args()

    # One week before today — allow a few days for chart publication lag
    cutoff = datetime.date.today() - datetime.timedelta(days=7)

    if args.all or args.dry_run:
        # Non-interactive: fetch all charts
        selection = []
        for name, slug, filename in CHARTS:
            earliest, last = get_date_range(RAW_DIR / filename)
            selection.append((name, slug, filename, earliest, last))
    else:
        items = print_menu(cutoff)
        selection = ask_selection(items)

    total_written = 0
    for name, slug, filename, earliest, last_date in selection:
        total_written += fetch_chart(
            name, slug, filename, earliest, last_date,
            cutoff, args.delay, args.dry_run
        )

    if args.dry_run:
        print("(dry run — no data fetched)")
    elif total_written:
        print(f"All done. {total_written:,} total entries written.")
    else:
        print("All selected charts are up to date.")


if __name__ == "__main__":
    main()
