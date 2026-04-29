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
import json
import os
import sys
import time
import urllib.request
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

RAW_DIR      = Path(__file__).parent / "raw"
HOT100_URL   = "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/all.json"
HOT100_CACHE = RAW_DIR / "all.json"

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
DELAY_START   = 0.0    # seconds (no delay unless rate-limited)
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

# Charts that Billboard only serves for the current week (no historical data).
# For these, we OVERWRITE the CSV with just the latest week each run.
SNAPSHOT_ONLY_SLUGS = {
    "alternative-songs",
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

# ── Hot 100 helpers ────────────────────────────────────────────────────────────

def get_hot100_latest_date():
    """Return the latest chart date in the cached all.json, or None if not cached."""
    if not HOT100_CACHE.exists():
        return None
    try:
        with open(HOT100_CACHE, encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            return None
        latest = max((entry.get("date", "") for entry in data), default="")
        return datetime.date.fromisoformat(latest) if latest else None
    except Exception:
        return None

def _download_with_progress(url: str, dest: Path, label: str):
    """Stream-download url to dest with an inline progress bar. Atomic write."""
    bar_w = 28
    start = time.perf_counter()
    tmp   = dest.with_suffix(".tmp")

    def hook(count, block_size, total_size):
        if total_size <= 0:
            return
        done    = min(count * block_size, total_size)
        frac    = done / total_size
        filled  = int(bar_w * frac)
        bar     = '█' * filled + '░' * (bar_w - filled)
        elapsed = time.perf_counter() - start
        speed   = done / elapsed / 1e6 if elapsed > 0.1 else 0.0
        mb_done  = done  / 1e6
        mb_total = total_size / 1e6
        sys.stdout.write(
            f'\r  [{bar}]  {mb_done:.1f}/{mb_total:.1f} MB  {speed:.1f} MB/s  '
        )
        sys.stdout.flush()

    dest.parent.mkdir(exist_ok=True)
    tmp.unlink(missing_ok=True)
    try:
        urllib.request.urlretrieve(url, tmp, hook)
        sys.stdout.write('\n')
        sys.stdout.flush()
        os.replace(tmp, dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise

def fetch_hot100(cutoff: datetime.date, dry_run: bool) -> int:
    """Re-download Hot 100 all.json from GitHub if cache is behind the cutoff date."""
    latest = get_hot100_latest_date()
    stale  = latest is None or latest < cutoff
    if not stale:
        print(f"  Hot 100: up to date (latest chart: {latest})")
        return 0
    if dry_run:
        action = "re-download" if latest else "download"
        print(f"  Hot 100: would {action} all.json from GitHub (~50 MB)")
        return 0
    if latest:
        print(f"  Hot 100: cache is stale (latest: {latest}) — re-downloading from GitHub …")
    else:
        print("  Hot 100: no cache — downloading from GitHub (~50 MB) …")
    try:
        _download_with_progress(HOT100_URL, HOT100_CACHE, "Hot 100")
    except KeyboardInterrupt:
        print("\nInterrupted.")
        raise
    new_latest = get_hot100_latest_date()
    print(f"  Hot 100: done — latest chart: {new_latest}")
    return 1

# ── Progress bar ───────────────────────────────────────────────────────────────

class Progress:
    """Single-line overwriting progress bar with ETA.
    Use interrupt() to print messages above the bar without corrupting it."""
    _FULL  = '█'
    _EMPTY = '░'

    def __init__(self, total: int, bar_width: int = 28):
        self.total   = max(total, 1)
        self.done    = 0
        self.start   = time.perf_counter()
        self.desc    = ''
        self._bar_w  = bar_width
        self._t_last = -1.0
        self._last_len = 0

    def update(self, n: int = 1, desc: str = None):
        self.done += n
        if desc is not None:
            self.desc = desc
        t = time.perf_counter()
        if t - self._t_last >= 0.1 or self.done >= self.total:
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
            right = f'  100%  {fmt_duration(elapsed)}'
        elif frac > 0.01:
            eta = elapsed * (1.0 / frac - 1.0)
            right = f'  {frac*100:4.1f}%  {fmt_duration(elapsed)} + ETA {fmt_duration(eta)}'
        else:
            right = f'  {frac*100:4.1f}%  …'

        prefix = f'\r[{bar}] '
        avail  = cols - len(prefix) - len(right) - 2
        desc   = self.desc if avail > 0 else ''
        if len(desc) > avail:
            desc = desc[:max(avail - 1, 0)] + '…'
        line = prefix + desc.ljust(max(avail, 0)) + right
        sys.stdout.write(line)
        sys.stdout.flush()
        self._last_len = len(line)

    def interrupt(self, msg: str):
        """Print a message on its own line, then redraw the bar below it."""
        sys.stdout.write('\r' + ' ' * self._last_len + '\r')  # clear bar
        print(msg)
        self._render()

    def finish(self, msg: str = ''):
        self.done = self.total
        if msg:
            self.desc = msg
        self._render()
        sys.stdout.write('\n')
        sys.stdout.flush()

# ── Interactive menu ───────────────────────────────────────────────────────────

def print_menu(cutoff: datetime.date):
    """Print chart status and return list of selectable items."""
    print()
    print("  #   Chart                  Status")
    print("  -   -----                  ------")
    items = []

    # ── Hot 100 (GitHub download, not billboard.py) ────────────────────────────
    hot100_latest = get_hot100_latest_date()
    if hot100_latest is None:
        h100_status = "not cached — will download (~50 MB)"
    elif hot100_latest >= cutoff:
        h100_status = f"up to date (latest: {hot100_latest})"
    else:
        h100_status = f"latest: {hot100_latest}  →  stale, re-download (~50 MB)"
    print(f"  {1:<3} {'Hot 100':<22} {h100_status}")
    items.append(("Hot 100", None, "all.json", None, hot100_latest))

    # ── Genre charts (billboard.py scraper) ────────────────────────────────────
    for i, (name, slug, filename) in enumerate(CHARTS, 2):
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
            # Missing history before earliest entry
            hist_weeks = 0
            earliest_str = CHART_EARLIEST.get(slug, "")
            if earliest_str and earliest_date:
                chart_start = datetime.date.fromisoformat(earliest_str)
                if earliest_date > chart_start + datetime.timedelta(days=14):
                    hist_end = earliest_date - datetime.timedelta(weeks=1)
                    hist_weeks = len(list(weeks_between(chart_start, hist_end)))
            total = new_weeks + len(gap_dates) + hist_weeks
            if total == 0:
                status = f"up to date (last: {last})"
            else:
                parts = []
                if hist_weeks:
                    parts.append(f"{hist_weeks} weeks of missing history (since {earliest_str})")
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
    existing = get_existing_dates(csv_path) if csv_path.exists() else set()

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
        # Missing history before the earliest entry in the CSV
        hist_dates  = []
        earliest_str = CHART_EARLIEST.get(slug)
        if earliest_str and earliest_existing:
            chart_start = datetime.date.fromisoformat(earliest_str)
            if earliest_existing > chart_start + datetime.timedelta(days=14):
                hist_end   = earliest_existing - datetime.timedelta(weeks=1)
                hist_dates = list(weeks_between(chart_start, hist_end))
        # Snapshot-only slugs: Billboard only serves the current week, so any
        # historical request just returns the latest date. Skip backfill entirely.
        if slug in SNAPSHOT_ONLY_SLUGS:
            hist_dates = []

        # Fast-check: probe 2 widely-spaced historical dates to see if the API
        # actually serves historical data (some charts always return the current week).
        if hist_dates and not dry_run:
            probe = [hist_dates[0], hist_dates[len(hist_dates) // 2]]
            sys.stdout.write(f"  {name}: probing {len(hist_dates)} historical weeks "
                             f"({hist_dates[0]} → {hist_dates[-1]}) … ")
            sys.stdout.flush()
            returned = []
            for d in probe:
                try:
                    returned.append(fetch_with_retry(slug, d).date)
                except Exception:
                    returned.append(None)
            # If both probes returned the same already-known date, the API has no history
            if len(set(returned)) == 1 and returned[0] in existing:
                print(f"redirects to '{returned[0]}' — no historical data available, skipping.")
                hist_dates = []
            else:
                print("ok, historical data available.")

        week_dates  = sorted(set(hist_dates + gap_dates + new_dates))

    if not week_dates:
        print(f"  {name}: already up to date (last: {last_date})")
        return 0

    print(f"  {name}: {len(week_dates)} weeks to fetch  ({week_dates[0]} → {week_dates[-1]})")

    if dry_run:
        return 0

    # Create file with header now that we know there's real work to do
    RAW_DIR.mkdir(exist_ok=True)
    if not csv_path.exists():
        write_header(csv_path)
        existing = get_existing_dates(csv_path)  # re-read (now has header)

    delay        = start_delay
    added        = 0
    skipped      = 0
    errors       = 0
    total_entries = 0
    progress     = Progress(len(week_dates))

    for i, week_date in enumerate(week_dates):
        try:
            chart_data = fetch_with_retry(slug, week_date)
            actual, n  = append_rows(csv_path, chart_data, existing)

            if n:
                added += 1
                total_entries += n
                delay_s = f"  delay={delay:.2f}s" if delay > 0 else ""
                suffix = f" (got {actual})" if actual != week_date.isoformat() else ""
                progress.update(1, f"{week_date}{suffix}  +{n} entries{delay_s}")
                delay = max(0.0, delay * DELAY_RECOVER)
            else:
                suffix = f" → {actual}" if actual != week_date.isoformat() else ""
                skipped += 1
                progress.update(1, f"{week_date}{suffix}  (skipped)")

        except KeyboardInterrupt:
            progress.interrupt("\nInterrupted.")
            raise

        except Exception as e:
            msg = str(e)
            if "403" in msg or "429" in msg:
                delay = min(DELAY_MAX, max(delay, 1.0) * DELAY_BACKOFF)
                progress.interrupt(f"  ⚠ rate-limited {week_date} — sleeping {delay:.0f}s")
                time.sleep(delay)
                continue
            else:
                errors += 1
                progress.interrupt(f"  ✗ {week_date}: {e}")

        if delay > 0 and i < len(week_dates) - 1:
            time.sleep(delay)

    progress.finish(f"{added} weeks added, {skipped} skipped, {errors} errors")
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
        # Non-interactive: fetch all charts (Hot 100 first)
        hot100_latest = get_hot100_latest_date()
        selection = [("Hot 100", None, "all.json", None, hot100_latest)]
        for name, slug, filename in CHARTS:
            earliest, last = get_date_range(RAW_DIR / filename)
            selection.append((name, slug, filename, earliest, last))
    else:
        items = print_menu(cutoff)
        selection = ask_selection(items)

    total_written = 0
    for name, slug, filename, earliest, last_date in selection:
        if slug is None:
            # Hot 100 — full re-download from GitHub
            total_written += fetch_hot100(cutoff, args.dry_run)
        else:
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
