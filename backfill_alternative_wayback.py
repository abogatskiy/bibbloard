#!/usr/bin/env python3
"""
Backfill missing weeks of the Alternative Airplay chart from the Wayback Machine.

Billboard only serves the *current* Alternative chart (see SNAPSHOT_ONLY_SLUGS in
fetch_genre_updates.py), so a week missed by the weekly job can't be re-fetched
from billboard.com. This script looks for archived copies of the chart page,
parses each with the same patched parser the weekly job uses, and inserts any
chart weeks missing from raw/alternative.csv.

Usage:
    python3 backfill_alternative_wayback.py              # fill all gaps
    python3 backfill_alternative_wayback.py --dry-run    # report only
    python3 backfill_alternative_wayback.py --html FILE  # parse a saved page (testing)
"""

import argparse
import csv
import datetime
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

import billboard
# Keep billboard.py's own parser for pages archived before Billboard's
# Sept 2026 redesign; importing fetch_genre_updates replaces it.
_PRE_REDESIGN_PARSER = billboard.ChartData._parseNewStylePage
import fetch_genre_updates as fgu  # applies the patched billboard.py parser

CSV_PATH  = fgu.RAW_DIR / "alternative.csv"
SLUG      = "alternative-songs"
PAGE_URLS = [
    "billboard.com/charts/alternative-airplay/",
    "billboard.com/charts/alternative-songs/",
]
CDX_URL   = "https://web.archive.org/cdx/search/cdx"
DELAY     = 1.5   # seconds between Wayback requests, to be polite

session = requests.Session()
session.headers["User-Agent"] = "bibbloard-backfill (github.com/abogatskiy/bibbloard)"


def get(url, **kwargs):
    for attempt in range(4):
        try:
            r = session.get(url, timeout=60, **kwargs)
            if r.status_code == 200:
                return r
            print(f"    HTTP {r.status_code} for {url}")
        except requests.RequestException as e:
            print(f"    {type(e).__name__} for {url}")
        time.sleep(5 * (attempt + 1))
    return None


def read_rows():
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        return header, list(reader)


def missing_weeks(rows):
    """Saturdays between the first stored week and today with no stored chart."""
    have = {datetime.date.fromisoformat(r[-1]) for r in rows}
    d, end = min(have), datetime.date.today()
    missing = []
    while d <= end:
        if d not in have:
            missing.append(d)
        d += datetime.timedelta(weeks=1)
    return missing


def parse_page(html):
    soup = BeautifulSoup(html, "html.parser")
    chart = billboard.ChartData(SLUG, fetch=False)
    try:
        chart._parsePage(soup)
    except billboard.BillboardParseException:
        chart = billboard.ChartData(SLUG, fetch=False)
        _PRE_REDESIGN_PARSER(chart, soup)
    return chart


def list_captures(start, end):
    captures = []
    for page in PAGE_URLS:
        r = get(CDX_URL, params={
            "url": page, "from": start.strftime("%Y%m%d"), "to": end.strftime("%Y%m%d"),
            "filter": "statuscode:200", "fl": "timestamp,original",
            "collapse": "timestamp:8", "output": "json",
        })
        if r is None:
            sys.exit(f"Could not query the Wayback CDX API for {page}")
        rows = r.json()
        captures += [tuple(row) for row in rows[1:]]   # first row is the header
    return sorted(set(captures))


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Report only, don't write")
    parser.add_argument("--html", metavar="FILE", help="Parse a saved chart page and exit")
    args = parser.parse_args()

    if args.html:
        chart = parse_page(Path(args.html).read_text(encoding="utf-8"))
        print(f"{chart.date}: {len(chart.entries)} entries, #1 {chart.entries[0]}")
        return

    header, rows = read_rows()
    wanted = set(missing_weeks(rows))
    print(f"{len(wanted)} missing weeks: {', '.join(str(d) for d in sorted(wanted))}")
    if not wanted:
        return

    # A chart dated Saturday D is published the Tuesday before and replaced a
    # week later, so captures from D-5 to D+2 can show it.
    start = min(wanted) - datetime.timedelta(days=6)
    end   = max(wanted) + datetime.timedelta(days=3)
    captures = list_captures(start, end)
    print(f"{len(captures)} Wayback captures between {start} and {end}")

    found = {}
    for timestamp, original in captures:
        day = datetime.datetime.strptime(timestamp[:8], "%Y%m%d").date()
        if not any(-3 <= (w - day).days <= 6 for w in wanted - found.keys()):
            continue                     # no still-missing week could be on this page
        r = get(f"https://web.archive.org/web/{timestamp}id_/{original}")
        time.sleep(DELAY)
        if r is None:
            continue
        try:
            chart = parse_page(r.text)
        except Exception as e:
            print(f"  {timestamp}: parse failed ({type(e).__name__}: {e})")
            continue
        if not chart.date:
            print(f"  {timestamp}: no chart date on page")
            continue
        date = datetime.date.fromisoformat(chart.date)
        status = "already have" if date not in wanted else (
                 "duplicate" if date in found else f"NEW, {len(chart.entries)} entries")
        print(f"  {timestamp} → chart {date}: {status}")
        if date in wanted and date not in found and chart.entries:
            found[date] = chart

    print(f"\nRecovered {len(found)} of {len(wanted)} missing weeks")
    for d in sorted(wanted):
        print(f"  {d}: {'recovered' if d in found else 'not archived'}")
    if args.dry_run or not found:
        return

    for date, chart in found.items():
        for e in chart.entries:
            rows.append([f"{date}_{e.rank}", e.rank, e.artist, e.title,
                         e.lastPos or 0, e.peakPos, e.weeks, str(date)])
    rows.sort(key=lambda r: (r[-1], int(r[1])))
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Wrote {CSV_PATH}")


if __name__ == "__main__":
    main()
