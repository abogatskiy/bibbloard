# Bibbloard

**Live site: [bibbloard.abogatskiy.com](https://bibbloard.abogatskiy.com)**

An h-index for Billboard chart artists — because charting once doesn't make you a legend.

## What is a chart h-index?

By analogy with the [academic h-index](https://en.wikipedia.org/wiki/H-index):

- **Weeks h-index** — an artist has h-index *h* if they have at least *h* songs that each appeared on the chart for at least *h* weeks
- **Peak h-index** — an artist has h-index *h* if they have at least *h* songs each at least *h* spots from the end of the chart (i.e. chart size − peak position ≥ *h*, so a Hot 100 #1 scores 99)
- **Integrated h-index** — an artist has h-index *h* if they have at least *h* songs each with ≥ *h* peak-equivalent weeks on chart (each charting week contributes `(chart_size − position) / chart_size`, so #1 ≈ 1.0/week; normalised so chart size changes over time don't distort scores)

All three metrics reward breadth *and* depth: you need many charting songs, and each one has to hold up. The integrated variant additionally rewards longevity — a song that stays near the top for many weeks accumulates a much higher score than one that peaks and disappears.

## Charts covered

| Chart | Coverage |
|---|---|
| Hot 100 | 1958 – present |
| Country | 1958 – present |
| Hip-Hop | 1958 – present |
| Latin | 1986 – present |
| Pop | 1992 – present |
| Country Airplay | 2000 – present |
| Adult Pop | 2000 – present |
| Adult Contemporary | 2000 – present |
| Jazz | 2005 – present |
| Gospel | 2005 – present |
| Rock | 2009 – present |
| Dance/Electronic | 2013 – present |

The **genre h-h-index** table ranks the charts themselves: the largest *h* such that *h* artists on that chart have h-index ≥ *h*.

## Chart sizes and unified cs

Charts differ in size: the Hot 100 has 100 slots per week; Dance/Electronic has only 50. This matters for the **peak h-index** — a #1 on Hot 100 scores 99, while a #1 on Dance/Electronic scores just 49, making raw peak scores incomparable across charts.

The **Unified cs (100)** toggle normalises all charts to 100 slots so that every #1 scores 99, every #2 scores 98, etc., enabling fair cross-chart peak comparison. Weeks and integrated h-indices are already chart-size-neutral (weeks simply counts appearances; integrated divides by chart size each week) and are unaffected by this toggle.

## Features

- Filter by time window: all-time or since a given year (2000, 2005, 2010, 2015, 2020)
- **Unified cs (100)** toggle to normalise peak h-index across charts of different sizes
- Visual h-index curves (Plotly) for the top 10 / 20 / 30 artists per chart — weeks, peak, and integrated
- Combined ranked table with all three h-indices and year-over-year changes side by side
- Clickable genre table with h-h-index for all three metrics — jump straight to any chart
- Per-artist h-index timeline with optional velocity view
- Hover tooltips on integrated plot show a mini bar chart of the song's weekly position trajectory

## Data sources

- **Hot 100**: [mhollingshead/billboard-hot-100](https://github.com/mhollingshead/billboard-hot-100) — downloaded as `all.json` (~50 MB) and cached in `raw/`
- **Genre charts**: scraped directly from Billboard.com using [billboard.py](https://github.com/guoguo12/billboard-charts), stored as CSVs in `raw/`

## Running locally

### Step 1 — fetch raw data

`fetch_genre_updates.py` scrapes Billboard.com for all genre chart history and keeps the local CSVs up to date. It also re-downloads the Hot 100 JSON if stale. **This is the primary data source and must be run before `bibbloard.py`.**

```bash
pip3 install billboard.py
python3 fetch_genre_updates.py        # interactive menu — pick which charts to update
python3 fetch_genre_updates.py --all  # fetch everything non-interactively
python3 fetch_genre_updates.py --dry-run  # see what would be fetched without making requests
```

The script is rate-limit-aware and uses adaptive delays when Billboard.com throttles requests.

### Step 2 — generate JSON

`bibbloard.py` reads the raw CSVs from `raw/`, computes all h-indices, and writes pre-generated JSON files to `data/`. Run it after fetching new data:

```bash
python3 bibbloard.py
```

### Step 3 — serve locally

A local HTTP server is required for `fetch()` to load the JSON files:

```bash
python3 -m http.server 8080
# then open http://localhost:8080/bibbloard.html
```

## Deployment

```bash
./deploy.sh
```

Builds an ARM64 Docker image, pushes it to GitHub Container Registry, and updates the Portainer stack on a Raspberry Pi. A GitHub Actions workflow also runs this automatically every Monday.
