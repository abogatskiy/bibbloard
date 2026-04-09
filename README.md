# Bibbloard

**Live site: [bibbloard.abogatskiy.com](https://bibbloard.abogatskiy.com)**

An h-index for Billboard chart artists — because charting once doesn't make you a legend.

## What is a chart h-index?

By analogy with the [academic h-index](https://en.wikipedia.org/wiki/H-index):

- **Weeks h-index** — an artist has h-index *h* if they have at least *h* songs that each appeared on the chart for at least *h* weeks
- **Peak h-index** — an artist has h-index *h* if they have at least *h* songs each at least *h* spots from the end of the chart (i.e. chart size − peak position ≥ *h*, so a Hot 100 #1 scores 99)
- **Integrated h-index** — an artist has h-index *h* if they have at least *h* songs each with a cumulative chart score ≥ *h* (sum of chart size − peak position across all charting weeks)

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

## Features

- Filter by time window: all-time or since a given year (2000, 2005, 2010, 2015, 2020)
- Visual h-index curves (Plotly) for the top 10 / 20 / 30 artists per chart — weeks, peak, and integrated
- Combined ranked table with all three h-indices side by side
- Clickable genre table with h-h-index for all three metrics — jump straight to any chart
- Per-artist h-index timeline with optional velocity view

## Data sources

- **Hot 100**: [mhollingshead/billboard-hot-100](https://github.com/mhollingshead/billboard-hot-100)
- **Genre charts**: [pdp2600/chartscraper](https://github.com/pdp2600/chartscraper)

## Running locally

`bibbloard.py` fetches raw chart data from the sources above, computes h-indices, and writes pre-generated JSON files to `data/`. Run it whenever you want to refresh the data:

```bash
python3 bibbloard.py
```

The script downloads source data on first run (~42 MB for Hot 100) and caches it locally. Subsequent runs are fast.

To view the site locally, serve it with a local HTTP server (required for `fetch()` to load the JSON files):

```bash
python3 -m http.server 8080
# then open http://localhost:8080/bibbloard.html
```

## Deployment

```bash
./deploy.sh
```

Builds an ARM64 Docker image, pushes it to GitHub Container Registry, and updates the Portainer stack on a Raspberry Pi.
