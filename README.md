# Bibbloard

**Live site: [bibbloard.abogatskiy.com](https://bibbloard.abogatskiy.com)**

An h-index for Billboard chart artists — because charting once doesn't make you a legend.

## What is a chart h-index?

By analogy with the [academic h-index](https://en.wikipedia.org/wiki/H-index):

- **Weeks h-index** — an artist has h-index *h* if they have at least *h* songs that each appeared on the chart for at least *h* weeks
- **Peak h-index** — an artist has h-index *h* if they have at least *h* songs each with a chart score ≥ *h*, where score = chart size − peak position (so a #1 song scores 99 on the Hot 100, a #50 song scores 50, etc.)

Both metrics reward breadth *and* depth: you need many charting songs, and each one has to hold up.

## Charts covered

| Chart | Coverage |
|---|---|
| Hot 100 | 1958 – present |
| Hip-Hop | 1958 – 2018 |
| Latin | 1986 – 2018 |
| Pop | 1992 – 2018 |
| Country | 2011 – 2018 |
| Rock | 2009 – 2018 |
| Dance/Electronic | 2013 – 2018 |

The **genre h-h-index** table ranks the charts themselves: the largest *h* such that *h* artists on that chart have h-index ≥ *h*.

## Features

- Filter by time window: all-time, last 5 / 10 / 15 / 20 / 25 / 30 years
- Visual h-index curves (Plotly) showing the top 10 / 20 / 30 artists
- Ranked tables with up to 200 artists
- Clickable genre table — jump straight to any chart

## Data sources

- **Hot 100**: [mhollingshead/billboard-hot-100](https://github.com/mhollingshead/billboard-hot-100)
- **Genre charts**: [pdp2600/chartscraper](https://github.com/pdp2600/chartscraper)

## Running locally

```bash
# Install dependencies (standard library only)
python3 billboard_hindex.py   # downloads data, generates data/*.json

# Open in browser
open billboard_hindex.html
```

The script downloads source data on first run (~42 MB for Hot 100) and caches it in `data/`. Subsequent runs are fast.
