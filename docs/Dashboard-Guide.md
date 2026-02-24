# Search Analytics Dashboard Guide

## Overview

The Search Analytics Dashboard is a self-contained HTML application that provides interactive visualization and analysis of search telemetry data. It runs entirely in the browser using DuckDB WASM, requiring no server-side infrastructure.

## Prerequisites

Before running the dashboard, ensure you have:

1. **Generated parquet files** from the Python processing script
2. **A modern web browser** (Chrome, Firefox, Edge, or Safari)
3. **A local web server** (required for loading parquet files due to browser security restrictions)

## Required Data Files

The dashboard expects these parquet files in the `output/` folder (sibling to the `dashboard/` folder):

```
SearchAnalytics/
├── dashboard/
│   └── dashboard.html
├── mappings/                        ← Optional department mapping files
│   ├── GEDULD_2025_12_15.xlsx
│   └── GEDULD_2026_01_14.xlsx
└── output/
    ├── searches_daily.parquet
    ├── searches_journeys.parquet
    ├── searches_terms.parquet
    └── searches_term_clicks.parquet
```

| File | Description |
|------|-------------|
| `searches_daily.parquet` | Daily aggregated metrics |
| `searches_journeys.parquet` | Session-level journey data |
| `searches_terms.parquet` | Search term analysis data |
| `searches_term_clicks.parquet` | Term-to-content click pairs |

The dashboard automatically resolves the parquet file path relative to the HTML file location (`../output/`), with fallback paths.

## Department Mapping (Optional)

The processing script can map raw department OU codes to Business Division names using monthly GEDULD Excel files.

### Setup

Place GEDULD files in the `mappings/` folder with the naming convention `GEDULD_YYYY_MM_DD.xlsx`. Each file represents a monthly HR snapshot. The day in the filename is ignored — only the year and month matter.

### How It Works

1. Raw department values from App Insights follow the pattern `"AAAA - Department Name"`, where `AAAA` is the OU Code (characters before ` - `)
2. The script extracts the OU Code and looks up the corresponding "GCRS Division Desc" (Business Division) from the GEDULD file
3. The mapped Business Division replaces the raw department value in all outputs

### Required Columns in GEDULD Files

| Column | Description |
|--------|-------------|
| `OU Code` | Organizational unit code (matches prefix in department field) |
| `GCRS Division Desc` | Business Division name to use as the mapped department |
| `Work Location Country` | Country name (used for region mapping) |
| `Work Location Region` | Region name (e.g., EMEA, AMERICAS, APAC, SWITZERLAND) |

### Temporal Matching

People can change divisions over time. The script uses a **best-available** fallback strategy:

1. **Exact month match** — event in Feb 2026 → uses GEDULD Feb 2026
2. **Most recent prior file** — event in Jan 2026 but no Jan file → uses GEDULD Dec 2025
3. **Earliest future file** — event in Jan 2026 but no prior files → uses GEDULD Feb 2026
4. **Raw department value** — only if no GEDULD file contains that OU Code at all

### Traceability

The processing script adds two extra columns to the enriched data for debugging:
- `department_raw` — the original department value from App Insights
- `department_ou_code` — the extracted OU Code

## Region Mapping

The processing script maps each user's country (location) to a geographic region. This is used in the dashboard to show Region instead of individual country names.

### How It Works

1. **GEDULD-based lookup (primary):** If GEDULD files are present, the script reads the `Work Location Country` → `Work Location Region` relationship from the most recent file
2. **Hardcoded fallback:** If a country is not found in the GEDULD data, a built-in mapping assigns it to one of four regions:

| Region | Description |
|--------|-------------|
| `SWITZERLAND` | Switzerland |
| `EMEA` | Europe (excl. Switzerland), Middle East, Africa |
| `AMERICAS` | North and South America |
| `APAC` | Asia-Pacific |

Region mapping is **not time-aware** — countries don't change regions, so the most recent GEDULD file is always used. Both `location` (country) and `region` columns are available in the parquet files.

## Starting the Dashboard

### Option 1: Python HTTP Server (Recommended)

1. Open a terminal and navigate to the project root:
   ```bash
   cd /path/to/SearchAnalytics
   ```

2. Start a local web server:
   ```bash
   python -m http.server 8080
   ```

3. Open your browser and navigate to:
   ```
   http://localhost:8080/dashboard/dashboard.html
   ```

### Option 2: VS Code Live Server

1. Install the "Live Server" extension in VS Code
2. Ensure parquet files exist in the `output/` folder
3. Open the project root folder in VS Code
4. Right-click on `dashboard/dashboard.html`
5. Select "Open with Live Server"

### Option 3: Node.js HTTP Server

1. Install http-server globally:
   ```bash
   npm install -g http-server
   ```

2. Navigate to the project root and start the server:
   ```bash
   cd /path/to/SearchAnalytics
   http-server -p 8080
   ```

3. Open `http://localhost:8080/dashboard/dashboard.html`

## Executive Questions This Dashboard Answers

| Business Question | Where to Look | Key Metrics |
|---|---|---|
| **How effective is our intranet search?** | Overview tab → KPI cards | Success Rate, Effectiveness Score |
| **What content is missing from the intranet?** | Search Terms → Content Gaps sub-tab | Null Result Rate, Priority Score, problem terms list |
| **Are users finding what they need?** | Overview → Journey Outcomes funnel; Journeys tab | Success Rate, Abandonment Rate, Recovery Rate |
| **What are employees searching for most?** | Search Terms → Top Terms sub-tab | Searches, CTR per term, term status badges |
| **When do employees search?** | Patterns tab → Time of Day, Weekday, Seasonality | Regional distribution (APAC/CET/Americas), weekday peaks |
| **How fast is search?** | Performance tab | Avg Latency, P50/P95 Latency, Time to Click |
| **Are new topics emerging?** | Search Terms → New/Trending sub-tab | Term age, lifecycle stage, search volume |
| **How do users behave in search sessions?** | Journeys tab → Complexity, Duration, Reformulation | Session complexity, reformulation rate, recovery rate |
| **Which content gets clicked from search?** | Insights & Export → Content Discovery | Click counts, avg click position, top departments |
| **How is search trending over time?** | Overview → Daily Trends chart; Insights & Export → Daily Trends | Searches/day, success rate trend, user cohorts |

---

## Metric Calculations Reference

### Core KPI Metrics

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **Total Searches** | `SUM(search_starts)` | Total number of search-triggered events in the period |
| **Unique Sessions** | `SUM(unique_sessions)` | Distinct search sessions (a session groups events by user within a time window) |
| **Unique Users** | `SUM(unique_users)` | Distinct users identified by cookie |
| **Success Rate** | `sessions_with_clicks / sessions_with_results × 100` | % of sessions (that had results) where the user clicked a result. Higher = better. |
| **Null Result Rate** | `null_results / result_events × 100` | % of result events that returned zero results. Lower = better. |
| **Abandonment Rate** | `sessions_abandoned / sessions_with_results × 100` | % of sessions (that had results) where the user left without clicking. Lower = better. |
| **Effectiveness Score** | `Success Rate − (Null Rate × 0.5)` | Composite score penalizing both low click-through and high null rates. Higher = better. |
| **Searches per Session** | `total_searches / total_sessions` | Average number of search queries per session. High values may indicate difficulty finding content. |

### Per-Term Metrics

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **Success CTR** | `success_clicks / searches × 100` | % of searches for this term that led to a result click |
| **Null Rate** | `null_results / result_events × 100` | % of result events for this term that returned zero results |
| **Avg Results** | `sum_result_count / result_events` | Average number of results shown per search for this term |
| **Score** | `CTR − (Null Rate × 0.5)` | Per-term effectiveness score (same formula as overall Effectiveness Score) |
| **Priority Score** | `searches × null_rate / 100` | Volume-weighted gap score. High priority = many users hitting zero results. >= 10 = "High Priority". |

### Performance Metrics

| Metric | Formula | Source |
|--------|---------|--------|
| **Avg Latency** | `AVG(sec_search_to_result)` | Average time from search trigger to results displayed |
| **P50 Latency** | `PERCENTILE_CONT(0.5)` over `sec_search_to_result` | Median latency — 50% of searches are faster than this |
| **P95 Latency** | `PERCENTILE_CONT(0.95)` over `sec_search_to_result` | 95th percentile — only 5% of searches are slower |
| **Avg Time to Click** | `AVG(sec_result_to_click)` | Average time from results displayed to first click |
| **Avg Session Duration** | `AVG(total_duration_sec)` | Average total duration of a search session |

### Classifications

**Journey Outcomes** (pre-calculated per session):

| Outcome | Definition |
|---------|------------|
| **Success** | User clicked a search result |
| **Engaged** | User clicked something (trending, tabs, filters) but not a result |
| **Abandoned** | Results were shown but user left without any click |
| **No Results** | All searches in the session returned zero results |
| **Unknown** | Could not be classified |

**Session Complexity** (by event count):

| Level | Events |
|-------|--------|
| Single Action | 1 event |
| Simple | 2–3 events |
| Medium | 4–6 events |
| Complex | 7+ events |

**Term Lifecycle** (by term age in days since first seen):

| Stage | Age |
|-------|-----|
| New | 1–3 days |
| Emerging | 4–7 days |
| Establishing | 8–14 days |
| Established | 15–30 days |
| Mature | 31+ days |

**Term Performance Classification:**

| Category | Condition |
|----------|-----------|
| Zero Results | null_rate >= 100% |
| Mostly No Results | null_rate >= 50% |
| No Clicks | CTR = 0% |
| Low CTR | CTR < 20% |
| Success | Everything else |

**Seasonality Concentration** (for terms with 6+ months of data):

| Type | Concentration (peak / avg volume) |
|------|-----------------------------------|
| Highly Seasonal | >= 3.0× |
| Moderately Seasonal | >= 2.0× |
| Slightly Seasonal | >= 1.5× |
| Consistent | < 1.5× |

**Peak Region** (based on hour-of-day search volume):

| Region | CET Hours |
|--------|-----------|
| APAC | 03:00–09:00 |
| CET | 09:00–16:00 |
| Americas | 16:00–22:00 |
| Dead Time | 22:00–03:00 |

---

## Tab-by-Tab Walkthrough

### 1. Overview Tab

The landing page provides a high-level picture of search health.

**Executive Summary** (top section):
- Auto-generated insight cards with conditional messages:
  - Search Activity (always shown): daily average searches
  - Good Success Rate (green): shown when Success Rate >= 40%
  - Low Success Rate (red): shown when Success Rate < 25%
  - Content Gaps Detected (amber): shown when Null Rate > 10%
  - Top Content Gap (action card): the term with the most null results

**KPI Cards** (8 cards):

| Card | Color Bar | What It Shows |
|------|-----------|---------------|
| Total Searches | Red (brand) | Search volume with period-over-period change |
| Unique Sessions | Gray | Session count with change |
| Unique Users | None | User count with change |
| Success Rate | Green | % sessions with result clicks |
| Null Result Rate | Amber | % results returning zero |
| Abandonment Rate | Red | % sessions abandoned |
| Effectiveness Score | Gray | Composite score (higher = better) |
| Searches/Session | None | Avg queries per session |

**Daily Trends Chart** (line chart, dual Y-axis):
- Left axis: Searches (solid gray line, filled area) + Sessions (dashed gray line)
- Right axis: Success Rate % (green line, 0–100 scale)
- X-axis: dates
- Use this to spot trends and anomalies over time.

**Journey Outcomes Funnel** (horizontal bar):
- Shows session counts for Success, Engaged, Abandoned, No Results
- Click any bar to filter the entire dashboard by that outcome

**Outcome Doughnut**:
- Same data as the funnel in pie form. Click a slice to filter.

**Hourly Distribution** (bar chart):
- 4 bars: APAC, CET, Americas, Dead Time
- Shows when searches happen by time zone

**Weekday Distribution** (bar chart):
- 7 bars (Mon–Sun). Click a bar to filter all data by that weekday.

**User Cohorts** (stacked bar, time series):
- New Users (dark) vs Returning Users (light) over time

**Click Categories** (horizontal bar):
- Result (Success), Trending, Tab, Pagination, Filter
- Shows what users click on besides search results

---

### 2. Search Terms Tab

Contains 3 sub-tabs:

#### Top Terms
- **Table columns**: Search Term, Searches, Avg Results, Users, Success CTR, Null Rate, Status (badge), Score
- **Status badges**: High CTR (>30%, green), Moderate CTR (10–30%, neutral), Low CTR (<10%, amber), High Null Rate (>50%, red)
- **Text search**: Filter terms by keyword (with autocomplete)
- **Status filter dropdown**: Filter by badge status
- Click any term row to open the **Term Detail Modal** showing:
  - 4 metric cards (Searches, Users, CTR, Null Rate)
  - Daily trend mini-chart for that term
  - Recommendation text if Null Rate > 50%

#### Content Gaps
- Shows terms with null results, requiring minimum 3 searches
- **Table columns**: Search Term, Searches, Null Results, Null Rate, Priority (High/Medium/Low), Action
- Priority: Top 5 = High, 5–10 = Medium, 10+ = Low (by null result count)
- Top 20 terms displayed

**Term Performance Doughnut**:
- Shows distribution of all terms across 5 categories: Success, Low CTR, No Clicks, Mostly No Results, Zero Results

**Query Length Distribution** (bar chart):
- Shows search volume by word count (1 word, 2 words, …, 5+ words)

#### New/Trending
- Terms first seen within the current date filter period
- **Table columns**: Search Term, Searches, Users, Term Age, Lifecycle (badge)
- Top 20 terms displayed

---

### 3. User Journeys Tab

Analyzes session-level behavior patterns.

**Session Complexity** (pie chart):
- Single Action / Simple / Medium / Complex
- Click a slice to filter all journey data by complexity level

**Session Duration** (bar chart):
- Buckets: <5s, 5–30s, 30–60s, 1–3min, 3–5min, >5min
- Click a bar to filter

**Reformulation Rate** (doughnut):
- "Refined Query" (user changed search terms) vs "Single Query"
- Click to filter by reformulation behavior

**Null Result Recovery** (doughnut):
- Only shows sessions that encountered a null result
- "Recovered" (eventually clicked a result) vs "Gave Up"
- Click to filter

**Outcome by Complexity** (stacked bar):
- X-axis: complexity levels; stacked by outcome (Success, Engaged, Abandoned, No Results)
- Shows whether more complex sessions have better or worse outcomes

---

### 4. Performance Tab

Focuses on search speed and responsiveness.

**5 Metric Cards**: Avg Latency, P50 Latency, P95 Latency, Avg Time to Click, Avg Session Duration

**Search Latency Distribution** (bar chart with RAG colors):
- Green bars: fast buckets; Amber: medium; Red: slow
- Shows how latency is distributed across sessions

**Time to Click Distribution** (bar chart):
- Time from results displayed to first click, in buckets
- Excludes "No Click" sessions

**Latency Trend** (line chart):
- Two lines over time: Avg Latency (solid) + Avg Time to Click (dashed)
- Use to detect performance degradation

---

### 5. Patterns Tab

Identifies temporal patterns in search behavior.

**Time of Day** (bar chart + doughnut):
- Search volume split by APAC / CET / Americas / Dead Time
- Bar chart shows absolute counts; doughnut shows proportions

**Top Terms by Peak Time** (table):
- Columns: Term, Searches, Peak Period, Concentration, regional %
- Shows which terms are searched predominantly in which time zone

**Monthly Seasonality** (bar chart):
- Jan–Dec search volumes. Q4 (Oct–Dec) highlighted darker.
- Identifies seasonal search patterns

**Seasonal Search Terms** (table):
- Columns: Term, Total Searches, Peak Month, Concentration, Type, Coverage
- Filterable by month and seasonality type
- Concentration = peak monthly volume / average monthly volume

---

### 6. Insights & Export Tab

The data export and deep-dive tab with 4 sub-tabs. Each sub-tab has KPI summary cards, a sortable/filterable table, and CSV/XLSX export buttons.

#### Search Terms
- **KPI Cards**: Total Terms, Content Gaps (null rate >= 50%), Avg Success CTR, High Priority Terms (priority >= 10)
- **Table columns**: Search Term, Department, Region, Words, Searches, Users, Null Rate %, Avg Results, CTR %, Avg Pos, Engagement %, Priority, Lifecycle, Outcome, Peak Region, First Seen
- All columns sortable. Text search with autocomplete. Filterable by Outcome and Lifecycle.
- XLSX export includes a Glossary sheet with column definitions.

#### Sessions
- **KPI Cards**: Total Sessions, Success Rate, Avg Duration, Reformulation Rate
- **Table columns**: Date, Department, Region, Device, Searches, Clicks, Success, Outcome, Complexity, Duration, Reformulated
- Sortable. Filterable by Outcome and Complexity.

#### Daily Trends
- **KPI Cards**: Total Days, Avg Daily Searches, Avg Daily Users, Avg Success Rate
- **Table columns**: Date, Day, Sessions, Users, Searches, Success Rate %, Null Rate %, Avg Latency (ms), New Users
- All columns sortable.

#### Content Discovery
- **KPI Cards**: Term-Content Pairs, Unique Content URLs, Avg Click Position, Top Department
- **Table columns**: Search Term, Content Title, Clicks, Users, Sessions, Avg Pos, Department, Device
- Sortable. Text search on term or content title. Min clicks filter.

---

## Interactive Features Reference

### Click-to-Filter
Many charts support click-to-filter. Clicking a chart element filters all dashboard data by that value. Active filters appear as removable tags above the content area.

| Chart | Filters By |
|-------|-----------|
| Journey Outcomes Funnel | Journey outcome |
| Outcome Doughnut | Journey outcome |
| Weekday Distribution | Day of week |
| Session Complexity Pie | Complexity level |
| Session Duration Bar | Duration bucket |
| Reformulation Doughnut | Had reformulation (yes/no) |
| Recovery Doughnut | Recovered from null (yes/no) |
| Outcome by Complexity | Complexity level |

Click the same element again to remove the filter. Click "Clear all" to remove all active filters.

### Date Filtering
Pre-defined presets: Last 7 days, Last 30 days, This month, Last month, This year (YTD), Last year, All time (default). Custom date range via From/To inputs.

### Term Detail Modal
Click any term in the Top Terms, Content Gaps, or Insights tables to open a detail modal with per-term KPIs and a daily trend chart.

### Autocomplete Search
All search inputs provide autocomplete suggestions with highlighted matches and keyboard navigation (arrows, Enter, Escape).

### Export
Each Insights sub-tab offers CSV and XLSX export. XLSX files include a Glossary sheet with definitions for every column.

---

## Troubleshooting

### Dashboard Shows "Loading..."

- Verify parquet files are in the same directory as the HTML file
- Ensure you're accessing via HTTP (not file://)
- Check browser console for specific error messages

### "Failed to Load Data" Error

- Confirm parquet files were generated successfully
- Check file permissions
- Verify the web server is running

### Slow Performance

- Filter to a smaller date range
- Ensure you're using a modern browser
- Close other browser tabs to free memory

### Data Not Updating

- Regenerate parquet files with the latest data
- Clear browser cache (Ctrl+Shift+R or Cmd+Shift+R)
- Restart the local web server

## Technical Details

### Dependencies (loaded via CDN)

- **DuckDB WASM** v1.28.0 - In-browser SQL database
- **Chart.js** v4.4.1 - Charting library
- **chartjs-adapter-date-fns** v3.0.0 - Date handling for charts
- **SheetJS** - XLSX export

### Browser Compatibility

| Browser | Minimum Version |
|---------|-----------------|
| Chrome | 88+ |
| Firefox | 78+ |
| Edge | 88+ |
| Safari | 14+ |

### Data Processing

The dashboard uses DuckDB WASM to:
1. Load parquet files directly in the browser
2. Execute SQL queries for aggregations
3. Filter and transform data based on user selections

All processing happens client-side; no data is sent to external servers.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 3.0 | 2026-02-24 | Added department mapping (GEDULD temporal lookup), region mapping (GEDULD + hardcoded fallback), Location→Region in dashboard display |
| 2.0 | 2026-02-24 | Added executive questions, metric calculations, tab walkthrough |
| 1.0 | 2026-01-27 | Initial documentation |
