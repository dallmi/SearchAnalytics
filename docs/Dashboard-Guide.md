# Search Analytics Dashboard Guide

## Overview

The Search Analytics Dashboard is a self-contained HTML application that provides interactive visualization and analysis of search telemetry data. It runs entirely in the browser using DuckDB WASM, requiring no server-side infrastructure.

## Prerequisites

Before running the dashboard, ensure you have:

1. **Generated parquet files** from the Python processing script
2. **A modern web browser** (Chrome, Firefox, Edge, or Safari)
3. **A local web server** (required for loading parquet files due to browser security restrictions)

## Required Data Files

The dashboard expects these parquet files in the same directory as the HTML file:

| File | Description |
|------|-------------|
| `searches_daily.parquet` | Daily aggregated metrics |
| `searches_journeys.parquet` | Session-level journey data |
| `searches_terms.parquet` | Search term analysis data |

## Starting the Dashboard

### Option 1: Python HTTP Server (Recommended)

1. Open a terminal and navigate to the dashboard directory:
   ```bash
   cd /path/to/SearchAnalytics/dashboard
   ```

2. Copy the required parquet files to this directory:
   ```bash
   cp ../output/searches_daily.parquet .
   cp ../output/searches_journeys.parquet .
   cp ../output/searches_terms.parquet .
   ```

3. Start a local web server:
   ```bash
   python -m http.server 8080
   ```

4. Open your browser and navigate to:
   ```
   http://localhost:8080/search-analytics-dashboard.html
   ```

### Option 2: VS Code Live Server

1. Install the "Live Server" extension in VS Code
2. Copy parquet files to the dashboard directory
3. Right-click on `search-analytics-dashboard.html`
4. Select "Open with Live Server"

### Option 3: Node.js HTTP Server

1. Install http-server globally:
   ```bash
   npm install -g http-server
   ```

2. Navigate to the dashboard directory and start the server:
   ```bash
   cd /path/to/SearchAnalytics/dashboard
   http-server -p 8080
   ```

3. Open `http://localhost:8080/search-analytics-dashboard.html`

## Dashboard Features

### Tabs

| Tab | Purpose |
|-----|---------|
| **Overview** | Key metrics, trends, and automated insights |
| **Top Terms** | Most searched terms with CTR and null rates |
| **Content Gaps** | Terms returning zero results (content opportunities) |
| **Journeys** | Session-level analysis and user behavior patterns |

### Date Filtering

- Use the date range selectors in the header to filter data
- All visualizations and metrics update automatically
- The data status indicator shows the loaded date range

### Key Metrics

| Metric | Description |
|--------|-------------|
| **Total Searches** | Count of SEARCH_TRIGGERED events |
| **Sessions** | Unique search sessions |
| **Users** | Distinct users (cookie-based) |
| **Success Rate** | Percentage of sessions with result clicks |
| **Null Rate** | Percentage of searches returning zero results |
| **Avg Results** | Average number of results shown per search |

### Interactive Features

- **KPI Cards**: Click to see period-over-period comparisons
- **Top Terms Table**: Click any term to view detailed analysis
- **Charts**: Hover for detailed tooltips
- **Sorting**: Click table headers to sort data

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
| 1.0 | 2025-01-27 | Initial documentation |
