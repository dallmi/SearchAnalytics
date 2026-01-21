# Search Analytics Processing Script

This document explains how the `process_search_analytics.py` script works step by step.

## Overview

The script processes weekly search analytics data exported from KQL (Kusto Query Language) queries. It creates and maintains a DuckDB database with all calculated analytics columns and exports Parquet files for Power BI consumption.

## Prerequisites

- Python 3.8+
- Required packages: `duckdb`, `pandas`
- Input files: KQL exports in `.xlsx`, `.xls`, or `.csv` format

## Directory Structure

```
SearchAnalytics/
├── input/                          # Place KQL export files here
│   └── search_export_2025_01_13.xlsx
├── data/
│   └── searchanalytics.db          # DuckDB database (created automatically)
├── output/                         # Parquet files for Power BI
│   ├── searches_raw.parquet
│   ├── searches_daily.parquet
│   └── searches_journeys.parquet
└── process_search_analytics.py
```

## Usage

### Basic Usage (Auto-detect latest file)
```bash
python process_search_analytics.py
```
The script automatically finds the file with the most recent date suffix in the `input/` folder.

### Process a Specific File
```bash
python process_search_analytics.py input/search_export_2025_01_13.xlsx
```

### Full Refresh (Reprocess all files)
```bash
python process_search_analytics.py --full-refresh
```
This deletes the existing database and reprocesses all files in chronological order.

---

## Step-by-Step Processing Flow

### Step 1: File Discovery

The script looks for input files in the `input/` folder:

1. **Scans for files** with extensions `.xlsx`, `.xls`, or `.csv`
2. **Parses date from filename** using pattern `_YYYY_MM_DD`
   - Example: `search_export_2025_01_13.xlsx` → Date: 2025-01-13
3. **Selects the most recent file** based on the date in the filename
4. **Fallback**: If no files have date suffixes, uses file modification time

```python
# Filename format expected:
search_export_2025_01_13.xlsx  ✓
search_data_2025_01_13.csv     ✓
export.xlsx                     ✗ (no date - uses fallback)
```

### Step 2: Load Data into Temporary Table

The script loads the input file into a temporary DuckDB table:

1. **Detects file format** (Excel or CSV)
2. **Creates temp table** using DuckDB's native file readers:
   - Excel: `st_read()` function
   - CSV: `read_csv()` with auto-detection
3. **Normalizes column names**:
   - `user_Id` → `user_id`
   - `session_Id` → `session_id`
4. **Converts German date formats** (e.g., `13.01.2025 14:30`) to proper timestamps

### Step 3: Upsert Data (Merge Strategy)

The script uses an upsert strategy to handle overlapping data exports:

**Primary Key**: `timestamp` + `user_id` + `session_id` + `name`

**Process**:
1. If `searches_raw` table doesn't exist → Create it from temp table
2. If table exists:
   - **DELETE** existing rows where PK matches new data
   - **INSERT** all rows from the new file
   - This ensures the latest file's data takes precedence

```sql
-- Simplified upsert logic:
DELETE FROM searches_raw
WHERE EXISTS (
    SELECT 1 FROM temp_import t
    WHERE searches_raw.timestamp = t.timestamp
      AND searches_raw.user_id = t.user_id
      AND searches_raw.session_id = t.session_id
      AND searches_raw.name = t.name
);

INSERT INTO searches_raw SELECT * FROM temp_import;
```

### Step 4: Add Calculated Columns

The script creates a `searches` table with all calculated analytics columns:

#### Session Identification
| Column | Description | Example |
|--------|-------------|---------|
| `session_date` | Date portion of timestamp | `2025-01-13` |
| `session_key` | Unique session identifier | `2025-01-13_user123_sess456` |

#### Event Sequencing (Window Functions)
| Column | Description |
|--------|-------------|
| `event_order` | Position within session (1, 2, 3...) |
| `prev_event` | Previous event name in session |
| `prev_timestamp` | Previous event timestamp |
| `ms_since_prev_event` | Milliseconds since previous event |
| `sec_since_prev_event` | Seconds since previous event |
| `time_since_prev_bucket` | Time bucket (`< 0.5s`, `0.5-1s`, `1-2s`, etc.) |

#### Search Term Analysis
| Column | Description |
|--------|-------------|
| `search_term_normalized` | Lowercase, trimmed search query |
| `search_term_length` | Character count of search term |
| `search_term_word_count` | Word count of search term |

#### Time Extraction
| Column | Description |
|--------|-------------|
| `event_hour` | Hour of day (0-23) |
| `event_weekday` | Day name (`Monday`, `Tuesday`, etc.) |
| `event_weekday_num` | ISO weekday (1=Monday, 7=Sunday) |

#### Behavioral Flags
| Column | Description |
|--------|-------------|
| `is_null_result` | True if search returned 0 results |
| `is_clickable_result` | True if search returned >0 results (user could click) |
| `click_category` | Simplified click type (`General`, `All`, `News`, `GoTo`, `People`) |
| `is_first_search_of_day` | True if this is user's first search of the day |

### Step 5: Export Parquet Files

Four Parquet files are generated for Power BI:

#### 1. `searches_raw.parquet`
- **Content**: All event-level data with calculated columns
- **Use case**: Detailed drill-down analysis
- **Size**: Largest file (contains all rows)

#### 2. `searches_daily.parquet`
- **Content**: Aggregated metrics by day for trend analysis
- **Columns include**:
  - `total_events`, `unique_sessions`, `unique_users`, `unique_search_terms`
  - `search_starts`, `result_events`, `click_events`, `null_results`, `clickable_results`
  - **Rate metrics**:
    - `click_through_rate_pct` - Clicks / Searches × 100
    - `null_rate_pct` - Null results / Results shown × 100
    - `abandonment_rate_pct` - (Results without click) / Results × 100
  - **Session metrics**:
    - `avg_searches_per_session` - Average searches per session
  - **Search term metrics** (includes SUM columns for weighted DAX calculations):
    - `avg_search_term_length`, `avg_search_term_words` - Daily averages
    - `sum_search_term_length`, `sum_search_term_words` - Daily sums for weighted avg in Power BI
    - `search_term_count` - Count of search terms (denominator for weighted avg)
  - `first_searches_of_day`
  - Click breakdowns by category (`clicks_general`, `clicks_all`, `clicks_news`, `clicks_goto`, `clicks_people`)

#### 3. `searches_journeys.parquet`
- **Content**: Session-level data with timing metrics (consolidated)
- **Event counts**:
  - `search_count_in_session`, `result_count`, `click_count`, `unique_search_terms`
  - `null_result_count`, `max_total_results`
- **Timing metrics**:
  - `sec_search_to_result` - Time from search to results displayed
  - `sec_result_to_click` - Time from results to user click
  - `total_duration_sec` - Total session duration
- **Time buckets**: Pre-categorized performance tiers
  - `search_to_result_bucket`, `result_to_click_bucket`, `session_duration_bucket`
- **Classifications**:
  - `journey_outcome` (`Success`, `No Results`, `Abandoned`, `Unknown`)
  - `had_reformulation` (user modified search query)
  - `session_complexity` (`Single Event`, `Simple`, `Medium`, `Complex`)
- **Click breakdown**: `general_clicks`, `all_tab_clicks`, `news_clicks`, etc.

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        INPUT                                     │
│  input/search_export_2025_01_13.xlsx                            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 1: Load to Temp Table                                      │
│  - Read Excel/CSV                                                │
│  - Normalize column names (user_Id → user_id)                   │
│  - Convert German dates (13.01.2025 → 2025-01-13)              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 2: Upsert to searches_raw                                  │
│  - PK: timestamp + user_id + session_id + name                  │
│  - Delete existing matches, insert new data                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 3: Calculate Columns → searches table                      │
│  - Session keys, event ordering                                  │
│  - Time intervals (window functions)                             │
│  - Search term analysis                                          │
│  - Behavioral flags                                              │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  STEP 4: Export Parquet Files                                    │
│  ├── searches_raw.parquet       (event-level)                   │
│  ├── searches_daily.parquet     (daily aggregates)              │
│  └── searches_journeys.parquet  (session data with timing)      │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                        OUTPUT                                    │
│  data/searchanalytics.db    (DuckDB database)                   │
│  output/*.parquet           (Power BI files)                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Weekly Processing Workflow

### Recommended Weekly Process

1. **Export data from KQL** (manual step)
   - Run your KQL query in Azure/Log Analytics
   - Export results as Excel (.xlsx)
   - Save with date suffix: `search_export_2025_01_20.xlsx`

2. **Place file in input folder**
   ```
   input/search_export_2025_01_20.xlsx
   ```

3. **Run the processing script**
   ```bash
   python process_search_analytics.py
   ```

4. **Refresh Power BI**
   - Open Power BI
   - Refresh data sources pointing to `output/*.parquet`

### Handling Overlapping Data

If your weekly exports have overlapping date ranges (e.g., last 30 days each time):
- The script handles this automatically via upsert
- Duplicate events (same timestamp + user + session + name) are replaced
- Most recent file's data always takes precedence

### Full Database Rebuild

If you need to rebuild from scratch:
```bash
python process_search_analytics.py --full-refresh
```
This processes all files in `input/` in chronological order (oldest first).

---

## Troubleshooting

### No input files found
```
ERROR: No input files found in input/
```
**Solution**: Place your KQL export file in the `input/` folder with format `filename_YYYY_MM_DD.xlsx`

### Column name issues
If you see errors about missing columns like `user_Id`:
- The script auto-converts `user_Id` → `user_id` and `session_Id` → `session_id`
- Ensure your KQL export includes these columns

### German date format not recognized
The script auto-detects and converts German date formats (`DD.MM.YYYY HH:MM`).
If dates aren't converting:
- Check that the date column is VARCHAR type in the source
- Verify format matches `13.01.2025 14:30` or `13.01.2025`

### Power BI refresh issues
If Power BI can't read the Parquet files:
- Ensure `output/` folder path is correctly configured
- Check that DuckDB exported files successfully (check file sizes)
- Verify no process is locking the files

---

## Event Types and Counting Logic

### Search Events
The telemetry captures two search initiation events that can overlap:
- `SEARCH_TRIGGERED` - User initiates a search (presses enter, clicks button)
- `SEARCH_TRIGGERED` - System begins processing the search

**Important:** To avoid double-counting, search counts use only `SEARCH_TRIGGERED` events. This applies to:
- `search_starts` in daily parquet
- `search_count_in_session` in journeys parquet
- Click-through rate calculations
- Average searches per session

### Other Event Types
| Event | Description |
|-------|-------------|
| `SEARCH_RESULT_COUNT` | Results are displayed to user |
| `SEARCH_TAB_CLICK` | Click on general result |
| `SEARCH_ALL_TAB_PAGE_CLICK` | Click on "All" tab result |
| `SEARCH_NEWS_TAB_PAGE_CLICK` | Click on news result |
| `SEARCH_GOTO_TAB_PAGE_CLICK` | Click on "Go To" result |

---

## Technical Details

### DuckDB Functions Used

| Function | Purpose |
|----------|---------|
| `st_read()` | Read Excel files |
| `read_csv()` | Read CSV files with auto-detection |
| `LAG()` | Get previous row values within session |
| `ROW_NUMBER()` | Assign event order within session |
| `ISODOW()` | ISO weekday (1=Mon, 7=Sun) |
| `DATEDIFF()` | Calculate time differences |
| `DATE_TRUNC()` | Extract date from timestamp |

### Performance Notes

- DuckDB processes data in-memory, making it fast for analytics
- Parquet files are columnar and compressed, efficient for Power BI
- Window functions run in a single pass over the data
- Upsert uses EXISTS subquery for efficient conflict detection

### Dependencies

```python
import duckdb    # Database engine
import pandas    # DataFrame handling (used for summary output)
from pathlib import Path  # Cross-platform file paths
from datetime import datetime  # Date parsing
import re        # Regex for filename date extraction
import glob      # File pattern matching
```
