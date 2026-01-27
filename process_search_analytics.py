#!/usr/bin/env python3
"""
Search Analytics Weekly Processing Script

This script processes weekly search analytics data extracted via KQL.
It creates/updates a DuckDB database with all calculated columns and exports
Parquet files for Power BI consumption.

Usage:
    python process_search_analytics.py                    # Auto-detect latest file in input/
    python process_search_analytics.py input/export.xlsx  # Process specific file
    python process_search_analytics.py --full-refresh     # Delete DB and reprocess all files

Input folder: input/
    Place your KQL export files here with date suffix _YYYY_MM_DD, e.g.:
    - search_export_2025_01_13.xlsx
    - search_export_2025_01_13.csv

    The file with the most recent date in the filename will be processed.

Output:
    - data/searchanalytics.db              (DuckDB database)
    - output/searches_raw.parquet          (all event-level data)
    - output/searches_daily.parquet        (aggregated by day)
    - output/searches_journeys.parquet     (session-level data with timing)

Primary Key: timestamp + user_id + session_id + name
    On conflict, the latest file's data takes precedence.
"""

import sys
import os
import re
import glob
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime


def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def extract_date_from_filename(filepath):
    """
    Extract date from filename with format _YYYY_MM_DD.
    Returns a date object or None if not found.
    """
    filename = Path(filepath).stem
    # Match _YYYY_MM_DD pattern
    match = re.search(r'_(\d{4})_(\d{2})_(\d{2})$', filename)
    if match:
        try:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return datetime(year, month, day).date()
        except ValueError:
            return None
    return None


def find_latest_input_file(input_dir):
    """
    Find the latest input file in the input directory based on date in filename.
    Expects format: filename_YYYY_MM_DD.xlsx or filename_YYYY_MM_DD.csv
    """
    patterns = ['*.xlsx', '*.xls', '*.csv']
    all_files = []

    for pattern in patterns:
        all_files.extend(glob.glob(str(input_dir / pattern)))

    if not all_files:
        return None

    # Parse dates from filenames and sort
    files_with_dates = []
    for f in all_files:
        file_date = extract_date_from_filename(f)
        if file_date:
            files_with_dates.append((Path(f), file_date))

    if not files_with_dates:
        # No files with valid date suffix found, fall back to modification time
        log("  Warning: No files with _YYYY_MM_DD suffix found, using modification time")
        all_files.sort(key=os.path.getmtime, reverse=True)
        return Path(all_files[0])

    # Sort by date (most recent first)
    files_with_dates.sort(key=lambda x: x[1], reverse=True)

    return files_with_dates[0][0]


def get_all_input_files(input_dir):
    """Get all input files sorted by date in filename (oldest first for processing order)."""
    patterns = ['*.xlsx', '*.xls', '*.csv']
    all_files = []

    for pattern in patterns:
        all_files.extend(glob.glob(str(input_dir / pattern)))

    # Parse dates from filenames and sort
    files_with_dates = []
    files_without_dates = []

    for f in all_files:
        file_date = extract_date_from_filename(f)
        if file_date:
            files_with_dates.append((Path(f), file_date))
        else:
            files_without_dates.append(Path(f))

    # Sort by date (oldest first for chronological processing)
    files_with_dates.sort(key=lambda x: x[1])

    # Return dated files first (in order), then undated files by modification time
    result = [f for f, _ in files_with_dates]
    files_without_dates.sort(key=os.path.getmtime)
    result.extend(files_without_dates)

    return result


def load_file_to_temp_table(con, input_path, temp_table='temp_import'):
    """Load a CSV or Excel file into a temporary table."""
    con.execute(f"DROP TABLE IF EXISTS {temp_table}")

    if input_path.suffix.lower() in ['.xlsx', '.xls']:
        # Use pandas to read Excel files (works in corporate environments without DuckDB extension)
        import pandas as pd

        # First pass: read only column names (nrows=0 to avoid parsing data)
        df_cols = pd.read_excel(input_path, nrows=0)
        all_cols = df_cols.columns.tolist()
        timestamp_cols = [col for col in all_cols if 'timestamp' in col.lower()]

        # Read Excel with timestamp columns as strings to preserve precision
        # Excel datetime values lose subsecond precision, so we must read as string
        if timestamp_cols:
            dtype_dict = {col: str for col in timestamp_cols}
            df = pd.read_excel(input_path, dtype=dtype_dict)
            log(f"  Reading timestamp columns as strings: {timestamp_cols}")
        else:
            df = pd.read_excel(input_path)

        con.register('excel_df', df)
        con.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM excel_df")
        con.unregister('excel_df')
    else:
        con.execute(f"""
            CREATE TABLE {temp_table} AS
            SELECT * FROM read_csv('{input_path}', auto_detect=true)
        """)

    # Normalize column names
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    col_names = schema['column_name'].tolist()

    rename_map = {
        'user_Id': 'user_id',
        'session_Id': 'session_id',
        'timestamp [UTC]': 'timestamp'  # App Insights export column name
    }
    for old_name, new_name in rename_map.items():
        if old_name in col_names:
            con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{old_name}" TO {new_name}')

    # Convert date formats (German dd.MM.yyyy and App Insights dd/MM/yyyy)
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    varchar_cols = schema[schema['column_type'] == 'VARCHAR']['column_name'].tolist()

    for col in varchar_cols:
        sample = con.execute(f'SELECT "{col}" FROM {temp_table} WHERE "{col}" IS NOT NULL LIMIT 1').df()
        if len(sample) > 0:
            val = str(sample.iloc[0, 0])
            fmt = None

            # Format: dd/MM/yyyy HH:mm:ss.fffffff (App Insights export with microseconds)
            # Note: strptime %f only supports 6 digits, so we truncate longer fractional seconds
            if re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}\.\d+$', val):
                fmt = '%d/%m/%Y %H:%M:%S.%f'
                # Check if fractional seconds > 6 digits, need special handling
                frac_part = val.split('.')[-1]
                if len(frac_part) > 6:
                    fmt = 'TRUNCATE_FRAC'  # Special marker for truncation
            # Format: dd/MM/yyyy HH:mm:ss (App Insights without microseconds)
            elif re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$', val):
                fmt = '%d/%m/%Y %H:%M:%S'
            # Format: dd/MM/yyyy HH:mm
            elif re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$', val):
                fmt = '%d/%m/%Y %H:%M'
            # Format: dd/MM/yyyy (date only)
            elif re.match(r'^\d{2}/\d{2}/\d{4}$', val):
                fmt = '%d/%m/%Y'
            # Format: dd.MM.yyyy HH:mm:ss (German with seconds)
            elif re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$', val):
                fmt = '%d.%m.%Y %H:%M:%S'
            # Format: dd.MM.yyyy HH:mm (German without seconds)
            elif re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}$', val):
                fmt = '%d.%m.%Y %H:%M'
            # Format: dd.MM.yyyy (German date only)
            elif re.match(r'^\d{2}\.\d{2}\.\d{4}$', val):
                fmt = '%d.%m.%Y'
            # Format: yyyy-MM-dd HH:mm:ss.ffffff (ISO format with microseconds)
            elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$', val):
                fmt = '%Y-%m-%d %H:%M:%S.%f'
            # Format: yyyy-MM-dd HH:mm:ss (ISO format)
            elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', val):
                fmt = '%Y-%m-%d %H:%M:%S'
            # Format: yyyy-MM-ddTHH:mm:ss (ISO with T separator)
            elif re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', val):
                fmt = 'ISO'  # Use DuckDB's native parsing

            if fmt == 'TRUNCATE_FRAC':
                # dd/MM/yyyy HH:mm:ss with >6 digit fractional seconds - truncate to 6 digits
                try:
                    con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                    # Truncate fractional seconds to 6 digits using string manipulation
                    con.execute(f'''
                        UPDATE {temp_table} SET "{col}_temp" = strptime(
                            CASE
                                WHEN "{col}" LIKE '%.%'
                                THEN SUBSTRING("{col}", 1, POSITION('.' IN "{col}") + 6)
                                ELSE "{col}"
                            END,
                            '%d/%m/%Y %H:%M:%S.%f'
                        )
                    ''')
                    con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                    con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                except Exception as e:
                    log(f"  WARNING: Failed to convert '{col}' with truncation: {e}")
            elif fmt == 'ISO':
                # ISO format - use CAST instead of strptime
                try:
                    con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                    con.execute(f'UPDATE {temp_table} SET "{col}_temp" = CAST("{col}" AS TIMESTAMP)')
                    con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                    con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                except Exception:
                    pass
            elif fmt:
                # Regular format - use strptime
                try:
                    con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                    con.execute(f'UPDATE {temp_table} SET "{col}_temp" = strptime("{col}", \'{fmt}\')')
                    con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                    con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                except Exception:
                    pass

    # Fallback: Try to convert any remaining VARCHAR timestamp column using CAST
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    for _, row in schema.iterrows():
        col = row['column_name']
        col_type = row['column_type']
        if col.lower() == 'timestamp' and col_type == 'VARCHAR':
            try:
                con.execute(f'ALTER TABLE {temp_table} ADD COLUMN "{col}_temp" TIMESTAMP')
                con.execute(f'UPDATE {temp_table} SET "{col}_temp" = TRY_CAST("{col}" AS TIMESTAMP)')
                con.execute(f'ALTER TABLE {temp_table} DROP COLUMN "{col}"')
                con.execute(f'ALTER TABLE {temp_table} RENAME COLUMN "{col}_temp" TO "{col}"')
                log(f"  Converted '{col}' to TIMESTAMP using TRY_CAST")
            except Exception as e:
                log(f"  WARNING: Could not convert '{col}' to TIMESTAMP: {e}")

    # Check for timestamp precision and warn if microseconds are missing
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    timestamp_cols = [col for col in schema['column_name'].tolist()
                      if 'timestamp' in col.lower()]

    for col in timestamp_cols:
        # Check if any row has non-zero microseconds
        try:
            result = con.execute(f"""
                SELECT COUNT(*) as cnt
                FROM {temp_table}
                WHERE EXTRACT(microsecond FROM "{col}") != 0
            """).df()
            has_microseconds = result['cnt'][0] > 0

            if not has_microseconds:
                log(f"  WARNING: Column '{col}' has no microsecond precision!")
                log(f"           Event ordering may be inaccurate for timing calculations.")
                log(f"           For precise timing, export from App Insights as CSV (not Excel).")
        except Exception:
            pass  # Column might not be a timestamp type

    row_count = con.execute(f"SELECT COUNT(*) as n FROM {temp_table}").df()['n'][0]
    return row_count


def create_base_table(con):
    """Create the base searches table with proper schema."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS searches_raw (
            -- Original columns will be added dynamically
            -- This is just a placeholder
            _placeholder INTEGER
        )
    """)


def upsert_data(con, temp_table='temp_import'):
    """
    Upsert data from temp table into main searches_raw table.
    Primary key: timestamp + user_id + session_id + name
    """
    # Check if searches_raw exists and has data
    tables = con.execute("SHOW TABLES").df()
    table_exists = 'searches_raw' in tables['name'].values if len(tables) > 0 else False

    if not table_exists:
        # First time: just rename temp table
        con.execute(f"ALTER TABLE {temp_table} RENAME TO searches_raw")
        log("  Created new searches_raw table")
        return

    # Get row count before
    before_count = con.execute("SELECT COUNT(*) as n FROM searches_raw").df()['n'][0]

    # Delete existing rows that match the PK from new data
    con.execute(f"""
        DELETE FROM searches_raw
        WHERE EXISTS (
            SELECT 1 FROM {temp_table} t
            WHERE searches_raw.timestamp = t.timestamp
              AND searches_raw.user_id = t.user_id
              AND searches_raw.session_id = t.session_id
              AND searches_raw.name = t.name
        )
    """)

    deleted_count = before_count - con.execute("SELECT COUNT(*) as n FROM searches_raw").df()['n'][0]

    # Insert all rows from temp table
    con.execute(f"""
        INSERT INTO searches_raw
        SELECT * FROM {temp_table}
    """)

    after_count = con.execute("SELECT COUNT(*) as n FROM searches_raw").df()['n'][0]
    new_rows = after_count - before_count + deleted_count

    if deleted_count > 0:
        log(f"  Updated {deleted_count:,} existing rows, added {new_rows - deleted_count:,} new rows")
    else:
        log(f"  Added {new_rows:,} new rows")

    # Clean up temp table
    con.execute(f"DROP TABLE IF EXISTS {temp_table}")


def add_calculated_columns(con):
    """Add all calculated columns to searches_raw and create final searches table."""
    log("Adding calculated columns...")

    # Drop existing searches table
    con.execute("DROP TABLE IF EXISTS searches")

    # Set timezone to UTC so DuckDB interprets naive timestamps as UTC
    # This is required for AT TIME ZONE conversions to work correctly
    con.execute("SET TIMEZONE='UTC'")

    # Get column list
    schema = con.execute("DESCRIBE searches_raw").df()
    col_names = schema['column_name'].tolist()

    has_user_id = 'user_id' in col_names
    has_session_id = 'session_id' in col_names
    has_timestamp = 'timestamp' in col_names

    # Build the main query with all calculated columns
    con.execute("""
        CREATE TABLE searches AS
        SELECT
            r.* EXCLUDE(name),
            UPPER(r.name) as name,
            -- Timestamp as string for Power BI (Parquet connector loses precision) - UTC
            STRFTIME(timestamp, '%Y-%m-%d %H:%M:%S.%g') as timestamp_str,
            -- CET timestamp (convert UTC to Europe/Berlin which handles CET/CEST automatically)
            -- DuckDB requires: first mark as UTC, then convert to target timezone
            ((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::TIMESTAMP as timestamp_cet,
            STRFTIME((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin', '%Y-%m-%d %H:%M:%S.%g') as timestamp_cet_str,
            -- Session columns (CET-based)
            DATE_TRUNC('day', (timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::DATE as session_date,
            COALESCE(CAST(DATE_TRUNC('day', (timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::DATE AS VARCHAR), '') || '_' ||
                COALESCE(user_id, '') || '_' ||
                COALESCE(session_id, '') as session_key,
            -- Time interval columns (calculated via window functions below)
            NULL::INTEGER as event_order,
            NULL::VARCHAR as prev_event,
            NULL::TIMESTAMP as prev_timestamp,
            NULL::BIGINT as ms_since_prev_event,
            NULL::DOUBLE as sec_since_prev_event,
            NULL::VARCHAR as time_since_prev_bucket,
            -- Search term columns
            LOWER(TRIM(COALESCE(CP_searchQuery, searchQuery, query))) as search_term_normalized,
            LENGTH(LOWER(TRIM(COALESCE(CP_searchQuery, searchQuery, query)))) as search_term_length,
            CASE
                WHEN LOWER(TRIM(COALESCE(CP_searchQuery, searchQuery, query))) IS NULL
                     OR LOWER(TRIM(COALESCE(CP_searchQuery, searchQuery, query))) = '' THEN 0
                ELSE LENGTH(LOWER(TRIM(COALESCE(CP_searchQuery, searchQuery, query)))) -
                     LENGTH(REPLACE(LOWER(TRIM(COALESCE(CP_searchQuery, searchQuery, query))), ' ', '')) + 1
            END as search_term_word_count,
            -- Time extraction (CET-based)
            EXTRACT(HOUR FROM (timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin')::INTEGER as event_hour,
            DAYNAME((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin') as event_weekday,
            ISODOW((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin') as event_weekday_num,
            -- Flags
            CASE
                WHEN name = 'SEARCH_RESULT_COUNT' AND CAST(CP_totalResultCount AS INTEGER) = 0 THEN true
                WHEN name = 'SEARCH_RESULT_COUNT' AND CAST(CP_totalResultCount AS INTEGER) > 0 THEN false
                ELSE NULL
            END as is_null_result,
            CASE
                WHEN name = 'SEARCH_RESULT_COUNT' AND CAST(CP_totalResultCount AS INTEGER) > 0 THEN true
                WHEN name = 'SEARCH_RESULT_COUNT' THEN false
                ELSE NULL
            END as is_clickable_result,
            -- Store result count for aggregation (sum/count pattern for weighted avg)
            CASE
                WHEN name = 'SEARCH_RESULT_COUNT' THEN CAST(CP_totalResultCount AS INTEGER)
                ELSE NULL
            END as cp_total_result_count,
            -- Click category: categorizes ALL click events for analysis
            CASE
                WHEN name = 'SEARCH_RESULT_CLICK' THEN 'Result'
                WHEN name = 'SEARCH_TRENDING_CLICKED' THEN 'Trending'
                WHEN name = 'SEARCH_TAB_CLICK' THEN 'Tab'
                WHEN name = 'SEARCH_ALL_TAB_PAGE_CLICK' THEN 'Pagination_All'
                WHEN name = 'SEARCH_NEWS_TAB_PAGE_CLICK' THEN 'Pagination_News'
                WHEN name = 'SEARCH_GOTO_TAB_PAGE_CLICK' THEN 'Pagination_GoTo'
                WHEN name = 'SEARCH_FILTER_CLICK' THEN 'Filter'
                ELSE NULL
            END as click_category,
            -- Success click: TRUE only for actual result clicks (content found)
            -- Note: SEARCH_TRENDING_CLICKED is NOT a success - it's a search initiation via suggestion
            CASE
                WHEN name = 'SEARCH_RESULT_CLICK' THEN true
                ELSE false
            END as is_success_click
        FROM searches_raw r
    """)

    # Now update the window function columns
    con.execute("""
        CREATE OR REPLACE TABLE searches AS
        SELECT
            s.* EXCLUDE (event_order, prev_event, prev_timestamp, ms_since_prev_event, sec_since_prev_event, time_since_prev_bucket),
            ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY timestamp) as event_order,
            LAG(name) OVER (PARTITION BY session_key ORDER BY timestamp) as prev_event,
            LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp) as prev_timestamp,
            DATEDIFF('millisecond',
                LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp),
                timestamp
            ) as ms_since_prev_event,
            ROUND(
                DATEDIFF('millisecond',
                    LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp),
                    timestamp
                ) / 1000.0,
            3) as sec_since_prev_event,
            CASE
                WHEN LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp) IS NULL THEN 'First Event'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 500 THEN '< 0.5s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 1000 THEN '0.5-1s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 2000 THEN '1-2s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 5000 THEN '2-5s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 10000 THEN '5-10s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 30000 THEN '10-30s'
                WHEN DATEDIFF('millisecond', LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp), timestamp) < 60000 THEN '30-60s'
                ELSE '> 60s'
            END as time_since_prev_bucket,
            -- Carry forward the most recent SEARCH_TRIGGERED timestamp for timing calculation
            LAST_VALUE(CASE WHEN name = 'SEARCH_TRIGGERED' THEN timestamp END IGNORE NULLS)
                OVER (PARTITION BY session_key ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_search_started_ts
        FROM searches s
    """)

    # Add is_first_search_of_day
    con.execute("""
        CREATE OR REPLACE TABLE searches AS
        SELECT
            s.*,
            CASE
                WHEN name = 'SEARCH_TRIGGERED' AND
                     ROW_NUMBER() OVER (PARTITION BY user_id, session_date ORDER BY timestamp) = 1
                THEN true
                WHEN name = 'SEARCH_TRIGGERED'
                THEN false
                ELSE NULL
            END as is_first_search_of_day
        FROM searches s
    """)

    row_count = con.execute("SELECT COUNT(*) as n FROM searches").df()['n'][0]
    log(f"  Calculated columns added for {row_count:,} rows")

    # Verify CET timezone conversion
    cet_sample = con.execute("""
        SELECT
            timestamp as utc_timestamp,
            timestamp_cet as cet_timestamp,
            EXTRACT(HOUR FROM timestamp) as utc_hour,
            event_hour as cet_hour,
            session_date
        FROM searches
        ORDER BY timestamp
        LIMIT 3
    """).df()

    if len(cet_sample) > 0:
        log("  CET timezone conversion verification:")
        for _, row in cet_sample.iterrows():
            utc_ts = str(row['utc_timestamp'])[:23]
            cet_ts = str(row['cet_timestamp'])[:23]
            log(f"    UTC: {utc_ts} (hour {int(row['utc_hour']):02d}) → CET: {cet_ts} (hour {int(row['cet_hour']):02d}) | session_date: {row['session_date']}")


def export_parquet_files(con, output_dir):
    """Export all Parquet files for Power BI."""
    log("Exporting Parquet files...")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Raw data export
    raw_file = output_dir / 'searches_raw.parquet'
    if raw_file.exists():
        raw_file.unlink()
    con.execute(f"COPY searches TO '{raw_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    raw_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{raw_file}')").df()['n'][0]
    raw_size = os.path.getsize(raw_file) / (1024 * 1024)
    log(f"  searches_raw.parquet ({raw_count:,} rows, {raw_size:.1f} MB)")

    # Daily aggregation
    daily_file = output_dir / 'searches_daily.parquet'
    if daily_file.exists():
        daily_file.unlink()
    con.execute(f"""
        COPY (
            WITH session_stats AS (
                -- Pre-calculate session-level flags for accurate daily aggregation
                SELECT
                    session_key,
                    session_date,
                    MAX(CASE WHEN is_clickable_result = true THEN 1 ELSE 0 END) as had_results,
                    MAX(CASE WHEN is_success_click = true THEN 1 ELSE 0 END) as had_clicks
                FROM searches
                GROUP BY session_key, session_date
            ),
            daily_session_metrics AS (
                SELECT
                    session_date,
                    COUNT(*) as total_sessions,
                    SUM(had_results) as sessions_with_results,
                    SUM(CASE WHEN had_results = 1 AND had_clicks = 1 THEN 1 ELSE 0 END) as sessions_with_clicks,
                    SUM(CASE WHEN had_results = 1 AND had_clicks = 0 THEN 1 ELSE 0 END) as sessions_abandoned
                FROM session_stats
                GROUP BY session_date
            ),
            user_first_seen AS (
                -- Find the first date each user appeared
                SELECT user_id, MIN(session_date) as first_seen_date
                FROM searches
                GROUP BY user_id
            ),
            daily_user_cohorts AS (
                -- Count new vs returning users per day
                SELECT
                    s.session_date,
                    COUNT(DISTINCT CASE WHEN s.session_date = u.first_seen_date THEN s.user_id END) as new_users,
                    COUNT(DISTINCT CASE WHEN s.session_date > u.first_seen_date THEN s.user_id END) as returning_users
                FROM searches s
                JOIN user_first_seen u ON s.user_id = u.user_id
                GROUP BY s.session_date
            )
            SELECT
                s.session_date as date,
                COUNT(*) as total_events,
                COUNT(DISTINCT s.session_key) as unique_sessions,
                COUNT(DISTINCT s.user_id) as unique_users,
                COUNT(DISTINCT s.search_term_normalized) as unique_search_terms,
                COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' THEN 1 END) as search_starts,
                COUNT(CASE WHEN s.name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_events,
                COUNT(CASE WHEN s.click_category IS NOT NULL THEN 1 END) as click_events,
                COUNT(CASE WHEN s.is_success_click = true THEN 1 END) as success_clicks,
                SUM(CASE WHEN s.is_null_result = true THEN 1 ELSE 0 END) as null_results,
                SUM(CASE WHEN s.is_clickable_result = true THEN 1 ELSE 0 END) as result_events_with_results,
                -- Session-based metrics for accurate rate calculations
                MAX(d.sessions_with_results) as sessions_with_results,
                MAX(d.sessions_with_clicks) as sessions_with_clicks,
                MAX(d.sessions_abandoned) as sessions_abandoned,
                -- Rate metrics (success = actual result clicks, not navigation/filter clicks)
                ROUND(100.0 * COUNT(CASE WHEN s.is_success_click = true THEN 1 END)
                    / NULLIF(COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' THEN 1 END), 0), 2) as click_rate_pct,
                ROUND(100.0 * SUM(CASE WHEN s.is_null_result = true THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(CASE WHEN s.name = 'SEARCH_RESULT_COUNT' THEN 1 END), 0), 2) as null_rate_pct,
                -- Session-based rates (always 0-100%)
                ROUND(100.0 * MAX(d.sessions_with_clicks)
                    / NULLIF(MAX(d.sessions_with_results), 0), 2) as session_success_rate_pct,
                ROUND(100.0 * MAX(d.sessions_abandoned)
                    / NULLIF(MAX(d.sessions_with_results), 0), 2) as session_abandonment_rate_pct,
                -- Session metrics
                ROUND(1.0 * COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' THEN 1 END)
                    / NULLIF(COUNT(DISTINCT s.session_key), 0), 2) as avg_searches_per_session,
                -- Search term metrics (includes SUM columns for weighted DAX calculations)
                ROUND(AVG(s.search_term_length), 1) as avg_search_term_length,
                ROUND(AVG(s.search_term_word_count), 1) as avg_search_term_words,
                SUM(s.search_term_length) as sum_search_term_length,
                SUM(s.search_term_word_count) as sum_search_term_words,
                COUNT(CASE WHEN s.search_term_length IS NOT NULL THEN 1 END) as search_term_count,
                COUNT(CASE WHEN s.is_first_search_of_day = true THEN 1 END) as first_searches_of_day,
                -- Click category breakdown (Result/Trending = success, others = navigation/refinement)
                COUNT(CASE WHEN s.click_category = 'Result' THEN 1 END) as clicks_result,
                COUNT(CASE WHEN s.click_category = 'Trending' THEN 1 END) as clicks_trending,
                COUNT(CASE WHEN s.click_category = 'Tab' THEN 1 END) as clicks_tab,
                COUNT(CASE WHEN s.click_category LIKE 'Pagination%' THEN 1 END) as clicks_pagination,
                COUNT(CASE WHEN s.click_category = 'Pagination_All' THEN 1 END) as clicks_pagination_all,
                COUNT(CASE WHEN s.click_category = 'Pagination_News' THEN 1 END) as clicks_pagination_news,
                COUNT(CASE WHEN s.click_category = 'Pagination_GoTo' THEN 1 END) as clicks_pagination_goto,
                COUNT(CASE WHEN s.click_category = 'Filter' THEN 1 END) as clicks_filter,
                -- Temporal patterns
                DAYNAME(s.session_date) as day_of_week,
                ISODOW(s.session_date) as day_of_week_num,
                -- Hour distribution (searches by time of day, CET-based, regional alignment)
                COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 0 AND s.event_hour < 8 THEN 1 END) as searches_night,       -- 0-8 CET (APAC peak)
                COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 8 AND s.event_hour < 12 THEN 1 END) as searches_morning,    -- 8-12 CET (EMEA peak)
                COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 12 AND s.event_hour < 18 THEN 1 END) as searches_afternoon, -- 12-18 CET (EMEA+Americas overlap)
                COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 18 AND s.event_hour < 24 THEN 1 END) as searches_evening,   -- 18-24 CET (Americas peak)
                -- User cohort metrics
                MAX(uc.new_users) as new_users,
                MAX(uc.returning_users) as returning_users
            FROM searches s
            JOIN daily_session_metrics d ON s.session_date = d.session_date
            JOIN daily_user_cohorts uc ON s.session_date = uc.session_date
            GROUP BY 1
            ORDER BY 1
        ) TO '{daily_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    daily_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{daily_file}')").df()['n'][0]
    log(f"  searches_daily.parquet ({daily_count} days)")

    # Session journeys (consolidated - includes timing metrics)
    journeys_file = output_dir / 'searches_journeys.parquet'
    if journeys_file.exists():
        journeys_file.unlink()
    con.execute(f"""
        COPY (
            WITH session_data AS (
                SELECT
                    session_key,
                    session_date,
                    user_id,
                    MIN(timestamp) as session_start,
                    MIN(timestamp_cet) as session_start_cet,
                    COUNT(*) as total_events,
                    -- Timing metrics (SEARCH_TRIGGERED to SEARCH_RESULT_COUNT = full user-perceived latency)
                    MIN(CASE WHEN name = 'SEARCH_RESULT_COUNT' AND last_search_started_ts IS NOT NULL
                        THEN DATEDIFF('millisecond', last_search_started_ts, timestamp) END) as ms_search_to_result,
                    MIN(CASE WHEN is_success_click = true AND prev_event = 'SEARCH_RESULT_COUNT' THEN ms_since_prev_event END) as ms_result_to_click,
                    DATEDIFF('millisecond', MIN(timestamp), MAX(timestamp)) as total_duration_ms,
                    -- Event counts
                    COUNT(CASE WHEN name = 'SEARCH_TRIGGERED' THEN 1 END) as search_count_in_session,
                    COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_count,
                    COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as click_count,
                    COUNT(CASE WHEN is_success_click = true THEN 1 END) as success_click_count,
                    COUNT(DISTINCT search_term_normalized) as unique_search_terms,
                    SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_result_count,
                    -- Result metrics
                    MAX(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN CAST(CP_totalResultCount AS INTEGER) END) as max_total_results,
                    -- Time of day
                    MIN(event_hour) as first_event_hour,
                    MAX(event_hour) as last_event_hour,
                    -- Click breakdown (Result/Trending = success, others = navigation)
                    COUNT(CASE WHEN click_category = 'Result' THEN 1 END) as result_clicks,
                    COUNT(CASE WHEN click_category = 'Trending' THEN 1 END) as trending_clicks,
                    COUNT(CASE WHEN click_category = 'Tab' THEN 1 END) as tab_clicks,
                    COUNT(CASE WHEN click_category LIKE 'Pagination%' THEN 1 END) as pagination_clicks,
                    COUNT(CASE WHEN click_category = 'Pagination_All' THEN 1 END) as pagination_all_clicks,
                    COUNT(CASE WHEN click_category = 'Pagination_News' THEN 1 END) as pagination_news_clicks,
                    COUNT(CASE WHEN click_category = 'Pagination_GoTo' THEN 1 END) as pagination_goto_clicks,
                    COUNT(CASE WHEN click_category = 'Filter' THEN 1 END) as filter_clicks,
                    MAX(CASE WHEN is_first_search_of_day = true THEN 1 ELSE 0 END) as includes_first_search_of_day,
                    -- Session flow: distinct click categories used
                    COUNT(DISTINCT click_category) as distinct_click_categories
                FROM searches
                GROUP BY session_key, session_date, user_id
            ),
            session_with_user_rank AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_start) as user_session_number
                FROM session_data
            )
            SELECT
                session_date,
                session_start_cet,
                STRFTIME(session_start_cet, '%Y-%m-%d %H:%M:%S.%g') as session_start_str,
                total_events,
                search_count_in_session,
                result_count,
                click_count,
                unique_search_terms,
                null_result_count,
                max_total_results,
                -- Timing in seconds
                ROUND(ms_search_to_result / 1000.0, 2) as sec_search_to_result,
                ROUND(ms_result_to_click / 1000.0, 2) as sec_result_to_click,
                ROUND(total_duration_ms / 1000.0, 2) as total_duration_sec,
                -- Time of day
                first_event_hour,
                last_event_hour,
                -- Click breakdown (Result/Trending = success clicks, others = navigation)
                result_clicks,
                trending_clicks,
                tab_clicks,
                pagination_clicks,
                pagination_all_clicks,
                pagination_news_clicks,
                pagination_goto_clicks,
                filter_clicks,
                success_click_count,
                CASE WHEN includes_first_search_of_day = 1 THEN true ELSE false END as includes_first_search_of_day,
                -- Time buckets
                CASE
                    WHEN ms_search_to_result IS NULL THEN 'No Result'
                    WHEN ms_search_to_result < 500 THEN '< 0.5s'
                    WHEN ms_search_to_result < 1000 THEN '0.5-1s'
                    WHEN ms_search_to_result < 2000 THEN '1-2s'
                    WHEN ms_search_to_result < 5000 THEN '2-5s'
                    ELSE '> 5s'
                END as search_to_result_bucket,
                CASE
                    WHEN ms_result_to_click IS NULL THEN 'No Click'
                    WHEN ms_result_to_click < 2000 THEN '< 2s (quick)'
                    WHEN ms_result_to_click < 5000 THEN '2-5s'
                    WHEN ms_result_to_click < 10000 THEN '5-10s'
                    WHEN ms_result_to_click < 30000 THEN '10-30s'
                    WHEN ms_result_to_click < 60000 THEN '30-60s'
                    ELSE '> 60s (browsing)'
                END as result_to_click_bucket,
                CASE
                    WHEN total_duration_ms < 5000 THEN '< 5s (quick)'
                    WHEN total_duration_ms < 30000 THEN '5-30s'
                    WHEN total_duration_ms < 60000 THEN '30-60s'
                    WHEN total_duration_ms < 180000 THEN '1-3 min'
                    WHEN total_duration_ms < 300000 THEN '3-5 min'
                    ELSE '> 5 min (extended)'
                END as session_duration_bucket,
                -- Classifications (success = actual result click, not navigation/filter clicks)
                -- Engaged = user interacted (tabs/pagination/filters) but didn't click actual content
                CASE
                    WHEN success_click_count > 0 THEN 'Success'
                    WHEN click_count > 0 AND success_click_count = 0 THEN 'Engaged'
                    WHEN result_count > 0 AND null_result_count = result_count THEN 'No Results'
                    WHEN result_count > 0 AND click_count = 0 THEN 'Abandoned'
                    ELSE 'Unknown'
                END as journey_outcome,
                CASE WHEN unique_search_terms > 1 THEN true ELSE false END as had_reformulation,
                -- Session complexity based on USER ACTIONS (searches + clicks), not all telemetry events
                -- This reflects actual user engagement, not backend event noise
                CASE
                    WHEN (search_count_in_session + click_count) = 1 THEN 'Single Action'
                    WHEN (search_count_in_session + click_count) <= 3 THEN 'Simple'
                    WHEN (search_count_in_session + click_count) <= 10 THEN 'Medium'
                    ELSE 'Complex'
                END as session_complexity,
                -- Sort order columns for Power BI
                CASE
                    WHEN ms_search_to_result IS NULL THEN 6
                    WHEN ms_search_to_result < 500 THEN 1
                    WHEN ms_search_to_result < 1000 THEN 2
                    WHEN ms_search_to_result < 2000 THEN 3
                    WHEN ms_search_to_result < 5000 THEN 4
                    ELSE 5
                END as search_to_result_sort,
                CASE
                    WHEN ms_result_to_click IS NULL THEN 7
                    WHEN ms_result_to_click < 2000 THEN 1
                    WHEN ms_result_to_click < 5000 THEN 2
                    WHEN ms_result_to_click < 10000 THEN 3
                    WHEN ms_result_to_click < 30000 THEN 4
                    WHEN ms_result_to_click < 60000 THEN 5
                    ELSE 6
                END as result_to_click_sort,
                CASE
                    WHEN total_duration_ms < 5000 THEN 1
                    WHEN total_duration_ms < 30000 THEN 2
                    WHEN total_duration_ms < 60000 THEN 3
                    WHEN total_duration_ms < 180000 THEN 4
                    WHEN total_duration_ms < 300000 THEN 5
                    ELSE 6
                END as session_duration_sort,
                CASE
                    WHEN success_click_count > 0 THEN 1
                    WHEN click_count > 0 AND success_click_count = 0 THEN 2
                    WHEN result_count > 0 AND null_result_count = result_count THEN 4
                    WHEN result_count > 0 AND click_count = 0 THEN 3
                    ELSE 5
                END as journey_outcome_sort,
                CASE
                    WHEN (search_count_in_session + click_count) = 1 THEN 1
                    WHEN (search_count_in_session + click_count) <= 3 THEN 2
                    WHEN (search_count_in_session + click_count) <= 10 THEN 3
                    ELSE 4
                END as session_complexity_sort,
                -- Null result recovery analysis
                CASE WHEN null_result_count > 0 THEN true ELSE false END as had_null_result,
                CASE WHEN null_result_count > 0 AND success_click_count > 0 THEN true ELSE false END as recovered_from_null,
                -- User cohort analysis
                user_session_number,
                CASE WHEN user_session_number = 1 THEN true ELSE false END as is_users_first_session,
                -- Session flow analysis
                distinct_click_categories,
                CASE WHEN distinct_click_categories > 1 THEN true ELSE false END as had_tab_switch
            FROM session_with_user_rank
            ORDER BY session_date, session_start
        ) TO '{journeys_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    journeys_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{journeys_file}')").df()['n'][0]
    log(f"  searches_journeys.parquet ({journeys_count:,} sessions)")

    # Search terms analysis (aggregated by date + term)
    terms_file = output_dir / 'searches_terms.parquet'
    if terms_file.exists():
        terms_file.unlink()
    con.execute(f"""
        COPY (
            WITH search_terms_with_context AS (
                -- Propagate search term forward to subsequent events (clicks, results)
                SELECT
                    session_date,
                    session_key,
                    user_id,
                    name,
                    is_null_result,
                    cp_total_result_count,
                    click_category,
                    is_success_click,
                    search_term_normalized,
                    prev_event,
                    ms_since_prev_event,
                    event_hour,
                    -- Forward-fill search term to clicks and result events
                    LAST_VALUE(search_term_normalized IGNORE NULLS) OVER (
                        PARTITION BY session_key
                        ORDER BY timestamp
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) as active_search_term
                FROM searches
                WHERE name = 'SEARCH_TRIGGERED'
                   OR name = 'SEARCH_RESULT_COUNT'
                   OR click_category IS NOT NULL
            ),
            term_first_seen AS (
                -- Find when each search term first appeared (for trend detection)
                SELECT
                    search_term_normalized,
                    MIN(session_date) as first_seen_date
                FROM searches
                WHERE search_term_normalized IS NOT NULL AND search_term_normalized != ''
                GROUP BY search_term_normalized
            ),
            term_aggregates AS (
                SELECT
                    session_date,
                    active_search_term as search_term,
                    -- Word count for query length analysis
                    CASE
                        WHEN active_search_term IS NULL OR active_search_term = '' THEN 0
                        ELSE LENGTH(active_search_term) - LENGTH(REPLACE(active_search_term, ' ', '')) + 1
                    END as word_count,
                    -- Volume metrics
                    COUNT(CASE WHEN name = 'SEARCH_TRIGGERED' THEN 1 END) as search_count,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT session_key) as unique_sessions,
                    -- Result metrics
                    COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_events,
                    SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_result_count,
                    SUM(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN COALESCE(cp_total_result_count, 0) ELSE 0 END) as sum_result_count,
                    -- Click metrics (clicks attributed to this search term)
                    COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as click_count,
                    COUNT(CASE WHEN is_success_click = true THEN 1 END) as success_click_count,
                    COUNT(CASE WHEN click_category = 'Result' THEN 1 END) as clicks_result,
                    COUNT(CASE WHEN click_category = 'Trending' THEN 1 END) as clicks_trending,
                    COUNT(CASE WHEN click_category = 'Tab' THEN 1 END) as clicks_tab,
                    COUNT(CASE WHEN click_category LIKE 'Pagination%' THEN 1 END) as clicks_pagination,
                    COUNT(CASE WHEN click_category = 'Pagination_All' THEN 1 END) as clicks_pagination_all,
                    COUNT(CASE WHEN click_category = 'Pagination_News' THEN 1 END) as clicks_pagination_news,
                    COUNT(CASE WHEN click_category = 'Pagination_GoTo' THEN 1 END) as clicks_pagination_goto,
                    COUNT(CASE WHEN click_category = 'Filter' THEN 1 END) as clicks_filter,
                    -- Timing metrics (result to success click time for this term)
                    ROUND(AVG(CASE
                        WHEN is_success_click = true AND prev_event = 'SEARCH_RESULT_COUNT'
                        THEN ms_since_prev_event / 1000.0
                    END), 2) as avg_sec_to_click,
                    COUNT(CASE
                        WHEN is_success_click = true AND prev_event = 'SEARCH_RESULT_COUNT'
                        THEN 1
                    END) as clicks_with_timing,
                    SUM(CASE
                        WHEN is_success_click = true AND prev_event = 'SEARCH_RESULT_COUNT'
                        THEN ms_since_prev_event / 1000.0
                        ELSE 0
                    END) as sum_sec_to_click,
                    -- Hour distribution (when is this term searched? CET-based, regional alignment)
                    COUNT(CASE WHEN name = 'SEARCH_TRIGGERED' AND event_hour >= 0 AND event_hour < 8 THEN 1 END) as searches_night,       -- 0-8 CET (APAC peak)
                    COUNT(CASE WHEN name = 'SEARCH_TRIGGERED' AND event_hour >= 8 AND event_hour < 12 THEN 1 END) as searches_morning,    -- 8-12 CET (EMEA peak)
                    COUNT(CASE WHEN name = 'SEARCH_TRIGGERED' AND event_hour >= 12 AND event_hour < 18 THEN 1 END) as searches_afternoon, -- 12-18 CET (EMEA+Americas overlap)
                    COUNT(CASE WHEN name = 'SEARCH_TRIGGERED' AND event_hour >= 18 AND event_hour < 24 THEN 1 END) as searches_evening,   -- 18-24 CET (Americas peak)
                    -- Trend detection columns
                    MAX(tfs.first_seen_date) as first_seen_date,
                    CASE WHEN stc.session_date = MAX(tfs.first_seen_date) THEN true ELSE false END as is_new_term,
                    -- Seasonality (for monthly pattern analysis)
                    EXTRACT(MONTH FROM stc.session_date)::INTEGER as month_num
                FROM search_terms_with_context stc
                LEFT JOIN term_first_seen tfs ON stc.active_search_term = tfs.search_term_normalized
                WHERE stc.active_search_term IS NOT NULL
                  AND stc.active_search_term != ''
                GROUP BY stc.session_date, stc.active_search_term
            )
            SELECT t.*
            FROM term_aggregates t
            ORDER BY t.session_date, t.search_count DESC
        ) TO '{terms_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    terms_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{terms_file}')").df()['n'][0]
    log(f"  searches_terms.parquet ({terms_count:,} term-day combinations)")


def print_summary(con):
    """Print processing summary."""
    row_count = con.execute("SELECT COUNT(*) as n FROM searches").df()['n'][0]

    # Date range
    date_range = con.execute("""
        SELECT
            MIN(session_date) as first_date,
            MAX(session_date) as last_date,
            COUNT(DISTINCT session_date) as days
        FROM searches
    """).df()

    log("="*60)
    log("SUMMARY")
    log("="*60)
    log(f"Total rows: {row_count:,}")

    if len(date_range) > 0 and date_range['first_date'][0] is not None:
        log(f"Date range: {date_range['first_date'][0]} to {date_range['last_date'][0]} ({date_range['days'][0]} days)")

    # Journey outcomes (success = actual result click, not navigation/filter clicks)
    outcomes = con.execute("""
        WITH session_summary AS (
            SELECT
                session_key,
                COUNT(CASE WHEN is_success_click = true THEN 1 END) as success_clicks,
                SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_results,
                COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as results
            FROM searches
            GROUP BY session_key
        )
        SELECT
            CASE
                WHEN success_clicks > 0 THEN 'Success'
                WHEN results > 0 AND null_results = results AND success_clicks = 0 THEN 'No Results'
                WHEN results > 0 AND success_clicks = 0 THEN 'Abandoned'
                ELSE 'Unknown'
            END as outcome,
            COUNT(*) as sessions,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
        FROM session_summary
        GROUP BY 1
        ORDER BY 2 DESC
    """).df()

    log("\nJourney Outcomes:")
    for _, row in outcomes.iterrows():
        log(f"  {row['outcome']:12} {row['sessions']:>8,} ({row['pct']}%)")


def process_search_analytics(input_file=None, full_refresh=False):
    """
    Main processing function.

    Args:
        input_file: Specific file to process, or None to auto-detect
        full_refresh: If True, delete DB and reprocess all files
    """
    # Determine paths
    script_dir = Path(__file__).parent
    input_dir = script_dir / 'input'
    data_dir = script_dir / 'data'
    output_dir = script_dir / 'output'
    db_path = data_dir / 'searchanalytics.db'

    # Create directories
    input_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    log("="*60)
    log("SEARCH ANALYTICS PROCESSING")
    log("="*60)

    # Handle full refresh
    if full_refresh:
        if db_path.exists():
            db_path.unlink()
            log("Full refresh: deleted existing database")

        # Process all files
        files_to_process = get_all_input_files(input_dir)
        if not files_to_process:
            log(f"ERROR: No input files found in {input_dir}")
            log("Place your KQL export files (xlsx/csv) in the input/ folder")
            sys.exit(1)
        log(f"Full refresh: processing {len(files_to_process)} files")
    elif input_file:
        # Process specific file
        files_to_process = [Path(input_file)]
        if not files_to_process[0].exists():
            log(f"ERROR: File not found: {input_file}")
            sys.exit(1)
    else:
        # Auto-detect latest file
        latest_file = find_latest_input_file(input_dir)
        if not latest_file:
            log(f"ERROR: No input files found in {input_dir}")
            log("Place your KQL export files (xlsx/csv) in the input/ folder")
            log("Supported formats: .xlsx, .xls, .csv")
            log("\nFilename format: filename_YYYY_MM_DD.xlsx")
            log("Example filenames:")
            log("  search_export_2025_01_13.xlsx")
            log("  search_export_2025_01_13.csv")
            sys.exit(1)
        files_to_process = [latest_file]
        log(f"Auto-detected latest file: {latest_file.name}")

    # Connect to DuckDB
    con = duckdb.connect(str(db_path))

    # Process each file
    for input_path in files_to_process:
        log(f"\nProcessing: {input_path.name}")

        # Load file into temp table
        row_count = load_file_to_temp_table(con, input_path)
        log(f"  Loaded {row_count:,} rows")

        # Upsert into main table
        upsert_data(con)

    # Add calculated columns
    add_calculated_columns(con)

    # Export Parquet files
    export_parquet_files(con, output_dir)

    # Print summary
    print_summary(con)

    log(f"\nDatabase: {db_path}")
    log(f"Parquet files: {output_dir}")

    # Close connection
    con.close()
    log("\nDone!")


if __name__ == "__main__":
    # Parse arguments
    full_refresh = '--full-refresh' in sys.argv

    # Get input file if specified
    input_file = None
    for arg in sys.argv[1:]:
        if not arg.startswith('--'):
            input_file = arg
            break

    if len(sys.argv) == 1:
        # No arguments - show help and auto-detect
        print(__doc__)
        print("\nNo arguments provided - auto-detecting latest file in input/\n")

    process_search_analytics(input_file=input_file, full_refresh=full_refresh)
