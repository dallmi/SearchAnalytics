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
        con.execute(f"""
            CREATE TABLE {temp_table} AS
            SELECT * FROM st_read('{input_path}')
        """)
    else:
        con.execute(f"""
            CREATE TABLE {temp_table} AS
            SELECT * FROM read_csv('{input_path}', auto_detect=true)
        """)

    # Normalize column names
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    col_names = schema['column_name'].tolist()

    rename_map = {'user_Id': 'user_id', 'session_Id': 'session_id'}
    for old_name, new_name in rename_map.items():
        if old_name in col_names:
            con.execute(f"ALTER TABLE {temp_table} RENAME COLUMN {old_name} TO {new_name}")

    # Convert German date formats
    schema = con.execute(f"DESCRIBE {temp_table}").df()
    varchar_cols = schema[schema['column_type'] == 'VARCHAR']['column_name'].tolist()

    for col in varchar_cols:
        sample = con.execute(f"SELECT {col} FROM {temp_table} WHERE {col} IS NOT NULL LIMIT 1").df()
        if len(sample) > 0:
            val = str(sample.iloc[0, 0])
            if re.match(r'^\d{2}\.\d{2}\.\d{4}', val):
                try:
                    if re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}(:\d{2})?$', val):
                        fmt = '%d.%m.%Y %H:%M:%S' if val.count(':') == 2 else '%d.%m.%Y %H:%M'
                    else:
                        fmt = '%d.%m.%Y'

                    con.execute(f"ALTER TABLE {temp_table} ADD COLUMN {col}_temp TIMESTAMP")
                    con.execute(f"UPDATE {temp_table} SET {col}_temp = strptime({col}, '{fmt}')")
                    con.execute(f"ALTER TABLE {temp_table} DROP COLUMN {col}")
                    con.execute(f"ALTER TABLE {temp_table} RENAME COLUMN {col}_temp TO {col}")
                except Exception:
                    pass

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
            r.*,
            -- Session columns
            DATE_TRUNC('day', timestamp)::DATE as session_date,
            COALESCE(CAST(DATE_TRUNC('day', timestamp)::DATE AS VARCHAR), '') || '_' ||
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
            -- Time extraction
            EXTRACT(HOUR FROM timestamp)::INTEGER as event_hour,
            DAYNAME(timestamp) as event_weekday,
            ISODOW(timestamp) as event_weekday_num,
            -- Flags
            CASE
                WHEN name = 'SEARCH_RESULT_COUNT' AND CAST(CP_totalResultCount AS INTEGER) = 0 THEN true
                WHEN name = 'SEARCH_RESULT_COUNT' AND CAST(CP_totalResultCount AS INTEGER) > 0 THEN false
                ELSE NULL
            END as is_null_result,
            CASE
                WHEN name = 'SEARCH_TAB_CLICK' THEN 'General'
                WHEN name = 'SEARCH_ALL_TAB_PAGE_CLICK' THEN 'All'
                WHEN name = 'SEARCH_NEWS_TAB_PAGE_CLICK' THEN 'News'
                WHEN name = 'SEARCH_GOTO_TAB_PAGE_CLICK' THEN 'GoTo'
                WHEN name LIKE '%PEOPLE%' OR name LIKE '%people%' THEN 'People'
                ELSE NULL
            END as click_category
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
            END as time_since_prev_bucket
        FROM searches s
    """)

    # Add is_first_search_of_day
    con.execute("""
        CREATE OR REPLACE TABLE searches AS
        SELECT
            s.*,
            CASE
                WHEN name = 'SEARCH_STARTED' AND
                     ROW_NUMBER() OVER (PARTITION BY user_id, session_date ORDER BY timestamp) = 1
                THEN true
                WHEN name = 'SEARCH_STARTED'
                THEN false
                ELSE NULL
            END as is_first_search_of_day
        FROM searches s
    """)

    row_count = con.execute("SELECT COUNT(*) as n FROM searches").df()['n'][0]
    log(f"  Calculated columns added for {row_count:,} rows")


def export_parquet_files(con, output_dir):
    """Export all Parquet files for Power BI."""
    log("Exporting Parquet files...")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Raw data export
    raw_file = output_dir / 'searches_raw.parquet'
    if raw_file.exists():
        raw_file.unlink()
    con.execute(f"COPY searches TO '{raw_file}' (FORMAT PARQUET)")
    raw_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{raw_file}')").df()['n'][0]
    raw_size = os.path.getsize(raw_file) / (1024 * 1024)
    log(f"  searches_raw.parquet ({raw_count:,} rows, {raw_size:.1f} MB)")

    # Daily aggregation
    daily_file = output_dir / 'searches_daily.parquet'
    if daily_file.exists():
        daily_file.unlink()
    con.execute(f"""
        COPY (
            SELECT
                session_date as date,
                COUNT(*) as total_events,
                COUNT(DISTINCT session_key) as unique_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT search_term_normalized) as unique_queries,
                COUNT(CASE WHEN name = 'SEARCH_STARTED' THEN 1 END) as search_starts,
                COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_events,
                COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as click_events,
                SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_results,
                -- Rate metrics
                ROUND(100.0 * COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END)
                    / NULLIF(COUNT(CASE WHEN name = 'SEARCH_STARTED' THEN 1 END), 0), 2) as click_through_rate_pct,
                ROUND(100.0 * SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END), 0), 2) as null_rate_pct,
                ROUND(100.0 * (COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' AND is_null_result = false THEN 1 END) - COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END))
                    / NULLIF(COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' AND is_null_result = false THEN 1 END), 0), 2) as abandonment_rate_pct,
                -- Session metrics
                ROUND(1.0 * COUNT(CASE WHEN name = 'SEARCH_STARTED' THEN 1 END)
                    / NULLIF(COUNT(DISTINCT session_key), 0), 2) as avg_searches_per_session,
                -- Search term metrics
                ROUND(AVG(search_term_length), 1) as avg_search_term_length,
                ROUND(AVG(search_term_word_count), 1) as avg_search_term_words,
                COUNT(CASE WHEN is_first_search_of_day = true THEN 1 END) as first_searches_of_day,
                -- Click category breakdown
                COUNT(CASE WHEN click_category = 'General' THEN 1 END) as clicks_general,
                COUNT(CASE WHEN click_category = 'All' THEN 1 END) as clicks_all,
                COUNT(CASE WHEN click_category = 'News' THEN 1 END) as clicks_news,
                COUNT(CASE WHEN click_category = 'GoTo' THEN 1 END) as clicks_goto,
                COUNT(CASE WHEN click_category = 'People' THEN 1 END) as clicks_people
            FROM searches
            GROUP BY 1
            ORDER BY 1
        ) TO '{daily_file}' (FORMAT PARQUET)
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
                    MIN(timestamp) as session_start,
                    COUNT(*) as total_events,
                    -- Timing metrics
                    MIN(CASE WHEN name = 'SEARCH_RESULT_COUNT' AND prev_event = 'SEARCH_STARTED' THEN ms_since_prev_event END) as ms_search_to_result,
                    MIN(CASE WHEN click_category IS NOT NULL AND prev_event = 'SEARCH_RESULT_COUNT' THEN ms_since_prev_event END) as ms_result_to_click,
                    AVG(ms_since_prev_event) as avg_ms_between_events,
                    DATEDIFF('millisecond', MIN(timestamp), MAX(timestamp)) as total_duration_ms,
                    -- Event counts
                    COUNT(CASE WHEN name = 'SEARCH_STARTED' THEN 1 END) as search_count,
                    COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_count,
                    COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as click_count,
                    COUNT(DISTINCT search_term_normalized) as unique_queries,
                    SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_result_count,
                    -- Result metrics
                    AVG(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN CAST(CP_totalResultCount AS FLOAT) END) as avg_total_results,
                    MAX(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN CAST(CP_totalResultCount AS INTEGER) END) as max_total_results,
                    -- Search term metrics
                    ROUND(AVG(search_term_length), 1) as avg_search_term_length,
                    ROUND(AVG(search_term_word_count), 1) as avg_search_term_words,
                    -- Time of day
                    MIN(event_hour) as first_event_hour,
                    MAX(event_hour) as last_event_hour,
                    -- Click breakdown
                    COUNT(CASE WHEN click_category = 'General' THEN 1 END) as general_clicks,
                    COUNT(CASE WHEN click_category = 'All' THEN 1 END) as all_tab_clicks,
                    COUNT(CASE WHEN click_category = 'News' THEN 1 END) as news_clicks,
                    COUNT(CASE WHEN click_category = 'GoTo' THEN 1 END) as goto_clicks,
                    COUNT(CASE WHEN click_category = 'People' THEN 1 END) as people_clicks,
                    MAX(CASE WHEN is_first_search_of_day = true THEN 1 ELSE 0 END) as includes_first_search_of_day
                FROM searches
                GROUP BY session_key, session_date
            )
            SELECT
                session_date,
                session_start,
                total_events,
                search_count,
                result_count,
                click_count,
                unique_queries,
                null_result_count,
                ROUND(avg_total_results, 1) as avg_total_results,
                max_total_results,
                -- Timing in seconds
                ROUND(ms_search_to_result / 1000.0, 2) as sec_search_to_result,
                ROUND(ms_result_to_click / 1000.0, 2) as sec_result_to_click,
                ROUND(avg_ms_between_events / 1000.0, 2) as avg_sec_between_events,
                ROUND(total_duration_ms / 1000.0, 2) as total_duration_sec,
                -- Search term metrics
                avg_search_term_length,
                avg_search_term_words,
                -- Time of day
                first_event_hour,
                last_event_hour,
                -- Click breakdown
                general_clicks,
                all_tab_clicks,
                news_clicks,
                goto_clicks,
                people_clicks,
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
                -- Classifications
                CASE
                    WHEN click_count > 0 THEN 'Success'
                    WHEN null_result_count > 0 AND click_count = 0 THEN 'No Results'
                    WHEN result_count > 0 AND click_count = 0 THEN 'Abandoned'
                    ELSE 'Unknown'
                END as journey_outcome,
                CASE WHEN unique_queries > 1 THEN true ELSE false END as had_reformulation,
                CASE
                    WHEN total_events = 1 THEN 'Single Event'
                    WHEN total_events <= 3 THEN 'Simple'
                    WHEN total_events <= 10 THEN 'Medium'
                    ELSE 'Complex'
                END as session_complexity
            FROM session_data
            ORDER BY session_date, session_start
        ) TO '{journeys_file}' (FORMAT PARQUET)
    """)
    journeys_count = con.execute(f"SELECT COUNT(*) as n FROM read_parquet('{journeys_file}')").df()['n'][0]
    log(f"  searches_journeys.parquet ({journeys_count:,} sessions)")


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

    # Journey outcomes
    outcomes = con.execute("""
        WITH session_summary AS (
            SELECT
                session_key,
                COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as clicks,
                SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_results,
                COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as results
            FROM searches
            GROUP BY session_key
        )
        SELECT
            CASE
                WHEN clicks > 0 THEN 'Success'
                WHEN null_results > 0 AND clicks = 0 THEN 'No Results'
                WHEN results > 0 AND clicks = 0 THEN 'Abandoned'
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
