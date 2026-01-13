#!/usr/bin/env python3
"""
Search Analytics Weekly Processing Script

This script processes weekly search analytics data extracted via KQL.
It creates a DuckDB database with all calculated columns and exports
Parquet files for Power BI consumption.

Usage:
    python process_search_analytics.py <input_file>
    python process_search_analytics.py data/search_export.csv
    python process_search_analytics.py data/search_export.xlsx

Input:
    CSV or Excel file exported from KQL query

Output:
    - data/searchanalytics.db          (DuckDB database)
    - output/searches_raw.parquet      (all event-level data)
    - output/searches_daily.parquet    (aggregated by day)
    - output/searches_journeys.parquet (session-level journey data)
    - output/searches_journeys_timed.parquet (journeys with timing)
"""

import sys
import os
import re
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime


def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def process_search_analytics(input_file):
    """
    Process search analytics data and create DuckDB + Parquet outputs.

    Args:
        input_file: Path to CSV or Excel file with search data
    """
    input_path = Path(input_file)

    if not input_path.exists():
        log(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)

    log(f"Starting processing: {input_path.name}")

    # Determine output paths
    data_dir = input_path.parent
    output_dir = data_dir.parent / 'output'
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = data_dir / 'searchanalytics.db'

    # Delete old database to start fresh
    if db_path.exists():
        db_path.unlink()
        log(f"Removed old database: {db_path.name}")

    # Connect to DuckDB
    con = duckdb.connect(str(db_path))

    def query(sql):
        return con.execute(sql).df()

    def execute(sql):
        con.execute(sql)

    # =========================================================================
    # STEP 1: Import data
    # =========================================================================
    log("Step 1: Importing data...")

    if input_path.suffix.lower() in ['.xlsx', '.xls']:
        # Excel file
        execute(f"""
            CREATE TABLE searches AS
            SELECT * FROM st_read('{input_path}')
        """)
    else:
        # CSV file
        execute(f"""
            CREATE TABLE searches AS
            SELECT * FROM read_csv('{input_path}', auto_detect=true)
        """)

    row_count = query("SELECT COUNT(*) as n FROM searches")['n'][0]
    log(f"  Imported {row_count:,} rows")

    # =========================================================================
    # STEP 2: Normalize column names
    # =========================================================================
    log("Step 2: Normalizing column names...")

    schema = query("DESCRIBE searches")
    col_names = schema['column_name'].tolist()

    rename_map = {
        'user_Id': 'user_id',
        'session_Id': 'session_id'
    }

    for old_name, new_name in rename_map.items():
        if old_name in col_names:
            execute(f"ALTER TABLE searches RENAME COLUMN {old_name} TO {new_name}")
            log(f"  Renamed: {old_name} → {new_name}")

    # =========================================================================
    # STEP 3: Convert German date formats
    # =========================================================================
    log("Step 3: Checking for German date formats...")

    schema = query("DESCRIBE searches")
    varchar_cols = schema[schema['column_type'] == 'VARCHAR']['column_name'].tolist()

    for col in varchar_cols:
        sample = query(f"SELECT {col} FROM searches WHERE {col} IS NOT NULL LIMIT 1")
        if len(sample) > 0:
            val = str(sample.iloc[0, 0])
            if re.match(r'^\d{2}\.\d{2}\.\d{4}', val):
                try:
                    if re.match(r'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}(:\d{2})?$', val):
                        fmt = '%d.%m.%Y %H:%M:%S' if val.count(':') == 2 else '%d.%m.%Y %H:%M'
                    else:
                        fmt = '%d.%m.%Y'

                    execute(f"ALTER TABLE searches ADD COLUMN {col}_temp TIMESTAMP")
                    execute(f"UPDATE searches SET {col}_temp = strptime({col}, '{fmt}')")
                    execute(f"ALTER TABLE searches DROP COLUMN {col}")
                    execute(f"ALTER TABLE searches RENAME COLUMN {col}_temp TO {col}")
                    log(f"  Converted: {col}")
                except Exception as e:
                    log(f"  Warning: Could not convert {col}: {e}")

    # =========================================================================
    # STEP 4: Create session key
    # =========================================================================
    log("Step 4: Creating session key...")

    schema = query("DESCRIBE searches")
    col_names = schema['column_name'].tolist()

    has_user_id = 'user_id' in col_names
    has_session_id = 'session_id' in col_names
    has_timestamp = 'timestamp' in col_names

    if has_user_id and has_session_id and has_timestamp:
        execute("ALTER TABLE searches ADD COLUMN session_date DATE;")
        execute("UPDATE searches SET session_date = DATE_TRUNC('day', timestamp)::DATE;")
        execute("ALTER TABLE searches ADD COLUMN session_key VARCHAR;")
        execute("""
            UPDATE searches SET session_key =
                COALESCE(CAST(session_date AS VARCHAR), '') || '_' ||
                COALESCE(user_id, '') || '_' ||
                COALESCE(session_id, '');
        """)
        log("  Created: session_date, session_key")
    else:
        log(f"  Warning: Missing columns for session key")

    # =========================================================================
    # STEP 5: Calculate time intervals
    # =========================================================================
    log("Step 5: Calculating event time intervals...")

    schema = query("DESCRIBE searches")
    col_names = schema['column_name'].tolist()

    if 'session_key' in col_names and 'timestamp' in col_names:
        execute("ALTER TABLE searches ADD COLUMN event_order INTEGER;")
        execute("ALTER TABLE searches ADD COLUMN prev_event VARCHAR;")
        execute("ALTER TABLE searches ADD COLUMN prev_timestamp TIMESTAMP;")
        execute("ALTER TABLE searches ADD COLUMN ms_since_prev_event BIGINT;")
        execute("ALTER TABLE searches ADD COLUMN sec_since_prev_event DOUBLE;")

        execute("""
            CREATE OR REPLACE TABLE searches AS
            SELECT
                s.*,
                ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY timestamp) as event_order_new,
                LAG(name) OVER (PARTITION BY session_key ORDER BY timestamp) as prev_event_new,
                LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp) as prev_timestamp_new,
                DATEDIFF('millisecond',
                    LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp),
                    timestamp
                ) as ms_since_prev_event_new,
                ROUND(
                    DATEDIFF('millisecond',
                        LAG(timestamp) OVER (PARTITION BY session_key ORDER BY timestamp),
                        timestamp
                    ) / 1000.0,
                3) as sec_since_prev_event_new
            FROM searches s
        """)

        execute("ALTER TABLE searches DROP COLUMN event_order;")
        execute("ALTER TABLE searches DROP COLUMN prev_event;")
        execute("ALTER TABLE searches DROP COLUMN prev_timestamp;")
        execute("ALTER TABLE searches DROP COLUMN ms_since_prev_event;")
        execute("ALTER TABLE searches DROP COLUMN sec_since_prev_event;")
        execute("ALTER TABLE searches RENAME COLUMN event_order_new TO event_order;")
        execute("ALTER TABLE searches RENAME COLUMN prev_event_new TO prev_event;")
        execute("ALTER TABLE searches RENAME COLUMN prev_timestamp_new TO prev_timestamp;")
        execute("ALTER TABLE searches RENAME COLUMN ms_since_prev_event_new TO ms_since_prev_event;")
        execute("ALTER TABLE searches RENAME COLUMN sec_since_prev_event_new TO sec_since_prev_event;")

        execute("ALTER TABLE searches ADD COLUMN time_since_prev_bucket VARCHAR;")
        execute("""
            UPDATE searches SET time_since_prev_bucket = CASE
                WHEN ms_since_prev_event IS NULL THEN 'First Event'
                WHEN ms_since_prev_event < 500 THEN '< 0.5s'
                WHEN ms_since_prev_event < 1000 THEN '0.5-1s'
                WHEN ms_since_prev_event < 2000 THEN '1-2s'
                WHEN ms_since_prev_event < 5000 THEN '2-5s'
                WHEN ms_since_prev_event < 10000 THEN '5-10s'
                WHEN ms_since_prev_event < 30000 THEN '10-30s'
                WHEN ms_since_prev_event < 60000 THEN '30-60s'
                ELSE '> 60s'
            END;
        """)
        log("  Created: event_order, prev_event, prev_timestamp, ms_since_prev_event, sec_since_prev_event, time_since_prev_bucket")

    # =========================================================================
    # STEP 6: Add calculated analytics columns
    # =========================================================================
    log("Step 6: Adding calculated analytics columns...")

    schema = query("DESCRIBE searches")
    col_names = schema['column_name'].tolist()

    # Search term normalization
    execute("ALTER TABLE searches ADD COLUMN search_term_normalized VARCHAR;")
    execute("""
        UPDATE searches SET search_term_normalized =
            LOWER(TRIM(COALESCE(CP_searchQuery, searchQuery, query)));
    """)
    log("  Created: search_term_normalized")

    # Search term length and word count
    execute("ALTER TABLE searches ADD COLUMN search_term_length INTEGER;")
    execute("ALTER TABLE searches ADD COLUMN search_term_word_count INTEGER;")
    execute("""
        UPDATE searches SET
            search_term_length = LENGTH(search_term_normalized),
            search_term_word_count = CASE
                WHEN search_term_normalized IS NULL OR search_term_normalized = '' THEN 0
                ELSE LENGTH(search_term_normalized) - LENGTH(REPLACE(search_term_normalized, ' ', '')) + 1
            END;
    """)
    log("  Created: search_term_length, search_term_word_count")

    # Hour and weekday
    if 'timestamp' in col_names:
        execute("ALTER TABLE searches ADD COLUMN event_hour INTEGER;")
        execute("ALTER TABLE searches ADD COLUMN event_weekday VARCHAR;")
        execute("ALTER TABLE searches ADD COLUMN event_weekday_num INTEGER;")
        execute("""
            UPDATE searches SET
                event_hour = EXTRACT(HOUR FROM timestamp)::INTEGER,
                event_weekday = DAYNAME(timestamp),
                event_weekday_num = ISODOW(timestamp);
        """)
        log("  Created: event_hour, event_weekday, event_weekday_num")

    # Null result flag
    execute("ALTER TABLE searches ADD COLUMN is_null_result BOOLEAN;")
    execute("""
        UPDATE searches SET is_null_result = CASE
            WHEN name = 'SEARCH_RESULT_COUNT' AND CAST(CP_totalResultCount AS INTEGER) = 0 THEN true
            WHEN name = 'SEARCH_RESULT_COUNT' AND CAST(CP_totalResultCount AS INTEGER) > 0 THEN false
            ELSE NULL
        END;
    """)
    log("  Created: is_null_result")

    # Click category
    execute("ALTER TABLE searches ADD COLUMN click_category VARCHAR;")
    execute("""
        UPDATE searches SET click_category = CASE
            WHEN name = 'SEARCH_TAB_CLICK' THEN 'General'
            WHEN name = 'SEARCH_ALL_TAB_PAGE_CLICK' THEN 'All'
            WHEN name = 'SEARCH_NEWS_TAB_PAGE_CLICK' THEN 'News'
            WHEN name = 'SEARCH_GOTO_TAB_PAGE_CLICK' THEN 'GoTo'
            WHEN name LIKE '%PEOPLE%' OR name LIKE '%people%' THEN 'People'
            ELSE NULL
        END;
    """)
    log("  Created: click_category")

    # First search of day
    if 'user_id' in col_names and 'session_date' in col_names and 'timestamp' in col_names:
        execute("""
            CREATE OR REPLACE TABLE searches AS
            SELECT
                s.*,
                CASE
                    WHEN name IN ('SEARCH_TRIGGERED', 'SEARCH_STARTED') AND
                         ROW_NUMBER() OVER (
                             PARTITION BY user_id, session_date
                             ORDER BY timestamp
                         ) = 1
                    THEN true
                    WHEN name IN ('SEARCH_TRIGGERED', 'SEARCH_STARTED')
                    THEN false
                    ELSE NULL
                END as is_first_search_of_day
            FROM searches s
        """)
        log("  Created: is_first_search_of_day")

    # =========================================================================
    # STEP 7: Export Parquet files
    # =========================================================================
    log("Step 7: Exporting Parquet files...")

    # Raw data export
    raw_file = output_dir / 'searches_raw.parquet'
    if raw_file.exists():
        raw_file.unlink()
    execute(f"COPY searches TO '{raw_file}' (FORMAT PARQUET)")
    raw_count = query(f"SELECT COUNT(*) as n FROM read_parquet('{raw_file}')")['n'][0]
    raw_size = os.path.getsize(raw_file) / (1024 * 1024)
    log(f"  Exported: searches_raw.parquet ({raw_count:,} rows, {raw_size:.1f} MB)")

    # Daily aggregation
    daily_file = output_dir / 'searches_daily.parquet'
    if daily_file.exists():
        daily_file.unlink()
    execute(f"""
        COPY (
            SELECT
                session_date as date,
                COUNT(*) as total_events,
                COUNT(DISTINCT session_key) as unique_sessions,
                COUNT(DISTINCT search_term_normalized) as unique_queries,
                COUNT(CASE WHEN name IN ('SEARCH_TRIGGERED', 'SEARCH_STARTED') THEN 1 END) as search_starts,
                COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_events,
                COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as click_events,
                SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_results,
                ROUND(100.0 * SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END), 0), 2) as null_rate_pct,
                ROUND(AVG(search_term_length), 1) as avg_search_term_length,
                ROUND(AVG(search_term_word_count), 1) as avg_search_term_words,
                COUNT(CASE WHEN is_first_search_of_day = true THEN 1 END) as first_searches_of_day,
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
    daily_count = query(f"SELECT COUNT(*) as n FROM read_parquet('{daily_file}')")['n'][0]
    log(f"  Exported: searches_daily.parquet ({daily_count} days)")

    # Session journeys
    journeys_file = output_dir / 'searches_journeys.parquet'
    if journeys_file.exists():
        journeys_file.unlink()
    execute(f"""
        COPY (
            WITH session_events AS (
                SELECT
                    session_key,
                    session_date,
                    MIN(timestamp) as session_start,
                    MAX(timestamp) as session_end,
                    DATEDIFF('second', MIN(timestamp), MAX(timestamp)) as duration_seconds,
                    COUNT(*) as total_events,
                    COUNT(CASE WHEN name IN ('SEARCH_TRIGGERED', 'SEARCH_STARTED') THEN 1 END) as search_count,
                    COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_count,
                    COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as click_count,
                    COUNT(DISTINCT search_term_normalized) as unique_queries,
                    AVG(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN CAST(CP_totalResultCount AS FLOAT) END) as avg_total_results,
                    MAX(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN CAST(CP_totalResultCount AS INTEGER) END) as max_total_results,
                    SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_result_searches,
                    COUNT(CASE WHEN click_category = 'General' THEN 1 END) as general_clicks,
                    COUNT(CASE WHEN click_category = 'All' THEN 1 END) as all_tab_clicks,
                    COUNT(CASE WHEN click_category = 'News' THEN 1 END) as news_clicks,
                    COUNT(CASE WHEN click_category = 'GoTo' THEN 1 END) as goto_clicks,
                    COUNT(CASE WHEN click_category = 'People' THEN 1 END) as people_clicks,
                    ROUND(AVG(search_term_length), 1) as avg_search_term_length,
                    ROUND(AVG(search_term_word_count), 1) as avg_search_term_words,
                    MIN(event_hour) as first_event_hour,
                    MAX(event_hour) as last_event_hour,
                    MAX(CASE WHEN is_first_search_of_day = true THEN 1 ELSE 0 END) as includes_first_search_of_day
                FROM searches
                GROUP BY session_key, session_date
            )
            SELECT
                session_date,
                session_start,
                duration_seconds,
                total_events,
                search_count,
                result_count,
                click_count,
                unique_queries,
                ROUND(avg_total_results, 1) as avg_total_results,
                max_total_results,
                null_result_searches,
                general_clicks,
                all_tab_clicks,
                news_clicks,
                goto_clicks,
                people_clicks,
                avg_search_term_length,
                avg_search_term_words,
                first_event_hour,
                last_event_hour,
                CASE WHEN includes_first_search_of_day = 1 THEN true ELSE false END as includes_first_search_of_day,
                CASE
                    WHEN click_count > 0 THEN 'Success'
                    WHEN null_result_searches > 0 AND click_count = 0 THEN 'No Results'
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
            FROM session_events
            ORDER BY session_date, session_start
        ) TO '{journeys_file}' (FORMAT PARQUET)
    """)
    journeys_count = query(f"SELECT COUNT(*) as n FROM read_parquet('{journeys_file}')")['n'][0]
    log(f"  Exported: searches_journeys.parquet ({journeys_count:,} sessions)")

    # Session journeys with timing
    journeys_timed_file = output_dir / 'searches_journeys_timed.parquet'
    if journeys_timed_file.exists():
        journeys_timed_file.unlink()
    execute(f"""
        COPY (
            WITH session_timings AS (
                SELECT
                    session_key,
                    session_date,
                    MIN(timestamp) as session_start,
                    MAX(timestamp) as session_end,
                    COUNT(*) as total_events,
                    MIN(CASE
                        WHEN name = 'SEARCH_RESULT_COUNT' AND prev_event IN ('SEARCH_TRIGGERED', 'SEARCH_STARTED')
                        THEN ms_since_prev_event
                    END) as ms_search_to_result,
                    MIN(CASE
                        WHEN click_category IS NOT NULL AND prev_event = 'SEARCH_RESULT_COUNT'
                        THEN ms_since_prev_event
                    END) as ms_result_to_click,
                    AVG(ms_since_prev_event) as avg_ms_between_events,
                    DATEDIFF('millisecond', MIN(timestamp), MAX(timestamp)) as total_duration_ms,
                    COUNT(CASE WHEN name IN ('SEARCH_TRIGGERED', 'SEARCH_STARTED') THEN 1 END) as search_count,
                    COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_count,
                    COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as click_count,
                    COUNT(DISTINCT search_term_normalized) as unique_queries,
                    SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_result_count,
                    ROUND(AVG(search_term_length), 1) as avg_search_term_length,
                    ROUND(AVG(search_term_word_count), 1) as avg_search_term_words,
                    MIN(event_hour) as first_event_hour,
                    MAX(event_hour) as last_event_hour,
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
                ROUND(ms_search_to_result / 1000.0, 2) as sec_search_to_result,
                ROUND(ms_result_to_click / 1000.0, 2) as sec_result_to_click,
                ROUND(avg_ms_between_events / 1000.0, 2) as avg_sec_between_events,
                ROUND(total_duration_ms / 1000.0, 2) as total_duration_sec,
                avg_search_term_length,
                avg_search_term_words,
                first_event_hour,
                last_event_hour,
                general_clicks,
                all_tab_clicks,
                news_clicks,
                goto_clicks,
                people_clicks,
                CASE WHEN includes_first_search_of_day = 1 THEN true ELSE false END as includes_first_search_of_day,
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
                CASE
                    WHEN click_count > 0 THEN 'Success'
                    WHEN null_result_count > 0 AND click_count = 0 THEN 'No Results'
                    WHEN result_count > 0 AND click_count = 0 THEN 'Abandoned'
                    ELSE 'Unknown'
                END as journey_outcome,
                CASE WHEN unique_queries > 1 THEN true ELSE false END as had_reformulation
            FROM session_timings
            ORDER BY session_date, session_start
        ) TO '{journeys_timed_file}' (FORMAT PARQUET)
    """)
    journeys_timed_count = query(f"SELECT COUNT(*) as n FROM read_parquet('{journeys_timed_file}')")['n'][0]
    log(f"  Exported: searches_journeys_timed.parquet ({journeys_timed_count:,} sessions)")

    # =========================================================================
    # STEP 8: Summary
    # =========================================================================
    log("="*60)
    log("PROCESSING COMPLETE")
    log("="*60)

    # Show column summary
    schema = query("DESCRIBE searches")
    log(f"\nDuckDB database: {db_path}")
    log(f"Total columns: {len(schema)}")
    log(f"Total rows: {row_count:,}")

    # Show date range
    date_range = query("""
        SELECT
            MIN(session_date) as first_date,
            MAX(session_date) as last_date,
            COUNT(DISTINCT session_date) as days
        FROM searches
    """)
    if len(date_range) > 0:
        log(f"\nDate range: {date_range['first_date'][0]} to {date_range['last_date'][0]} ({date_range['days'][0]} days)")

    # Show journey outcomes
    outcomes = query("""
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
    """)

    log("\nJourney Outcomes:")
    for _, row in outcomes.iterrows():
        log(f"  {row['outcome']:12} {row['sessions']:>8,} ({row['pct']}%)")

    log(f"\nParquet files exported to: {output_dir}")
    log("  - searches_raw.parquet (all event-level data)")
    log("  - searches_daily.parquet (aggregated by day)")
    log("  - searches_journeys.parquet (session-level)")
    log("  - searches_journeys_timed.parquet (with timing intervals)")

    # Close connection
    con.close()
    log("\nDone!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nError: Please provide input file path")
        print("Example: python process_search_analytics.py data/search_export.csv")
        sys.exit(1)

    process_search_analytics(sys.argv[1])
