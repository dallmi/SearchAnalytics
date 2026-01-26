-- =============================================================================
-- 4_create_searches.sql
-- =============================================================================
-- Purpose: Transform raw_events into enriched searches table
-- Usage:   Run after raw_events has been loaded/updated
--
-- Transformations performed:
--   1. Normalize event names to UPPERCASE
--   2. Create session identifiers (session_key)
--   3. Calculate window functions (prev_event, time intervals)
--   4. Add business logic columns (is_null_result, click_category)
--   5. Carry forward last_search_started_ts for timing calculations
--   6. Calculate is_first_search_of_day
--
-- Performance note:
--   For large datasets, this uses a TRUNCATE + INSERT pattern
--   Alternatively, use incremental processing (see bottom of file)
-- =============================================================================

-- =============================================================================
-- Full Refresh: Rebuild entire searches table
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_refresh_searches()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_row_count INTEGER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting searches table refresh...';

    -- Step 1: Create base transformations in temp table
    DROP TABLE IF EXISTS temp_searches_base;

    CREATE TEMP TABLE temp_searches_base AS
    SELECT
        -- Original columns
        r.timestamp,
        UPPER(r.name) as name,  -- Normalize to uppercase
        r.user_id,
        r.session_id,
        r.search_query,
        r.cp_search_query,
        r.query,

        -- Parse result count to integer
        CASE
            WHEN r.cp_total_result_count ~ '^[0-9]+$'
            THEN r.cp_total_result_count::INTEGER
            ELSE NULL
        END as cp_total_result_count,

        r.cp_tab,
        CASE
            WHEN r.cp_result_position ~ '^[0-9]+$'
            THEN r.cp_result_position::INTEGER
            ELSE NULL
        END as cp_result_position,
        r.cp_clicked_url,

        -- Timestamp string for Power BI (preserves precision)
        TO_CHAR(r.timestamp, 'YYYY-MM-DD HH24:MI:SS.MS') as timestamp_str,
        -- CET timestamp (handles CET/CEST automatically via Europe/Berlin timezone)
        r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin' as timestamp_cet,
        TO_CHAR(r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD HH24:MI:SS.MS') as timestamp_cet_str,

        -- Session identification (CET-based)
        DATE(r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') as session_date,
        COALESCE(TO_CHAR(DATE(r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin'), 'YYYY-MM-DD'), '') || '_' ||
            COALESCE(r.user_id, '') || '_' ||
            COALESCE(r.session_id, '') as session_key,

        -- Search term normalization
        LOWER(TRIM(COALESCE(r.cp_search_query, r.search_query, r.query))) as search_term_normalized,

        -- Time extraction (CET-based)
        EXTRACT(HOUR FROM r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::INTEGER as event_hour,
        TO_CHAR(r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin', 'Day') as event_weekday,
        EXTRACT(ISODOW FROM r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::INTEGER as event_weekday_num,

        -- Click category based on event name
        CASE
            WHEN UPPER(r.name) = 'SEARCH_RESULT_CLICK' THEN 'Result'
            WHEN UPPER(r.name) = 'SEARCH_TRENDING_CLICKED' THEN 'Trending'
            WHEN UPPER(r.name) = 'SEARCH_TAB_CLICK' THEN 'Tab'
            WHEN UPPER(r.name) = 'SEARCH_ALL_TAB_PAGE_CLICK' THEN 'Pagination_All'
            WHEN UPPER(r.name) = 'SEARCH_NEWS_TAB_PAGE_CLICK' THEN 'Pagination_News'
            WHEN UPPER(r.name) = 'SEARCH_GOTO_TAB_PAGE_CLICK' THEN 'Pagination_GoTo'
            WHEN UPPER(r.name) = 'SEARCH_FILTER_CLICK' THEN 'Filter'
            ELSE NULL
        END as click_category,

        -- Success click: TRUE only for actual result clicks (content found)
        -- Note: SEARCH_TRENDING_CLICKED is NOT a success - it's a search initiation via suggestion
        CASE
            WHEN UPPER(r.name) = 'SEARCH_RESULT_CLICK' THEN true
            ELSE false
        END as is_success_click

    FROM raw_events r;

    -- Step 2: Add window function columns
    DROP TABLE IF EXISTS temp_searches_windowed;

    CREATE TEMP TABLE temp_searches_windowed AS
    SELECT
        b.*,

        -- Search term metrics
        CASE
            WHEN b.search_term_normalized IS NULL OR b.search_term_normalized = '' THEN 0
            ELSE LENGTH(b.search_term_normalized)
        END as search_term_length,

        CASE
            WHEN b.search_term_normalized IS NULL OR b.search_term_normalized = '' THEN 0
            ELSE LENGTH(b.search_term_normalized) - LENGTH(REPLACE(b.search_term_normalized, ' ', '')) + 1
        END as search_term_word_count,

        -- Flags based on result count
        CASE
            WHEN b.name = 'SEARCH_RESULT_COUNT' AND b.cp_total_result_count = 0 THEN true
            WHEN b.name = 'SEARCH_RESULT_COUNT' AND b.cp_total_result_count > 0 THEN false
            ELSE NULL
        END as is_null_result,

        CASE
            WHEN b.name = 'SEARCH_RESULT_COUNT' AND b.cp_total_result_count > 0 THEN true
            WHEN b.name = 'SEARCH_RESULT_COUNT' THEN false
            ELSE NULL
        END as is_clickable_result,

        -- Event ordering
        ROW_NUMBER() OVER (PARTITION BY b.session_key ORDER BY b.timestamp) as event_order,

        -- Previous event info (LAG)
        LAG(b.name) OVER (PARTITION BY b.session_key ORDER BY b.timestamp) as prev_event,
        LAG(b.timestamp) OVER (PARTITION BY b.session_key ORDER BY b.timestamp) as prev_timestamp,

        -- Carry forward last SEARCH_TRIGGERED timestamp
        -- PostgreSQL doesn't have IGNORE NULLS, so we use a subquery workaround
        (
            SELECT MAX(sub.timestamp)
            FROM temp_searches_base sub
            WHERE sub.session_key = b.session_key
              AND sub.timestamp <= b.timestamp
              AND UPPER(sub.name) = 'SEARCH_TRIGGERED'
        ) as last_search_started_ts

    FROM temp_searches_base b;

    -- Step 3: Add time calculations and buckets
    DROP TABLE IF EXISTS temp_searches_final;

    CREATE TEMP TABLE temp_searches_final AS
    SELECT
        w.*,

        -- Time since previous event (milliseconds)
        CASE
            WHEN w.prev_timestamp IS NOT NULL
            THEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000
            ELSE NULL
        END::BIGINT as ms_since_prev_event,

        -- Time since previous event (seconds)
        CASE
            WHEN w.prev_timestamp IS NOT NULL
            THEN ROUND(EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp))::NUMERIC, 3)
            ELSE NULL
        END as sec_since_prev_event,

        -- Time bucket
        CASE
            WHEN w.prev_timestamp IS NULL THEN 'First Event'
            WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 500 THEN '< 0.5s'
            WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 1000 THEN '0.5-1s'
            WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 2000 THEN '1-2s'
            WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 5000 THEN '2-5s'
            WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 10000 THEN '5-10s'
            WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 30000 THEN '10-30s'
            WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 60000 THEN '30-60s'
            ELSE '> 60s'
        END as time_since_prev_bucket

    FROM temp_searches_windowed w;

    -- Step 4: Add is_first_search_of_day
    TRUNCATE TABLE searches;

    INSERT INTO searches
    SELECT
        f.timestamp,
        f.name,
        f.user_id,
        f.session_id,
        f.search_query,
        f.cp_search_query,
        f.query,
        f.cp_total_result_count,
        f.cp_tab,
        f.cp_result_position,
        f.cp_clicked_url,
        f.timestamp_str,
        f.timestamp_cet,
        f.timestamp_cet_str,
        f.session_date,
        f.session_key,
        f.event_order,
        f.prev_event,
        f.prev_timestamp,
        f.ms_since_prev_event,
        f.sec_since_prev_event,
        f.time_since_prev_bucket,
        f.last_search_started_ts,
        f.search_term_normalized,
        f.search_term_length,
        f.search_term_word_count,
        f.event_hour,
        f.event_weekday,
        f.event_weekday_num,
        f.is_null_result,
        f.is_clickable_result,
        f.click_category,
        f.is_success_click,
        -- is_first_search_of_day
        CASE
            WHEN f.name = 'SEARCH_TRIGGERED' AND
                 ROW_NUMBER() OVER (PARTITION BY f.user_id, f.session_date ORDER BY f.timestamp) = 1
            THEN true
            WHEN f.name = 'SEARCH_TRIGGERED'
            THEN false
            ELSE NULL
        END as is_first_search_of_day
    FROM temp_searches_final f;

    -- Cleanup temp tables
    DROP TABLE IF EXISTS temp_searches_base;
    DROP TABLE IF EXISTS temp_searches_windowed;
    DROP TABLE IF EXISTS temp_searches_final;

    -- Get final count
    SELECT COUNT(*) INTO v_row_count FROM searches;

    RAISE NOTICE 'Searches table refresh complete: % rows in % seconds',
        v_row_count,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_refresh_searches IS 'Full refresh of searches table with all calculated columns and window functions.';

-- =============================================================================
-- Incremental Update: Process only new data (for daily runs)
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_update_searches_incremental(p_start_date DATE DEFAULT CURRENT_DATE - 1)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_row_count INTEGER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting incremental searches update from %...', p_start_date;

    -- Delete existing records for the date range
    DELETE FROM searches WHERE session_date >= p_start_date;

    -- Insert recalculated records
    -- (Uses same logic as full refresh but filtered by date)
    WITH base AS (
        SELECT
            r.timestamp,
            UPPER(r.name) as name,
            r.user_id,
            r.session_id,
            r.search_query,
            r.cp_search_query,
            r.query,
            CASE
                WHEN r.cp_total_result_count ~ '^[0-9]+$'
                THEN r.cp_total_result_count::INTEGER
                ELSE NULL
            END as cp_total_result_count,
            r.cp_tab,
            CASE
                WHEN r.cp_result_position ~ '^[0-9]+$'
                THEN r.cp_result_position::INTEGER
                ELSE NULL
            END as cp_result_position,
            r.cp_clicked_url,
            TO_CHAR(r.timestamp, 'YYYY-MM-DD HH24:MI:SS.MS') as timestamp_str,
            r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin' as timestamp_cet,
            TO_CHAR(r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin', 'YYYY-MM-DD HH24:MI:SS.MS') as timestamp_cet_str,
            DATE(r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') as session_date,
            COALESCE(TO_CHAR(DATE(r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin'), 'YYYY-MM-DD'), '') || '_' ||
                COALESCE(r.user_id, '') || '_' ||
                COALESCE(r.session_id, '') as session_key,
            LOWER(TRIM(COALESCE(r.cp_search_query, r.search_query, r.query))) as search_term_normalized,
            EXTRACT(HOUR FROM r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::INTEGER as event_hour,
            TO_CHAR(r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin', 'Day') as event_weekday,
            EXTRACT(ISODOW FROM r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::INTEGER as event_weekday_num,
            CASE
                WHEN UPPER(r.name) = 'SEARCH_RESULT_CLICK' THEN 'Result'
                WHEN UPPER(r.name) = 'SEARCH_TRENDING_CLICKED' THEN 'Trending'
                WHEN UPPER(r.name) = 'SEARCH_TAB_CLICK' THEN 'Tab'
                WHEN UPPER(r.name) = 'SEARCH_ALL_TAB_PAGE_CLICK' THEN 'Pagination_All'
                WHEN UPPER(r.name) = 'SEARCH_NEWS_TAB_PAGE_CLICK' THEN 'Pagination_News'
                WHEN UPPER(r.name) = 'SEARCH_GOTO_TAB_PAGE_CLICK' THEN 'Pagination_GoTo'
                WHEN UPPER(r.name) = 'SEARCH_FILTER_CLICK' THEN 'Filter'
                ELSE NULL
            END as click_category,
            CASE
                WHEN UPPER(r.name) = 'SEARCH_RESULT_CLICK' THEN true
                ELSE false
            END as is_success_click
        FROM raw_events r
        WHERE DATE(r.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') >= p_start_date
    ),
    windowed AS (
        SELECT
            b.*,
            CASE WHEN b.search_term_normalized IS NULL OR b.search_term_normalized = '' THEN 0
                 ELSE LENGTH(b.search_term_normalized) END as search_term_length,
            CASE WHEN b.search_term_normalized IS NULL OR b.search_term_normalized = '' THEN 0
                 ELSE LENGTH(b.search_term_normalized) - LENGTH(REPLACE(b.search_term_normalized, ' ', '')) + 1
            END as search_term_word_count,
            CASE WHEN b.name = 'SEARCH_RESULT_COUNT' AND b.cp_total_result_count = 0 THEN true
                 WHEN b.name = 'SEARCH_RESULT_COUNT' AND b.cp_total_result_count > 0 THEN false
                 ELSE NULL END as is_null_result,
            CASE WHEN b.name = 'SEARCH_RESULT_COUNT' AND b.cp_total_result_count > 0 THEN true
                 WHEN b.name = 'SEARCH_RESULT_COUNT' THEN false
                 ELSE NULL END as is_clickable_result,
            ROW_NUMBER() OVER (PARTITION BY b.session_key ORDER BY b.timestamp) as event_order,
            LAG(b.name) OVER (PARTITION BY b.session_key ORDER BY b.timestamp) as prev_event,
            LAG(b.timestamp) OVER (PARTITION BY b.session_key ORDER BY b.timestamp) as prev_timestamp
        FROM base b
    ),
    with_timing AS (
        SELECT
            w.*,
            (SELECT MAX(sub.timestamp)
             FROM raw_events sub
             WHERE COALESCE(TO_CHAR(DATE(sub.timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin'), 'YYYY-MM-DD'), '') || '_' ||
                   COALESCE(sub.user_id, '') || '_' ||
                   COALESCE(sub.session_id, '') = w.session_key
               AND sub.timestamp <= w.timestamp
               AND UPPER(sub.name) = 'SEARCH_TRIGGERED') as last_search_started_ts,
            CASE WHEN w.prev_timestamp IS NOT NULL
                 THEN (EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000)::BIGINT
                 ELSE NULL END as ms_since_prev_event,
            CASE WHEN w.prev_timestamp IS NOT NULL
                 THEN ROUND(EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp))::NUMERIC, 3)
                 ELSE NULL END as sec_since_prev_event,
            CASE
                WHEN w.prev_timestamp IS NULL THEN 'First Event'
                WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 500 THEN '< 0.5s'
                WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 1000 THEN '0.5-1s'
                WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 2000 THEN '1-2s'
                WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 5000 THEN '2-5s'
                WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 10000 THEN '5-10s'
                WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 30000 THEN '10-30s'
                WHEN EXTRACT(EPOCH FROM (w.timestamp - w.prev_timestamp)) * 1000 < 60000 THEN '30-60s'
                ELSE '> 60s'
            END as time_since_prev_bucket
        FROM windowed w
    )
    INSERT INTO searches
    SELECT
        t.timestamp, t.name, t.user_id, t.session_id,
        t.search_query, t.cp_search_query, t.query,
        t.cp_total_result_count, t.cp_tab, t.cp_result_position, t.cp_clicked_url,
        t.timestamp_str, t.timestamp_cet, t.timestamp_cet_str, t.session_date, t.session_key,
        t.event_order, t.prev_event, t.prev_timestamp,
        t.ms_since_prev_event, t.sec_since_prev_event, t.time_since_prev_bucket,
        t.last_search_started_ts,
        t.search_term_normalized, t.search_term_length, t.search_term_word_count,
        t.event_hour, t.event_weekday, t.event_weekday_num,
        t.is_null_result, t.is_clickable_result, t.click_category, t.is_success_click,
        CASE
            WHEN t.name = 'SEARCH_TRIGGERED' AND
                 ROW_NUMBER() OVER (PARTITION BY t.user_id, t.session_date ORDER BY t.timestamp) = 1
            THEN true
            WHEN t.name = 'SEARCH_TRIGGERED' THEN false
            ELSE NULL
        END as is_first_search_of_day
    FROM with_timing t;

    GET DIAGNOSTICS v_row_count = ROW_COUNT;

    RAISE NOTICE 'Incremental update complete: % rows updated in % seconds',
        v_row_count,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

-- =============================================================================
-- Usage
-- =============================================================================
/*
-- Full refresh (first time or monthly):
CALL sp_refresh_searches();

-- Incremental update (daily):
CALL sp_update_searches_incremental(CURRENT_DATE - 1);

-- Verify results:
SELECT session_date, COUNT(*) as events, COUNT(DISTINCT session_key) as sessions
FROM searches
GROUP BY session_date
ORDER BY session_date DESC
LIMIT 7;
*/
