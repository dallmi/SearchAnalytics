-- =============================================================================
-- 5_create_searches_journeys.sql
-- =============================================================================
-- Purpose: Aggregate searches table to session-level journeys
-- Usage:   Run after sp_refresh_searches() or sp_update_searches_incremental()
--
-- This creates one row per session with:
--   - Event counts (searches, results, clicks)
--   - Timing metrics (search_to_result, result_to_click, total_duration)
--   - Journey classification (Success, Abandoned, No Results)
--   - Time buckets for analysis
--   - User cohort information
-- =============================================================================

-- =============================================================================
-- Full Refresh: Rebuild entire searches_journeys table
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_refresh_searches_journeys()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_row_count INTEGER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting searches_journeys table refresh...';

    TRUNCATE TABLE searches_journeys;

    INSERT INTO searches_journeys
    WITH session_data AS (
        SELECT
            session_key,
            session_date,
            user_id,
            MIN(timestamp) as session_start,
            COUNT(*) as total_events,

            -- Timing metrics (SEARCH_STARTED to SEARCH_RESULT_COUNT = full user-perceived latency)
            MIN(CASE
                WHEN name = 'SEARCH_RESULT_COUNT' AND last_search_started_ts IS NOT NULL
                THEN EXTRACT(EPOCH FROM (timestamp - last_search_started_ts)) * 1000
            END)::BIGINT as ms_search_to_result,

            -- Result to click timing
            MIN(CASE
                WHEN click_category IS NOT NULL AND prev_event = 'SEARCH_RESULT_COUNT'
                THEN ms_since_prev_event
            END) as ms_result_to_click,

            -- Total session duration
            (EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) * 1000)::BIGINT as total_duration_ms,

            -- Event counts
            COUNT(CASE WHEN name = 'SEARCH_STARTED' THEN 1 END) as search_count_in_session,
            COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_count,
            COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as click_count,
            COUNT(DISTINCT search_term_normalized) as unique_search_terms,
            SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_result_count,

            -- Result metrics
            MAX(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN cp_total_result_count END) as max_total_results,

            -- Time of day
            MIN(event_hour) as first_event_hour,
            MAX(event_hour) as last_event_hour,

            -- Click breakdown
            COUNT(CASE WHEN click_category = 'General' THEN 1 END) as general_clicks,
            COUNT(CASE WHEN click_category = 'All' THEN 1 END) as all_tab_clicks,
            COUNT(CASE WHEN click_category = 'News' THEN 1 END) as news_clicks,
            COUNT(CASE WHEN click_category = 'GoTo' THEN 1 END) as goto_clicks,
            COUNT(CASE WHEN click_category = 'People' THEN 1 END) as people_clicks,

            -- Flags
            MAX(CASE WHEN is_first_search_of_day = true THEN 1 ELSE 0 END) as includes_first_search_of_day,

            -- Session flow: distinct click categories used
            COUNT(DISTINCT click_category) as distinct_click_categories

        FROM searches
        GROUP BY session_key, session_date, user_id
    ),
    session_with_user_rank AS (
        SELECT
            sd.*,
            ROW_NUMBER() OVER (PARTITION BY sd.user_id ORDER BY sd.session_start) as user_session_number
        FROM session_data sd
    )
    SELECT
        -- Session identification
        s.session_key,
        s.session_date,
        s.user_id,
        s.session_start,
        TO_CHAR(s.session_start, 'YYYY-MM-DD HH24:MI:SS.MS') as session_start_str,

        -- Event counts
        s.total_events,
        s.search_count_in_session,
        s.result_count,
        s.click_count,
        s.unique_search_terms,
        s.null_result_count,
        s.max_total_results,

        -- Timing in seconds
        ROUND(s.ms_search_to_result / 1000.0, 2) as sec_search_to_result,
        ROUND(s.ms_result_to_click / 1000.0, 2) as sec_result_to_click,
        ROUND(s.total_duration_ms / 1000.0, 2) as total_duration_sec,

        -- Time of day
        s.first_event_hour,
        s.last_event_hour,

        -- Click breakdown
        s.general_clicks,
        s.all_tab_clicks,
        s.news_clicks,
        s.goto_clicks,
        s.people_clicks,

        -- Flags
        CASE WHEN s.includes_first_search_of_day = 1 THEN true ELSE false END as includes_first_search_of_day,
        CASE WHEN s.null_result_count > 0 THEN true ELSE false END as had_null_result,
        CASE WHEN s.null_result_count > 0 AND s.click_count > 0 THEN true ELSE false END as recovered_from_null,
        CASE WHEN s.unique_search_terms > 1 THEN true ELSE false END as had_reformulation,
        CASE WHEN s.distinct_click_categories > 1 THEN true ELSE false END as had_tab_switch,
        CASE WHEN s.user_session_number = 1 THEN true ELSE false END as is_users_first_session,

        -- Journey outcome classification
        CASE
            WHEN s.click_count > 0 THEN 'Success'
            WHEN s.result_count > 0 AND s.null_result_count = s.result_count AND s.click_count = 0 THEN 'No Results'
            WHEN s.result_count > 0 AND s.click_count = 0 THEN 'Abandoned'
            ELSE 'Unknown'
        END as journey_outcome,

        -- Session complexity
        CASE
            WHEN s.total_events = 1 THEN 'Single Event'
            WHEN s.total_events <= 3 THEN 'Simple'
            WHEN s.total_events <= 10 THEN 'Medium'
            ELSE 'Complex'
        END as session_complexity,

        -- Time buckets
        CASE
            WHEN s.ms_search_to_result IS NULL THEN 'No Result'
            WHEN s.ms_search_to_result < 500 THEN '< 0.5s'
            WHEN s.ms_search_to_result < 1000 THEN '0.5-1s'
            WHEN s.ms_search_to_result < 2000 THEN '1-2s'
            WHEN s.ms_search_to_result < 5000 THEN '2-5s'
            ELSE '> 5s'
        END as search_to_result_bucket,

        CASE
            WHEN s.ms_result_to_click IS NULL THEN 'No Click'
            WHEN s.ms_result_to_click < 2000 THEN '< 2s (quick)'
            WHEN s.ms_result_to_click < 5000 THEN '2-5s'
            WHEN s.ms_result_to_click < 10000 THEN '5-10s'
            WHEN s.ms_result_to_click < 30000 THEN '10-30s'
            WHEN s.ms_result_to_click < 60000 THEN '30-60s'
            ELSE '> 60s (browsing)'
        END as result_to_click_bucket,

        CASE
            WHEN s.total_duration_ms < 5000 THEN '< 5s (quick)'
            WHEN s.total_duration_ms < 30000 THEN '5-30s'
            WHEN s.total_duration_ms < 60000 THEN '30-60s'
            WHEN s.total_duration_ms < 180000 THEN '1-3 min'
            WHEN s.total_duration_ms < 300000 THEN '3-5 min'
            ELSE '> 5 min (extended)'
        END as session_duration_bucket,

        -- Sort order columns (for Power BI)
        CASE
            WHEN s.click_count > 0 THEN 1
            WHEN s.result_count > 0 AND s.null_result_count = s.result_count AND s.click_count = 0 THEN 3
            WHEN s.result_count > 0 AND s.click_count = 0 THEN 2
            ELSE 4
        END as journey_outcome_sort,

        CASE
            WHEN s.total_events = 1 THEN 1
            WHEN s.total_events <= 3 THEN 2
            WHEN s.total_events <= 10 THEN 3
            ELSE 4
        END as session_complexity_sort,

        CASE
            WHEN s.ms_search_to_result IS NULL THEN 6
            WHEN s.ms_search_to_result < 500 THEN 1
            WHEN s.ms_search_to_result < 1000 THEN 2
            WHEN s.ms_search_to_result < 2000 THEN 3
            WHEN s.ms_search_to_result < 5000 THEN 4
            ELSE 5
        END as search_to_result_sort,

        CASE
            WHEN s.ms_result_to_click IS NULL THEN 7
            WHEN s.ms_result_to_click < 2000 THEN 1
            WHEN s.ms_result_to_click < 5000 THEN 2
            WHEN s.ms_result_to_click < 10000 THEN 3
            WHEN s.ms_result_to_click < 30000 THEN 4
            WHEN s.ms_result_to_click < 60000 THEN 5
            ELSE 6
        END as result_to_click_sort,

        CASE
            WHEN s.total_duration_ms < 5000 THEN 1
            WHEN s.total_duration_ms < 30000 THEN 2
            WHEN s.total_duration_ms < 60000 THEN 3
            WHEN s.total_duration_ms < 180000 THEN 4
            WHEN s.total_duration_ms < 300000 THEN 5
            ELSE 6
        END as session_duration_sort,

        -- User cohort
        s.user_session_number,
        s.distinct_click_categories

    FROM session_with_user_rank s
    ORDER BY s.session_date, s.session_start;

    SELECT COUNT(*) INTO v_row_count FROM searches_journeys;

    RAISE NOTICE 'searches_journeys refresh complete: % sessions in % seconds',
        v_row_count,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_refresh_searches_journeys IS 'Full refresh of searches_journeys with session-level metrics and journey classification.';

-- =============================================================================
-- Incremental Update: Process only new data
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_update_searches_journeys_incremental(p_start_date DATE DEFAULT CURRENT_DATE - 1)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_deleted INTEGER;
    v_inserted INTEGER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting incremental searches_journeys update from %...', p_start_date;

    -- Delete existing records for the date range
    DELETE FROM searches_journeys WHERE session_date >= p_start_date;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- Insert recalculated records (same logic as full refresh, but filtered)
    INSERT INTO searches_journeys
    WITH session_data AS (
        SELECT
            session_key,
            session_date,
            user_id,
            MIN(timestamp) as session_start,
            COUNT(*) as total_events,
            MIN(CASE WHEN name = 'SEARCH_RESULT_COUNT' AND last_search_started_ts IS NOT NULL
                THEN EXTRACT(EPOCH FROM (timestamp - last_search_started_ts)) * 1000 END)::BIGINT as ms_search_to_result,
            MIN(CASE WHEN click_category IS NOT NULL AND prev_event = 'SEARCH_RESULT_COUNT'
                THEN ms_since_prev_event END) as ms_result_to_click,
            (EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) * 1000)::BIGINT as total_duration_ms,
            COUNT(CASE WHEN name = 'SEARCH_STARTED' THEN 1 END) as search_count_in_session,
            COUNT(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_count,
            COUNT(CASE WHEN click_category IS NOT NULL THEN 1 END) as click_count,
            COUNT(DISTINCT search_term_normalized) as unique_search_terms,
            SUM(CASE WHEN is_null_result = true THEN 1 ELSE 0 END) as null_result_count,
            MAX(CASE WHEN name = 'SEARCH_RESULT_COUNT' THEN cp_total_result_count END) as max_total_results,
            MIN(event_hour) as first_event_hour,
            MAX(event_hour) as last_event_hour,
            COUNT(CASE WHEN click_category = 'General' THEN 1 END) as general_clicks,
            COUNT(CASE WHEN click_category = 'All' THEN 1 END) as all_tab_clicks,
            COUNT(CASE WHEN click_category = 'News' THEN 1 END) as news_clicks,
            COUNT(CASE WHEN click_category = 'GoTo' THEN 1 END) as goto_clicks,
            COUNT(CASE WHEN click_category = 'People' THEN 1 END) as people_clicks,
            MAX(CASE WHEN is_first_search_of_day = true THEN 1 ELSE 0 END) as includes_first_search_of_day,
            COUNT(DISTINCT click_category) as distinct_click_categories
        FROM searches
        WHERE session_date >= p_start_date
        GROUP BY session_key, session_date, user_id
    ),
    session_with_user_rank AS (
        SELECT sd.*,
            ROW_NUMBER() OVER (PARTITION BY sd.user_id ORDER BY sd.session_start) as user_session_number
        FROM session_data sd
    )
    SELECT
        s.session_key, s.session_date, s.user_id, s.session_start,
        TO_CHAR(s.session_start, 'YYYY-MM-DD HH24:MI:SS.MS'),
        s.total_events, s.search_count_in_session, s.result_count, s.click_count,
        s.unique_search_terms, s.null_result_count, s.max_total_results,
        ROUND(s.ms_search_to_result / 1000.0, 2),
        ROUND(s.ms_result_to_click / 1000.0, 2),
        ROUND(s.total_duration_ms / 1000.0, 2),
        s.first_event_hour, s.last_event_hour,
        s.general_clicks, s.all_tab_clicks, s.news_clicks, s.goto_clicks, s.people_clicks,
        CASE WHEN s.includes_first_search_of_day = 1 THEN true ELSE false END,
        CASE WHEN s.null_result_count > 0 THEN true ELSE false END,
        CASE WHEN s.null_result_count > 0 AND s.click_count > 0 THEN true ELSE false END,
        CASE WHEN s.unique_search_terms > 1 THEN true ELSE false END,
        CASE WHEN s.distinct_click_categories > 1 THEN true ELSE false END,
        CASE WHEN s.user_session_number = 1 THEN true ELSE false END,
        CASE WHEN s.click_count > 0 THEN 'Success'
             WHEN s.result_count > 0 AND s.null_result_count = s.result_count THEN 'No Results'
             WHEN s.result_count > 0 AND s.click_count = 0 THEN 'Abandoned'
             ELSE 'Unknown' END,
        CASE WHEN s.total_events = 1 THEN 'Single Event'
             WHEN s.total_events <= 3 THEN 'Simple'
             WHEN s.total_events <= 10 THEN 'Medium'
             ELSE 'Complex' END,
        CASE WHEN s.ms_search_to_result IS NULL THEN 'No Result'
             WHEN s.ms_search_to_result < 500 THEN '< 0.5s'
             WHEN s.ms_search_to_result < 1000 THEN '0.5-1s'
             WHEN s.ms_search_to_result < 2000 THEN '1-2s'
             WHEN s.ms_search_to_result < 5000 THEN '2-5s'
             ELSE '> 5s' END,
        CASE WHEN s.ms_result_to_click IS NULL THEN 'No Click'
             WHEN s.ms_result_to_click < 2000 THEN '< 2s (quick)'
             WHEN s.ms_result_to_click < 5000 THEN '2-5s'
             WHEN s.ms_result_to_click < 10000 THEN '5-10s'
             WHEN s.ms_result_to_click < 30000 THEN '10-30s'
             WHEN s.ms_result_to_click < 60000 THEN '30-60s'
             ELSE '> 60s (browsing)' END,
        CASE WHEN s.total_duration_ms < 5000 THEN '< 5s (quick)'
             WHEN s.total_duration_ms < 30000 THEN '5-30s'
             WHEN s.total_duration_ms < 60000 THEN '30-60s'
             WHEN s.total_duration_ms < 180000 THEN '1-3 min'
             WHEN s.total_duration_ms < 300000 THEN '3-5 min'
             ELSE '> 5 min (extended)' END,
        CASE WHEN s.click_count > 0 THEN 1
             WHEN s.result_count > 0 AND s.null_result_count = s.result_count THEN 3
             WHEN s.result_count > 0 AND s.click_count = 0 THEN 2
             ELSE 4 END,
        CASE WHEN s.total_events = 1 THEN 1 WHEN s.total_events <= 3 THEN 2
             WHEN s.total_events <= 10 THEN 3 ELSE 4 END,
        CASE WHEN s.ms_search_to_result IS NULL THEN 6 WHEN s.ms_search_to_result < 500 THEN 1
             WHEN s.ms_search_to_result < 1000 THEN 2 WHEN s.ms_search_to_result < 2000 THEN 3
             WHEN s.ms_search_to_result < 5000 THEN 4 ELSE 5 END,
        CASE WHEN s.ms_result_to_click IS NULL THEN 7 WHEN s.ms_result_to_click < 2000 THEN 1
             WHEN s.ms_result_to_click < 5000 THEN 2 WHEN s.ms_result_to_click < 10000 THEN 3
             WHEN s.ms_result_to_click < 30000 THEN 4 WHEN s.ms_result_to_click < 60000 THEN 5
             ELSE 6 END,
        CASE WHEN s.total_duration_ms < 5000 THEN 1 WHEN s.total_duration_ms < 30000 THEN 2
             WHEN s.total_duration_ms < 60000 THEN 3 WHEN s.total_duration_ms < 180000 THEN 4
             WHEN s.total_duration_ms < 300000 THEN 5 ELSE 6 END,
        s.user_session_number,
        s.distinct_click_categories
    FROM session_with_user_rank s;

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    RAISE NOTICE 'Incremental update complete: deleted %, inserted % in % seconds',
        v_deleted, v_inserted,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

-- =============================================================================
-- Usage
-- =============================================================================
/*
-- Full refresh:
CALL sp_refresh_searches_journeys();

-- Incremental update (after daily data load):
CALL sp_update_searches_journeys_incremental(CURRENT_DATE - 1);

-- Verify journey outcomes:
SELECT journey_outcome, COUNT(*) as sessions,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) as pct
FROM searches_journeys
GROUP BY journey_outcome
ORDER BY sessions DESC;
*/
