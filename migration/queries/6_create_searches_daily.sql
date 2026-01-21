-- =============================================================================
-- 6_create_searches_daily.sql
-- =============================================================================
-- Purpose: Aggregate searches to daily KPIs
-- Usage:   Run after sp_refresh_searches() or sp_update_searches_incremental()
--
-- This creates one row per day with:
--   - Volume metrics (events, sessions, users, search terms)
--   - Event counts by type
--   - Rate calculations (click rate, null rate, success rate)
--   - Time distribution (morning, afternoon, evening, night)
--   - User cohort metrics (new vs returning)
-- =============================================================================

-- =============================================================================
-- Full Refresh: Rebuild entire searches_daily table
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_refresh_searches_daily()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_row_count INTEGER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting searches_daily table refresh...';

    TRUNCATE TABLE searches_daily;

    INSERT INTO searches_daily
    WITH session_stats AS (
        -- Pre-calculate session-level flags for accurate daily aggregation
        SELECT
            session_key,
            session_date,
            MAX(CASE WHEN is_clickable_result = true THEN 1 ELSE 0 END) as had_results,
            MAX(CASE WHEN click_category IS NOT NULL THEN 1 ELSE 0 END) as had_clicks
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
        -- Date
        s.session_date as date,

        -- Volume metrics
        COUNT(*) as total_events,
        COUNT(DISTINCT s.session_key) as unique_sessions,
        COUNT(DISTINCT s.user_id) as unique_users,
        COUNT(DISTINCT s.search_term_normalized) as unique_search_terms,

        -- Event counts
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' THEN 1 END) as search_starts,
        COUNT(CASE WHEN s.name = 'SEARCH_RESULT_COUNT' THEN 1 END) as result_events,
        COUNT(CASE WHEN s.click_category IS NOT NULL THEN 1 END) as click_events,
        SUM(CASE WHEN s.is_null_result = true THEN 1 ELSE 0 END)::INTEGER as null_results,
        SUM(CASE WHEN s.is_clickable_result = true THEN 1 ELSE 0 END)::INTEGER as result_events_with_results,

        -- Session-based metrics
        MAX(d.sessions_with_results)::INTEGER as sessions_with_results,
        MAX(d.sessions_with_clicks)::INTEGER as sessions_with_clicks,
        MAX(d.sessions_abandoned)::INTEGER as sessions_abandoned,

        -- Rate metrics (event-based)
        ROUND(100.0 * COUNT(CASE WHEN s.click_category IS NOT NULL THEN 1 END)
            / NULLIF(COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' THEN 1 END), 0), 2) as click_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN s.is_null_result = true THEN 1 ELSE 0 END)
            / NULLIF(COUNT(CASE WHEN s.name = 'SEARCH_RESULT_COUNT' THEN 1 END), 0), 2) as null_rate_pct,

        -- Session-based rates
        ROUND(100.0 * MAX(d.sessions_with_clicks)
            / NULLIF(MAX(d.sessions_with_results), 0), 2) as session_success_rate_pct,
        ROUND(100.0 * MAX(d.sessions_abandoned)
            / NULLIF(MAX(d.sessions_with_results), 0), 2) as session_abandonment_rate_pct,

        -- Averages
        ROUND(1.0 * COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' THEN 1 END)
            / NULLIF(COUNT(DISTINCT s.session_key), 0), 2) as avg_searches_per_session,
        ROUND(AVG(s.search_term_length)::NUMERIC, 1) as avg_search_term_length,
        ROUND(AVG(s.search_term_word_count)::NUMERIC, 1) as avg_search_term_words,

        -- For weighted DAX calculations in Power BI
        SUM(s.search_term_length)::INTEGER as sum_search_term_length,
        SUM(s.search_term_word_count)::INTEGER as sum_search_term_words,
        COUNT(CASE WHEN s.search_term_length IS NOT NULL THEN 1 END)::INTEGER as search_term_count,
        COUNT(CASE WHEN s.is_first_search_of_day = true THEN 1 END)::INTEGER as first_searches_of_day,

        -- Click breakdown by category
        COUNT(CASE WHEN s.click_category = 'General' THEN 1 END)::INTEGER as clicks_general,
        COUNT(CASE WHEN s.click_category = 'All' THEN 1 END)::INTEGER as clicks_all,
        COUNT(CASE WHEN s.click_category = 'News' THEN 1 END)::INTEGER as clicks_news,
        COUNT(CASE WHEN s.click_category = 'GoTo' THEN 1 END)::INTEGER as clicks_goto,
        COUNT(CASE WHEN s.click_category = 'People' THEN 1 END)::INTEGER as clicks_people,

        -- Temporal
        TRIM(TO_CHAR(s.session_date, 'Day')) as day_of_week,
        EXTRACT(ISODOW FROM s.session_date)::INTEGER as day_of_week_num,

        -- Time distribution (when are searches happening?)
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 6 AND s.event_hour < 12 THEN 1 END)::INTEGER as searches_morning,
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 12 AND s.event_hour < 18 THEN 1 END)::INTEGER as searches_afternoon,
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 18 AND s.event_hour < 24 THEN 1 END)::INTEGER as searches_evening,
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 0 AND s.event_hour < 6 THEN 1 END)::INTEGER as searches_night,

        -- User cohort metrics
        MAX(uc.new_users)::INTEGER as new_users,
        MAX(uc.returning_users)::INTEGER as returning_users

    FROM searches s
    JOIN daily_session_metrics d ON s.session_date = d.session_date
    JOIN daily_user_cohorts uc ON s.session_date = uc.session_date
    GROUP BY s.session_date
    ORDER BY s.session_date;

    SELECT COUNT(*) INTO v_row_count FROM searches_daily;

    RAISE NOTICE 'searches_daily refresh complete: % days in % seconds',
        v_row_count,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_refresh_searches_daily IS 'Full refresh of searches_daily with daily KPIs and metrics.';

-- =============================================================================
-- Incremental Update: Process only new data
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_update_searches_daily_incremental(p_start_date DATE DEFAULT CURRENT_DATE - 1)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_deleted INTEGER;
    v_inserted INTEGER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting incremental searches_daily update from %...', p_start_date;

    -- Delete existing records for the date range
    DELETE FROM searches_daily WHERE date >= p_start_date;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- Insert recalculated records
    INSERT INTO searches_daily
    WITH session_stats AS (
        SELECT session_key, session_date,
            MAX(CASE WHEN is_clickable_result = true THEN 1 ELSE 0 END) as had_results,
            MAX(CASE WHEN click_category IS NOT NULL THEN 1 ELSE 0 END) as had_clicks
        FROM searches
        WHERE session_date >= p_start_date
        GROUP BY session_key, session_date
    ),
    daily_session_metrics AS (
        SELECT session_date,
            COUNT(*) as total_sessions,
            SUM(had_results) as sessions_with_results,
            SUM(CASE WHEN had_results = 1 AND had_clicks = 1 THEN 1 ELSE 0 END) as sessions_with_clicks,
            SUM(CASE WHEN had_results = 1 AND had_clicks = 0 THEN 1 ELSE 0 END) as sessions_abandoned
        FROM session_stats
        GROUP BY session_date
    ),
    user_first_seen AS (
        SELECT user_id, MIN(session_date) as first_seen_date
        FROM searches
        GROUP BY user_id
    ),
    daily_user_cohorts AS (
        SELECT s.session_date,
            COUNT(DISTINCT CASE WHEN s.session_date = u.first_seen_date THEN s.user_id END) as new_users,
            COUNT(DISTINCT CASE WHEN s.session_date > u.first_seen_date THEN s.user_id END) as returning_users
        FROM searches s
        JOIN user_first_seen u ON s.user_id = u.user_id
        WHERE s.session_date >= p_start_date
        GROUP BY s.session_date
    )
    SELECT
        s.session_date,
        COUNT(*),
        COUNT(DISTINCT s.session_key),
        COUNT(DISTINCT s.user_id),
        COUNT(DISTINCT s.search_term_normalized),
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' THEN 1 END),
        COUNT(CASE WHEN s.name = 'SEARCH_RESULT_COUNT' THEN 1 END),
        COUNT(CASE WHEN s.click_category IS NOT NULL THEN 1 END),
        SUM(CASE WHEN s.is_null_result = true THEN 1 ELSE 0 END)::INTEGER,
        SUM(CASE WHEN s.is_clickable_result = true THEN 1 ELSE 0 END)::INTEGER,
        MAX(d.sessions_with_results)::INTEGER,
        MAX(d.sessions_with_clicks)::INTEGER,
        MAX(d.sessions_abandoned)::INTEGER,
        ROUND(100.0 * COUNT(CASE WHEN s.click_category IS NOT NULL THEN 1 END)
            / NULLIF(COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' THEN 1 END), 0), 2),
        ROUND(100.0 * SUM(CASE WHEN s.is_null_result = true THEN 1 ELSE 0 END)
            / NULLIF(COUNT(CASE WHEN s.name = 'SEARCH_RESULT_COUNT' THEN 1 END), 0), 2),
        ROUND(100.0 * MAX(d.sessions_with_clicks) / NULLIF(MAX(d.sessions_with_results), 0), 2),
        ROUND(100.0 * MAX(d.sessions_abandoned) / NULLIF(MAX(d.sessions_with_results), 0), 2),
        ROUND(1.0 * COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' THEN 1 END)
            / NULLIF(COUNT(DISTINCT s.session_key), 0), 2),
        ROUND(AVG(s.search_term_length)::NUMERIC, 1),
        ROUND(AVG(s.search_term_word_count)::NUMERIC, 1),
        SUM(s.search_term_length)::INTEGER,
        SUM(s.search_term_word_count)::INTEGER,
        COUNT(CASE WHEN s.search_term_length IS NOT NULL THEN 1 END)::INTEGER,
        COUNT(CASE WHEN s.is_first_search_of_day = true THEN 1 END)::INTEGER,
        COUNT(CASE WHEN s.click_category = 'General' THEN 1 END)::INTEGER,
        COUNT(CASE WHEN s.click_category = 'All' THEN 1 END)::INTEGER,
        COUNT(CASE WHEN s.click_category = 'News' THEN 1 END)::INTEGER,
        COUNT(CASE WHEN s.click_category = 'GoTo' THEN 1 END)::INTEGER,
        COUNT(CASE WHEN s.click_category = 'People' THEN 1 END)::INTEGER,
        TRIM(TO_CHAR(s.session_date, 'Day')),
        EXTRACT(ISODOW FROM s.session_date)::INTEGER,
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 6 AND s.event_hour < 12 THEN 1 END)::INTEGER,
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 12 AND s.event_hour < 18 THEN 1 END)::INTEGER,
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 18 AND s.event_hour < 24 THEN 1 END)::INTEGER,
        COUNT(CASE WHEN s.name = 'SEARCH_TRIGGERED' AND s.event_hour >= 0 AND s.event_hour < 6 THEN 1 END)::INTEGER,
        MAX(uc.new_users)::INTEGER,
        MAX(uc.returning_users)::INTEGER
    FROM searches s
    JOIN daily_session_metrics d ON s.session_date = d.session_date
    JOIN daily_user_cohorts uc ON s.session_date = uc.session_date
    WHERE s.session_date >= p_start_date
    GROUP BY s.session_date
    ORDER BY s.session_date;

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
CALL sp_refresh_searches_daily();

-- Incremental update:
CALL sp_update_searches_daily_incremental(CURRENT_DATE - 1);

-- Check daily trends:
SELECT date, search_starts, click_events, session_success_rate_pct, null_rate_pct
FROM searches_daily
ORDER BY date DESC
LIMIT 14;
*/
