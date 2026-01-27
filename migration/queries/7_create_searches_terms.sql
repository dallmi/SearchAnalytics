-- =============================================================================
-- 7_create_searches_terms.sql
-- =============================================================================
-- Purpose: Aggregate search term analytics
-- Usage:   Run after sp_refresh_searches() or sp_update_searches_incremental()
--
-- This creates one row per date + search term combination with:
--   - Volume metrics (searches, users, sessions)
--   - Result metrics (result events, null results)
--   - Click metrics with category breakdown
--   - Timing metrics (average time to click)
--   - Trend detection (first seen date, is new term)
--
-- Key feature: Click attribution to search terms using LAST_VALUE
-- =============================================================================

-- =============================================================================
-- Full Refresh: Rebuild entire searches_terms table
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_refresh_searches_terms()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_row_count INTEGER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting searches_terms table refresh...';

    TRUNCATE TABLE searches_terms;

    INSERT INTO searches_terms
    WITH search_terms_with_context AS (
        -- Forward-fill search term to subsequent events (clicks, results)
        -- This attributes clicks to the search that triggered them
        SELECT
            session_date,
            session_key,
            user_id,
            name,
            is_null_result,
            click_category,
            is_success_click,
            search_term_normalized,
            prev_event,
            ms_since_prev_event,
            event_hour,
            timestamp,
            -- Forward-fill: get the most recent search term for this row
            (
                SELECT sub.search_term_normalized
                FROM searches sub
                WHERE sub.session_key = s.session_key
                  AND sub.timestamp <= s.timestamp
                  AND sub.search_term_normalized IS NOT NULL
                  AND sub.search_term_normalized != ''
                ORDER BY sub.timestamp DESC
                LIMIT 1
            ) as active_search_term
        FROM searches s
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
        WHERE search_term_normalized IS NOT NULL
          AND search_term_normalized != ''
        GROUP BY search_term_normalized
    ),
    term_aggregates AS (
        SELECT
            stc.session_date,
            stc.active_search_term as search_term,

            -- Word count for query length analysis
            CASE
                WHEN stc.active_search_term IS NULL OR stc.active_search_term = '' THEN 0
                ELSE LENGTH(stc.active_search_term) - LENGTH(REPLACE(stc.active_search_term, ' ', '')) + 1
            END as word_count,

            -- Volume metrics
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' THEN 1 END)::INTEGER as search_count,
            COUNT(DISTINCT stc.user_id)::INTEGER as unique_users,
            COUNT(DISTINCT stc.session_key)::INTEGER as unique_sessions,

            -- Result metrics
            COUNT(CASE WHEN stc.name = 'SEARCH_RESULT_COUNT' THEN 1 END)::INTEGER as result_events,
            SUM(CASE WHEN stc.is_null_result = true THEN 1 ELSE 0 END)::INTEGER as null_result_count,

            -- Click metrics (clicks attributed to this search term)
            COUNT(CASE WHEN stc.click_category IS NOT NULL THEN 1 END)::INTEGER as click_count,
            COUNT(CASE WHEN stc.click_category = 'Result' THEN 1 END)::INTEGER as clicks_result,
            COUNT(CASE WHEN stc.click_category = 'Trending' THEN 1 END)::INTEGER as clicks_trending,
            COUNT(CASE WHEN stc.click_category = 'Tab' THEN 1 END)::INTEGER as clicks_tab,
            COUNT(CASE WHEN stc.click_category LIKE 'Pagination%' THEN 1 END)::INTEGER as clicks_pagination,
            COUNT(CASE WHEN stc.click_category = 'Pagination_All' THEN 1 END)::INTEGER as clicks_pagination_all,
            COUNT(CASE WHEN stc.click_category = 'Pagination_News' THEN 1 END)::INTEGER as clicks_pagination_news,
            COUNT(CASE WHEN stc.click_category = 'Pagination_GoTo' THEN 1 END)::INTEGER as clicks_pagination_goto,
            COUNT(CASE WHEN stc.click_category = 'Filter' THEN 1 END)::INTEGER as clicks_filter,
            COUNT(CASE WHEN stc.is_success_click = true THEN 1 END)::INTEGER as success_click_count,

            -- Timing metrics (result to success click time for this term)
            ROUND(AVG(CASE
                WHEN stc.is_success_click = true AND stc.prev_event = 'SEARCH_RESULT_COUNT'
                THEN stc.ms_since_prev_event / 1000.0
            END)::NUMERIC, 2) as avg_sec_to_click,

            COUNT(CASE
                WHEN stc.is_success_click = true AND stc.prev_event = 'SEARCH_RESULT_COUNT'
                THEN 1
            END)::INTEGER as clicks_with_timing,

            SUM(CASE
                WHEN stc.is_success_click = true AND stc.prev_event = 'SEARCH_RESULT_COUNT'
                THEN stc.ms_since_prev_event / 1000.0
                ELSE 0
            END)::NUMERIC(12,2) as sum_sec_to_click,

            -- Time distribution (CET-based hours, regional alignment)
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' AND stc.event_hour >= 0 AND stc.event_hour < 8 THEN 1 END)::INTEGER as searches_night,       -- 0-8 CET (APAC evening)
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' AND stc.event_hour >= 8 AND stc.event_hour < 12 THEN 1 END)::INTEGER as searches_morning,    -- 8-12 CET (EMEA morning)
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' AND stc.event_hour >= 12 AND stc.event_hour < 18 THEN 1 END)::INTEGER as searches_afternoon, -- 12-18 CET (EMEA/Americas overlap)
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' AND stc.event_hour >= 18 AND stc.event_hour < 24 THEN 1 END)::INTEGER as searches_evening,   -- 18-24 CET (Americas afternoon)

            -- Trend detection columns
            MAX(tfs.first_seen_date) as first_seen_date,
            CASE WHEN stc.session_date = MAX(tfs.first_seen_date) THEN true ELSE false END as is_new_term

        FROM search_terms_with_context stc
        LEFT JOIN term_first_seen tfs ON stc.active_search_term = tfs.search_term_normalized
        WHERE stc.active_search_term IS NOT NULL
          AND stc.active_search_term != ''
        GROUP BY stc.session_date, stc.active_search_term
    )
    SELECT
        t.session_date,
        t.search_term,
        t.word_count,
        t.search_count,
        t.unique_users,
        t.unique_sessions,
        t.result_events,
        t.null_result_count,
        t.click_count,
        t.clicks_result,
        t.clicks_trending,
        t.clicks_tab,
        t.clicks_pagination,
        t.clicks_pagination_all,
        t.clicks_pagination_news,
        t.clicks_pagination_goto,
        t.clicks_filter,
        t.success_click_count,
        t.avg_sec_to_click,
        t.clicks_with_timing,
        t.sum_sec_to_click,
        t.searches_morning,
        t.searches_afternoon,
        t.searches_evening,
        t.searches_night,
        t.first_seen_date,
        t.is_new_term
    FROM term_aggregates t
    ORDER BY t.session_date, t.search_count DESC;

    SELECT COUNT(*) INTO v_row_count FROM searches_terms;

    RAISE NOTICE 'searches_terms refresh complete: % term-day combinations in % seconds',
        v_row_count,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_refresh_searches_terms IS 'Full refresh of searches_terms with search term analytics and click attribution.';

-- =============================================================================
-- Incremental Update: Process only new data
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_update_searches_terms_incremental(p_start_date DATE DEFAULT CURRENT_DATE - 1)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_deleted INTEGER;
    v_inserted INTEGER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting incremental searches_terms update from %...', p_start_date;

    -- Delete existing records for the date range
    DELETE FROM searches_terms WHERE session_date >= p_start_date;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    -- Insert recalculated records
    INSERT INTO searches_terms
    WITH search_terms_with_context AS (
        SELECT
            session_date, session_key, user_id, name,
            is_null_result, click_category, is_success_click, search_term_normalized,
            prev_event, ms_since_prev_event, event_hour, timestamp,
            (
                SELECT sub.search_term_normalized
                FROM searches sub
                WHERE sub.session_key = s.session_key
                  AND sub.timestamp <= s.timestamp
                  AND sub.search_term_normalized IS NOT NULL
                  AND sub.search_term_normalized != ''
                ORDER BY sub.timestamp DESC
                LIMIT 1
            ) as active_search_term
        FROM searches s
        WHERE session_date >= p_start_date
          AND (name = 'SEARCH_TRIGGERED' OR name = 'SEARCH_RESULT_COUNT' OR click_category IS NOT NULL)
    ),
    term_first_seen AS (
        SELECT search_term_normalized, MIN(session_date) as first_seen_date
        FROM searches
        WHERE search_term_normalized IS NOT NULL AND search_term_normalized != ''
        GROUP BY search_term_normalized
    ),
    term_aggregates AS (
        SELECT
            stc.session_date,
            stc.active_search_term as search_term,
            CASE WHEN stc.active_search_term IS NULL OR stc.active_search_term = '' THEN 0
                 ELSE LENGTH(stc.active_search_term) - LENGTH(REPLACE(stc.active_search_term, ' ', '')) + 1 END as word_count,
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' THEN 1 END)::INTEGER as search_count,
            COUNT(DISTINCT stc.user_id)::INTEGER as unique_users,
            COUNT(DISTINCT stc.session_key)::INTEGER as unique_sessions,
            COUNT(CASE WHEN stc.name = 'SEARCH_RESULT_COUNT' THEN 1 END)::INTEGER as result_events,
            SUM(CASE WHEN stc.is_null_result = true THEN 1 ELSE 0 END)::INTEGER as null_result_count,
            COUNT(CASE WHEN stc.click_category IS NOT NULL THEN 1 END)::INTEGER as click_count,
            COUNT(CASE WHEN stc.click_category = 'Result' THEN 1 END)::INTEGER as clicks_result,
            COUNT(CASE WHEN stc.click_category = 'Trending' THEN 1 END)::INTEGER as clicks_trending,
            COUNT(CASE WHEN stc.click_category = 'Tab' THEN 1 END)::INTEGER as clicks_tab,
            COUNT(CASE WHEN stc.click_category LIKE 'Pagination%' THEN 1 END)::INTEGER as clicks_pagination,
            COUNT(CASE WHEN stc.click_category = 'Pagination_All' THEN 1 END)::INTEGER as clicks_pagination_all,
            COUNT(CASE WHEN stc.click_category = 'Pagination_News' THEN 1 END)::INTEGER as clicks_pagination_news,
            COUNT(CASE WHEN stc.click_category = 'Pagination_GoTo' THEN 1 END)::INTEGER as clicks_pagination_goto,
            COUNT(CASE WHEN stc.click_category = 'Filter' THEN 1 END)::INTEGER as clicks_filter,
            COUNT(CASE WHEN stc.is_success_click = true THEN 1 END)::INTEGER as success_click_count,
            ROUND(AVG(CASE WHEN stc.is_success_click = true AND stc.prev_event = 'SEARCH_RESULT_COUNT'
                THEN stc.ms_since_prev_event / 1000.0 END)::NUMERIC, 2) as avg_sec_to_click,
            COUNT(CASE WHEN stc.is_success_click = true AND stc.prev_event = 'SEARCH_RESULT_COUNT' THEN 1 END)::INTEGER as clicks_with_timing,
            SUM(CASE WHEN stc.is_success_click = true AND stc.prev_event = 'SEARCH_RESULT_COUNT'
                THEN stc.ms_since_prev_event / 1000.0 ELSE 0 END)::NUMERIC(12,2) as sum_sec_to_click,
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' AND stc.event_hour >= 0 AND stc.event_hour < 8 THEN 1 END)::INTEGER as searches_night,
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' AND stc.event_hour >= 8 AND stc.event_hour < 12 THEN 1 END)::INTEGER as searches_morning,
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' AND stc.event_hour >= 12 AND stc.event_hour < 18 THEN 1 END)::INTEGER as searches_afternoon,
            COUNT(CASE WHEN stc.name = 'SEARCH_TRIGGERED' AND stc.event_hour >= 18 AND stc.event_hour < 24 THEN 1 END)::INTEGER as searches_evening,
            MAX(tfs.first_seen_date) as first_seen_date,
            CASE WHEN stc.session_date = MAX(tfs.first_seen_date) THEN true ELSE false END as is_new_term
        FROM search_terms_with_context stc
        LEFT JOIN term_first_seen tfs ON stc.active_search_term = tfs.search_term_normalized
        WHERE stc.active_search_term IS NOT NULL AND stc.active_search_term != ''
        GROUP BY stc.session_date, stc.active_search_term
    )
    SELECT
        t.session_date,
        t.search_term,
        t.word_count,
        t.search_count,
        t.unique_users,
        t.unique_sessions,
        t.result_events,
        t.null_result_count,
        t.click_count,
        t.clicks_result,
        t.clicks_trending,
        t.clicks_tab,
        t.clicks_pagination,
        t.clicks_pagination_all,
        t.clicks_pagination_news,
        t.clicks_pagination_goto,
        t.clicks_filter,
        t.success_click_count,
        t.avg_sec_to_click,
        t.clicks_with_timing,
        t.sum_sec_to_click,
        t.searches_morning,
        t.searches_afternoon,
        t.searches_evening,
        t.searches_night,
        t.first_seen_date,
        t.is_new_term
    FROM term_aggregates t
    ORDER BY t.session_date, t.search_count DESC;

    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    RAISE NOTICE 'Incremental update complete: deleted %, inserted % in % seconds',
        v_deleted, v_inserted,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

-- =============================================================================
-- Utility Views for Search Term Analysis
-- =============================================================================

-- Top search terms (all time)
CREATE OR REPLACE VIEW v_top_search_terms AS
SELECT
    search_term,
    SUM(search_count) as total_searches,
    SUM(click_count) as total_clicks,
    ROUND(100.0 * SUM(click_count) / NULLIF(SUM(search_count), 0), 1) as click_rate_pct,
    SUM(null_result_count) as total_null_results,
    ROUND(100.0 * SUM(null_result_count) / NULLIF(SUM(result_events), 0), 1) as null_rate_pct,
    COUNT(DISTINCT session_date) as days_active,
    MIN(first_seen_date) as first_seen,
    MAX(session_date) as last_seen
FROM searches_terms
GROUP BY search_term
ORDER BY total_searches DESC;

-- Trending terms (new terms with high volume)
CREATE OR REPLACE VIEW v_trending_terms AS
SELECT
    search_term,
    first_seen_date,
    SUM(search_count) as total_searches,
    SUM(click_count) as total_clicks,
    COUNT(DISTINCT session_date) as days_active
FROM searches_terms
WHERE first_seen_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY search_term, first_seen_date
ORDER BY total_searches DESC;

-- Zero-result terms (content gaps)
CREATE OR REPLACE VIEW v_zero_result_terms AS
SELECT
    search_term,
    SUM(search_count) as total_searches,
    SUM(null_result_count) as null_results,
    ROUND(100.0 * SUM(null_result_count) / NULLIF(SUM(result_events), 0), 1) as null_rate_pct,
    COUNT(DISTINCT session_date) as days_active
FROM searches_terms
GROUP BY search_term
HAVING SUM(null_result_count) > 0
ORDER BY null_results DESC;

-- Terms by status classification (aggregates ALL data - for PostgreSQL direct queries)
-- NOTE: For Power BI with date slicers, use DAX measures to calculate these dynamically
CREATE OR REPLACE VIEW v_terms_by_status AS
SELECT
    search_term,
    SUM(search_count) as total_searches,
    SUM(success_click_count) as total_success_clicks,
    SUM(null_result_count) as total_null_results,
    SUM(result_events) as total_result_events,
    ROUND(100.0 * SUM(success_click_count) / NULLIF(SUM(search_count), 0), 2) as ctr_pct,
    ROUND(100.0 * SUM(null_result_count) / NULLIF(SUM(result_events), 0), 2) as null_rate_pct,
    ROUND(
        (100.0 * SUM(success_click_count) / NULLIF(SUM(search_count), 0))
        - (100.0 * SUM(null_result_count) / NULLIF(SUM(result_events), 0) * 0.5)
    , 1) as effectiveness_score,
    -- Aggregate status based on overall metrics
    CASE
        WHEN 100.0 * SUM(null_result_count) / NULLIF(SUM(result_events), 0) > 50 THEN 'High Null Rate'
        WHEN 100.0 * SUM(success_click_count) / NULLIF(SUM(search_count), 0) > 30 THEN 'High CTR'
        WHEN 100.0 * SUM(success_click_count) / NULLIF(SUM(search_count), 0) < 10 THEN 'Low CTR'
        ELSE 'Moderate CTR'
    END as term_status,
    CASE
        WHEN 100.0 * SUM(null_result_count) / NULLIF(SUM(result_events), 0) > 50 THEN 1
        WHEN 100.0 * SUM(success_click_count) / NULLIF(SUM(search_count), 0) < 10 THEN 2
        WHEN 100.0 * SUM(success_click_count) / NULLIF(SUM(search_count), 0) > 30 THEN 4
        ELSE 3
    END as term_status_sort,
    COUNT(DISTINCT session_date) as days_active,
    MIN(first_seen_date) as first_seen,
    MAX(session_date) as last_seen
FROM searches_terms
GROUP BY search_term
ORDER BY total_searches DESC;

-- =============================================================================
-- Usage
-- =============================================================================
/*
-- Full refresh:
CALL sp_refresh_searches_terms();

-- Incremental update:
CALL sp_update_searches_terms_incremental(CURRENT_DATE - 1);

-- Top 20 search terms:
SELECT * FROM v_top_search_terms LIMIT 20;

-- New trending terms this week:
SELECT * FROM v_trending_terms LIMIT 20;

-- Terms with high null rates (content gaps):
SELECT * FROM v_zero_result_terms WHERE null_rate_pct > 50 LIMIT 20;
*/
