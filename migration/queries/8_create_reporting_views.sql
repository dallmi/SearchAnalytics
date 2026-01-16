-- =============================================================================
-- 8_create_reporting_views.sql
-- =============================================================================
-- Purpose: Create Power BI optimized views and reporting objects
-- Usage:   Run after all tables are populated
--
-- These views are designed for:
--   - Power BI DirectQuery performance
--   - Pre-calculated metrics to reduce DAX complexity
--   - Consistent business logic across reports
--
-- Power BI should connect to these views rather than base tables
-- =============================================================================

-- =============================================================================
-- View: rpt_searches_journeys
-- Purpose: Session-level reporting with all calculated fields
-- Power BI: Use for session analysis, funnel charts, timing analysis
-- =============================================================================
CREATE OR REPLACE VIEW rpt_searches_journeys AS
SELECT
    -- Dimensions
    session_date,
    journey_outcome,
    session_complexity,
    search_to_result_bucket,
    result_to_click_bucket,
    session_duration_bucket,

    -- Sort columns (use these to sort dimensions in Power BI)
    journey_outcome_sort,
    session_complexity_sort,
    search_to_result_sort,
    result_to_click_sort,
    session_duration_sort,

    -- Measures (use SUM in Power BI)
    1 as session_count,  -- COUNT(*) in Power BI = SUM(session_count)
    search_count_in_session,
    result_count,
    click_count,
    null_result_count,

    -- Timing (for averages, use SUM/COUNT pattern in DAX)
    sec_search_to_result,
    sec_result_to_click,
    total_duration_sec,

    -- Flags (for filtering)
    had_null_result,
    recovered_from_null,
    had_reformulation,
    is_users_first_session,

    -- Click breakdown
    general_clicks,
    news_clicks,
    goto_clicks,
    people_clicks,

    -- Time of day
    first_event_hour

FROM searches_journeys;

COMMENT ON VIEW rpt_searches_journeys IS 'Power BI optimized view for session-level journey analysis.';

-- =============================================================================
-- View: rpt_searches_daily
-- Purpose: Daily KPI dashboard
-- Power BI: Use for trend lines, daily overview, KPI cards
-- =============================================================================
CREATE OR REPLACE VIEW rpt_searches_daily AS
SELECT
    -- Date dimension
    date,
    day_of_week,
    day_of_week_num,
    EXTRACT(WEEK FROM date)::INTEGER as week_number,
    EXTRACT(MONTH FROM date)::INTEGER as month_number,
    TO_CHAR(date, 'Mon YYYY') as month_label,

    -- Volume KPIs
    total_events,
    unique_sessions,
    unique_users,
    search_starts,
    click_events,
    null_results,

    -- Rate KPIs (pre-calculated)
    session_success_rate_pct,
    session_abandonment_rate_pct,
    click_rate_pct,
    null_rate_pct,

    -- Averages
    avg_searches_per_session,
    avg_search_term_length,

    -- User cohorts
    new_users,
    returning_users,
    CASE WHEN unique_users > 0
         THEN ROUND(100.0 * returning_users / unique_users, 1)
         ELSE 0 END as returning_user_pct,

    -- Time distribution
    searches_morning,
    searches_afternoon,
    searches_evening,
    searches_night,

    -- Click distribution
    clicks_general,
    clicks_all,
    clicks_news,
    clicks_goto,
    clicks_people

FROM searches_daily;

COMMENT ON VIEW rpt_searches_daily IS 'Power BI optimized view for daily KPI dashboard.';

-- =============================================================================
-- View: rpt_searches_terms
-- Purpose: Search term analysis and content gap detection
-- Power BI: Use for top terms, zero-result analysis, term trends
-- =============================================================================
CREATE OR REPLACE VIEW rpt_searches_terms AS
SELECT
    -- Dimensions
    session_date,
    search_term,
    word_count,
    first_seen_date,
    is_new_term,

    -- Volume metrics
    search_count,
    unique_users,
    unique_sessions,

    -- Result metrics
    result_events,
    null_result_count,
    CASE WHEN result_events > 0
         THEN ROUND(100.0 * null_result_count / result_events, 1)
         ELSE 0 END as null_rate_pct,

    -- Click metrics
    click_count,
    CASE WHEN search_count > 0
         THEN ROUND(100.0 * click_count / search_count, 1)
         ELSE 0 END as click_rate_pct,

    -- Timing
    avg_sec_to_click,

    -- Classification
    CASE
        WHEN null_result_count > 0 AND result_events = null_result_count THEN 'Zero Results'
        WHEN click_count = 0 THEN 'No Clicks'
        WHEN click_count > 0 AND click_count < search_count THEN 'Partial Success'
        ELSE 'Success'
    END as term_outcome

FROM searches_terms;

COMMENT ON VIEW rpt_searches_terms IS 'Power BI optimized view for search term analysis.';

-- =============================================================================
-- View: rpt_journey_funnel
-- Purpose: Pre-aggregated funnel data for visualization
-- Power BI: Use for funnel charts showing conversion through stages
-- =============================================================================
CREATE OR REPLACE VIEW rpt_journey_funnel AS
SELECT
    session_date,
    COUNT(*) as total_sessions,
    SUM(CASE WHEN search_count_in_session > 0 THEN 1 ELSE 0 END) as sessions_with_search,
    SUM(CASE WHEN result_count > 0 THEN 1 ELSE 0 END) as sessions_with_results,
    SUM(CASE WHEN result_count > 0 AND null_result_count < result_count THEN 1 ELSE 0 END) as sessions_with_clickable_results,
    SUM(CASE WHEN click_count > 0 THEN 1 ELSE 0 END) as sessions_with_clicks
FROM searches_journeys
GROUP BY session_date;

COMMENT ON VIEW rpt_journey_funnel IS 'Pre-aggregated funnel metrics by date for conversion visualization.';

-- =============================================================================
-- View: rpt_timing_distribution
-- Purpose: Timing bucket distribution for histogram visualization
-- Power BI: Use for timing histograms and performance analysis
-- =============================================================================
CREATE OR REPLACE VIEW rpt_timing_distribution AS
SELECT
    session_date,
    search_to_result_bucket,
    search_to_result_sort,
    COUNT(*) as session_count,
    ROUND(AVG(sec_search_to_result)::NUMERIC, 2) as avg_seconds
FROM searches_journeys
WHERE sec_search_to_result IS NOT NULL
GROUP BY session_date, search_to_result_bucket, search_to_result_sort

UNION ALL

SELECT
    session_date,
    result_to_click_bucket as bucket,
    result_to_click_sort as sort_order,
    COUNT(*) as session_count,
    ROUND(AVG(sec_result_to_click)::NUMERIC, 2) as avg_seconds
FROM searches_journeys
WHERE sec_result_to_click IS NOT NULL
GROUP BY session_date, result_to_click_bucket, result_to_click_sort;

COMMENT ON VIEW rpt_timing_distribution IS 'Timing bucket distribution for histogram charts.';

-- =============================================================================
-- View: rpt_journey_types
-- Purpose: Human-readable journey pattern analysis
-- Power BI: Use for journey type breakdown (like the Excel Journey Types tab)
-- =============================================================================
CREATE OR REPLACE VIEW rpt_journey_types AS
SELECT
    session_date,

    -- Generate human-readable journey type string
    CASE
        WHEN search_count_in_session > 0 AND result_count > 0 AND click_count > 0 THEN
            search_count_in_session::TEXT || ' Search → ' ||
            result_count::TEXT || ' Result → ' ||
            click_count::TEXT || ' Click'

        WHEN search_count_in_session > 0 AND result_count > 0 AND null_result_count > 0 AND click_count = 0 THEN
            search_count_in_session::TEXT || ' Search → ' ||
            result_count::TEXT || ' Result (incl. ' || null_result_count::TEXT || ' null) → No Click'

        WHEN search_count_in_session > 0 AND result_count > 0 AND click_count = 0 THEN
            search_count_in_session::TEXT || ' Search → ' ||
            result_count::TEXT || ' Result → Abandoned'

        WHEN search_count_in_session > 0 AND result_count = 0 THEN
            search_count_in_session::TEXT || ' Search → No Result'

        ELSE 'Other'
    END as journey_type,

    -- Counts for aggregation
    1 as session_count,
    search_count_in_session,
    result_count,
    click_count,
    null_result_count,
    journey_outcome

FROM searches_journeys;

COMMENT ON VIEW rpt_journey_types IS 'Human-readable journey patterns matching Excel Journey Types analysis.';

-- =============================================================================
-- Materialized View: mv_hourly_patterns (optional, for large datasets)
-- Purpose: Pre-aggregated hourly patterns for performance
-- Note: Requires periodic refresh (REFRESH MATERIALIZED VIEW)
-- =============================================================================
-- Uncomment if you have performance issues with hourly analysis:

-- CREATE MATERIALIZED VIEW mv_hourly_patterns AS
-- SELECT
--     session_date,
--     first_event_hour as hour,
--     journey_outcome,
--     COUNT(*) as session_count,
--     SUM(search_count_in_session) as total_searches,
--     SUM(click_count) as total_clicks
-- FROM searches_journeys
-- GROUP BY session_date, first_event_hour, journey_outcome;
--
-- CREATE INDEX idx_mv_hourly_date ON mv_hourly_patterns (session_date);

-- =============================================================================
-- View: rpt_journeys_trend (Unified Historical + Recent)
-- =============================================================================
-- Combines:
--   - Recent 180 days: Full session granularity from searches_journeys
--   - Historical (180+ days): Daily aggregated from searches_journeys_daily_agg
--
-- Power BI: Use for 3-year trend analysis with automatic data source switching
-- =============================================================================
CREATE OR REPLACE VIEW rpt_journeys_trend AS

-- Recent data (last 180 days) - aggregated from full journeys
SELECT
    session_date,
    'Recent (Detail Available)' as data_source,

    -- Volume metrics
    COUNT(*) as total_sessions,
    SUM(search_count_in_session) as total_searches,
    SUM(result_count) as total_results,
    SUM(click_count) as total_clicks,
    SUM(null_result_count) as total_null_results,

    -- Journey outcomes
    SUM(CASE WHEN journey_outcome = 'Success' THEN 1 ELSE 0 END) as sessions_success,
    SUM(CASE WHEN journey_outcome = 'Abandoned' THEN 1 ELSE 0 END) as sessions_abandoned,
    SUM(CASE WHEN journey_outcome = 'No Results' THEN 1 ELSE 0 END) as sessions_no_results,

    -- Rates
    ROUND(100.0 * SUM(CASE WHEN journey_outcome = 'Success' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as success_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN journey_outcome = 'Abandoned' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as abandonment_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN journey_outcome = 'No Results' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) as no_results_rate_pct,

    -- Timing averages
    ROUND(AVG(sec_search_to_result)::NUMERIC, 2) as avg_sec_search_to_result,
    ROUND(AVG(sec_result_to_click)::NUMERIC, 2) as avg_sec_result_to_click,
    ROUND(AVG(total_duration_sec)::NUMERIC, 2) as avg_session_duration_sec,

    -- Timing distribution
    SUM(CASE WHEN search_to_result_bucket = '< 0.5s' THEN 1 ELSE 0 END) as timing_bucket_lt_05s,
    SUM(CASE WHEN search_to_result_bucket = '0.5-1s' THEN 1 ELSE 0 END) as timing_bucket_05_1s,
    SUM(CASE WHEN search_to_result_bucket = '1-2s' THEN 1 ELSE 0 END) as timing_bucket_1_2s,
    SUM(CASE WHEN search_to_result_bucket = '2-5s' THEN 1 ELSE 0 END) as timing_bucket_2_5s,
    SUM(CASE WHEN search_to_result_bucket = '> 5s' THEN 1 ELSE 0 END) as timing_bucket_gt_5s,
    SUM(CASE WHEN search_to_result_bucket = 'No Result' THEN 1 ELSE 0 END) as timing_bucket_no_result,

    -- Behavioral metrics
    SUM(CASE WHEN had_reformulation = true THEN 1 ELSE 0 END) as sessions_with_reformulation,
    SUM(CASE WHEN is_users_first_session = true THEN 1 ELSE 0 END) as sessions_first_time_users

FROM searches_journeys
GROUP BY session_date

UNION ALL

-- Historical data (older than 180 days) - from pre-aggregated table
SELECT
    session_date,
    'Historical (Aggregated)' as data_source,

    total_sessions,
    total_searches,
    total_results,
    total_clicks,
    total_null_results,

    sessions_success,
    sessions_abandoned,
    sessions_no_results,

    success_rate_pct,
    abandonment_rate_pct,
    no_results_rate_pct,

    avg_sec_search_to_result,
    avg_sec_result_to_click,
    avg_session_duration_sec,

    timing_bucket_lt_05s,
    timing_bucket_05_1s,
    timing_bucket_1_2s,
    timing_bucket_2_5s,
    timing_bucket_gt_5s,
    timing_bucket_no_result,

    sessions_with_reformulation,
    sessions_first_time_users

FROM searches_journeys_daily_agg
WHERE session_date < (CURRENT_DATE - 180);

COMMENT ON VIEW rpt_journeys_trend IS 'Unified 3-year trend view: recent 180 days from detail, older from daily aggregation.';

-- =============================================================================
-- View: rpt_journeys_daily_agg (Direct access to aggregated data)
-- =============================================================================
-- For when you specifically need the aggregated historical data
-- Power BI: Use for lightweight historical trend dashboards
-- =============================================================================
CREATE OR REPLACE VIEW rpt_journeys_daily_agg AS
SELECT
    session_date,

    -- Volume
    total_sessions,
    total_searches,
    total_clicks,
    total_null_results,

    -- Outcomes
    sessions_success,
    sessions_abandoned,
    sessions_no_results,

    -- Rates
    success_rate_pct,
    abandonment_rate_pct,
    no_results_rate_pct,

    -- Timing
    avg_sec_search_to_result,
    avg_sec_result_to_click,
    avg_session_duration_sec,
    avg_searches_per_session,

    -- For custom calculations
    sum_sec_search_to_result,
    sum_sec_result_to_click,
    sessions_with_result_timing,
    sessions_with_click_timing,

    -- Timing distribution (for histograms)
    timing_bucket_lt_05s,
    timing_bucket_05_1s,
    timing_bucket_1_2s,
    timing_bucket_2_5s,
    timing_bucket_gt_5s,
    timing_bucket_no_result,

    -- Complexity distribution
    sessions_single_event,
    sessions_simple,
    sessions_medium,
    sessions_complex,

    -- Behavioral
    sessions_with_reformulation,
    sessions_with_null_result,
    sessions_recovered_from_null,
    sessions_first_time_users,

    -- Time of day
    sessions_morning,
    sessions_afternoon,
    sessions_evening,
    sessions_night,

    -- Click breakdown
    total_general_clicks,
    total_news_clicks,
    total_goto_clicks,
    total_people_clicks

FROM searches_journeys_daily_agg;

COMMENT ON VIEW rpt_journeys_daily_agg IS 'Daily aggregated journey metrics for historical trend analysis.';

-- =============================================================================
-- Master Refresh Procedure: Run all transformations in sequence
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_refresh_all()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting full refresh of all tables...';

    -- Step 1: Transform raw events to searches
    CALL sp_refresh_searches();

    -- Step 2: Aggregate to journeys
    CALL sp_refresh_searches_journeys();

    -- Step 3: Aggregate to daily
    CALL sp_refresh_searches_daily();

    -- Step 4: Aggregate search terms
    CALL sp_refresh_searches_terms();

    RAISE NOTICE 'Full refresh complete in % seconds',
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_refresh_all IS 'Master procedure to refresh all tables in correct order.';

-- =============================================================================
-- Incremental Update Procedure: For daily runs
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_update_all_incremental(p_start_date DATE DEFAULT CURRENT_DATE - 1)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE 'Starting incremental update from %...', p_start_date;

    CALL sp_update_searches_incremental(p_start_date);
    CALL sp_update_searches_journeys_incremental(p_start_date);
    CALL sp_update_searches_daily_incremental(p_start_date);
    CALL sp_update_searches_terms_incremental(p_start_date);

    RAISE NOTICE 'Incremental update complete in % seconds',
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_update_all_incremental IS 'Master procedure for daily incremental updates.';

-- =============================================================================
-- Data Quality Check View
-- =============================================================================
CREATE OR REPLACE VIEW v_data_quality_check AS
SELECT
    'raw_events' as table_name,
    COUNT(*) as row_count,
    MIN(DATE(timestamp)) as min_date,
    MAX(DATE(timestamp)) as max_date,
    COUNT(DISTINCT DATE(timestamp)) as days_count
FROM raw_events

UNION ALL

SELECT 'searches', COUNT(*), MIN(session_date), MAX(session_date), COUNT(DISTINCT session_date)
FROM searches

UNION ALL

SELECT 'searches_journeys', COUNT(*), MIN(session_date), MAX(session_date), COUNT(DISTINCT session_date)
FROM searches_journeys

UNION ALL

SELECT 'searches_journeys_daily_agg', COUNT(*), MIN(session_date), MAX(session_date), COUNT(*)
FROM searches_journeys_daily_agg

UNION ALL

SELECT 'searches_daily', COUNT(*), MIN(date), MAX(date), COUNT(*)
FROM searches_daily

UNION ALL

SELECT 'searches_terms', COUNT(*), MIN(session_date), MAX(session_date), COUNT(DISTINCT session_date)
FROM searches_terms;

-- =============================================================================
-- Usage Examples
-- =============================================================================
/*
-- Full refresh (initial load or monthly):
CALL sp_refresh_all();

-- Daily incremental update:
CALL sp_update_all_incremental(CURRENT_DATE - 1);

-- Check data quality across all tables:
SELECT * FROM v_data_quality_check;

-- Power BI Queries - connect to these views:
--
-- RECENT DATA (last 180 days, full detail):
--   rpt_searches_journeys    - Session-level analysis, journey patterns
--   rpt_journey_types        - Human-readable journey patterns
--
-- HISTORICAL TRENDS (3 years):
--   rpt_journeys_trend       - Unified view (auto-switches recent/historical)
--   rpt_journeys_daily_agg   - Daily aggregated metrics
--
-- ALWAYS AVAILABLE:
--   rpt_searches_daily       - Daily event-level KPIs
--   rpt_searches_terms       - Search term analysis
--   rpt_journey_funnel       - Funnel metrics
*/

-- =============================================================================
-- Grant read access to Power BI service account
-- =============================================================================
-- Uncomment and adjust for your environment:
-- GRANT SELECT ON rpt_searches_daily TO powerbi_reader;
-- GRANT SELECT ON rpt_searches_journeys TO powerbi_reader;
-- GRANT SELECT ON rpt_searches_terms TO powerbi_reader;
-- GRANT SELECT ON rpt_journey_funnel TO powerbi_reader;
-- GRANT SELECT ON rpt_timing_distribution TO powerbi_reader;
-- GRANT SELECT ON rpt_journey_types TO powerbi_reader;
-- GRANT SELECT ON rpt_journeys_trend TO powerbi_reader;
-- GRANT SELECT ON rpt_journeys_daily_agg TO powerbi_reader;
