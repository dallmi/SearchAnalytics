-- =============================================================================
-- 9_data_retention.sql
-- =============================================================================
-- Purpose: Data retention and maintenance procedures
-- Usage:   Run daily as part of the maintenance pipeline
--
-- Retention Policies:
--   - searches_journeys:           180 days (full session granularity)
--   - searches_journeys_daily_agg: 2 prior years + current YTD (~3 years)
--   - raw_events:                  Partitions can be archived/dropped as needed
--
-- Schedule: Run sp_daily_maintenance() after daily data load
-- =============================================================================

-- =============================================================================
-- Procedure: Aggregate journeys to daily before purging
-- This MUST run before sp_purge_old_journeys() to preserve historical data
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_aggregate_journeys_to_daily(
    p_target_date DATE DEFAULT NULL  -- If NULL, aggregates all dates not yet in daily_agg
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_rows_inserted INTEGER;
    v_min_date DATE;
    v_max_date DATE;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;

    -- Determine date range to aggregate
    IF p_target_date IS NOT NULL THEN
        v_min_date := p_target_date;
        v_max_date := p_target_date;
    ELSE
        -- Find dates in journeys that aren't yet in daily_agg
        SELECT MIN(session_date), MAX(session_date)
        INTO v_min_date, v_max_date
        FROM searches_journeys sj
        WHERE NOT EXISTS (
            SELECT 1 FROM searches_journeys_daily_agg da
            WHERE da.session_date = sj.session_date
        );
    END IF;

    IF v_min_date IS NULL THEN
        RAISE NOTICE 'No new dates to aggregate';
        RETURN;
    END IF;

    RAISE NOTICE 'Aggregating journeys from % to %...', v_min_date, v_max_date;

    -- Insert or update daily aggregations
    INSERT INTO searches_journeys_daily_agg
    SELECT
        session_date,

        -- Volume metrics
        COUNT(*) as total_sessions,
        SUM(search_count_in_session) as total_searches,
        SUM(result_count) as total_results,
        SUM(click_count) as total_clicks,
        SUM(null_result_count) as total_null_results,

        -- Journey outcome counts
        SUM(CASE WHEN journey_outcome = 'Success' THEN 1 ELSE 0 END) as sessions_success,
        SUM(CASE WHEN journey_outcome = 'Engaged' THEN 1 ELSE 0 END) as sessions_engaged,
        SUM(CASE WHEN journey_outcome = 'Abandoned' THEN 1 ELSE 0 END) as sessions_abandoned,
        SUM(CASE WHEN journey_outcome = 'No Results' THEN 1 ELSE 0 END) as sessions_no_results,
        SUM(CASE WHEN journey_outcome = 'Unknown' THEN 1 ELSE 0 END) as sessions_unknown,

        -- Pre-calculated rates
        ROUND(100.0 * SUM(CASE WHEN journey_outcome = 'Success' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as success_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN journey_outcome = 'Engaged' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as engaged_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN journey_outcome = 'Abandoned' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as abandonment_rate_pct,
        ROUND(100.0 * SUM(CASE WHEN journey_outcome = 'No Results' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as no_results_rate_pct,

        -- Session complexity distribution
        SUM(CASE WHEN session_complexity = 'Single Event' THEN 1 ELSE 0 END) as sessions_single_event,
        SUM(CASE WHEN session_complexity = 'Simple' THEN 1 ELSE 0 END) as sessions_simple,
        SUM(CASE WHEN session_complexity = 'Medium' THEN 1 ELSE 0 END) as sessions_medium,
        SUM(CASE WHEN session_complexity = 'Complex' THEN 1 ELSE 0 END) as sessions_complex,

        -- Timing aggregates
        SUM(CASE WHEN sec_search_to_result IS NOT NULL THEN sec_search_to_result ELSE 0 END) as sum_sec_search_to_result,
        SUM(CASE WHEN sec_result_to_click IS NOT NULL THEN sec_result_to_click ELSE 0 END) as sum_sec_result_to_click,
        SUM(CASE WHEN total_duration_sec IS NOT NULL THEN total_duration_sec ELSE 0 END) as sum_total_duration_sec,
        SUM(CASE WHEN sec_search_to_result IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_result_timing,
        SUM(CASE WHEN sec_result_to_click IS NOT NULL THEN 1 ELSE 0 END) as sessions_with_click_timing,

        -- Pre-calculated averages
        ROUND(AVG(sec_search_to_result)::NUMERIC, 2) as avg_sec_search_to_result,
        ROUND(AVG(sec_result_to_click)::NUMERIC, 2) as avg_sec_result_to_click,
        ROUND(AVG(total_duration_sec)::NUMERIC, 2) as avg_session_duration_sec,
        ROUND(AVG(search_count_in_session)::NUMERIC, 2) as avg_searches_per_session,

        -- Timing bucket distribution
        SUM(CASE WHEN search_to_result_bucket = '< 0.5s' THEN 1 ELSE 0 END) as timing_bucket_lt_05s,
        SUM(CASE WHEN search_to_result_bucket = '0.5-1s' THEN 1 ELSE 0 END) as timing_bucket_05_1s,
        SUM(CASE WHEN search_to_result_bucket = '1-2s' THEN 1 ELSE 0 END) as timing_bucket_1_2s,
        SUM(CASE WHEN search_to_result_bucket = '2-5s' THEN 1 ELSE 0 END) as timing_bucket_2_5s,
        SUM(CASE WHEN search_to_result_bucket = '> 5s' THEN 1 ELSE 0 END) as timing_bucket_gt_5s,
        SUM(CASE WHEN search_to_result_bucket = 'No Result' THEN 1 ELSE 0 END) as timing_bucket_no_result,

        -- Click breakdown
        SUM(general_clicks) as total_general_clicks,
        SUM(news_clicks) as total_news_clicks,
        SUM(goto_clicks) as total_goto_clicks,
        SUM(people_clicks) as total_people_clicks,

        -- Behavioral flags
        SUM(CASE WHEN had_reformulation = true THEN 1 ELSE 0 END) as sessions_with_reformulation,
        SUM(CASE WHEN had_null_result = true THEN 1 ELSE 0 END) as sessions_with_null_result,
        SUM(CASE WHEN recovered_from_null = true THEN 1 ELSE 0 END) as sessions_recovered_from_null,
        SUM(CASE WHEN is_users_first_session = true THEN 1 ELSE 0 END) as sessions_first_time_users,

        -- Hour distribution
        SUM(CASE WHEN first_event_hour >= 6 AND first_event_hour < 12 THEN 1 ELSE 0 END) as sessions_morning,
        SUM(CASE WHEN first_event_hour >= 12 AND first_event_hour < 18 THEN 1 ELSE 0 END) as sessions_afternoon,
        SUM(CASE WHEN first_event_hour >= 18 AND first_event_hour < 24 THEN 1 ELSE 0 END) as sessions_evening,
        SUM(CASE WHEN first_event_hour >= 0 AND first_event_hour < 6 THEN 1 ELSE 0 END) as sessions_night

    FROM searches_journeys
    WHERE session_date BETWEEN v_min_date AND v_max_date
    GROUP BY session_date
    ON CONFLICT (session_date)
    DO UPDATE SET
        total_sessions = EXCLUDED.total_sessions,
        total_searches = EXCLUDED.total_searches,
        total_results = EXCLUDED.total_results,
        total_clicks = EXCLUDED.total_clicks,
        total_null_results = EXCLUDED.total_null_results,
        sessions_success = EXCLUDED.sessions_success,
        sessions_engaged = EXCLUDED.sessions_engaged,
        sessions_abandoned = EXCLUDED.sessions_abandoned,
        sessions_no_results = EXCLUDED.sessions_no_results,
        sessions_unknown = EXCLUDED.sessions_unknown,
        success_rate_pct = EXCLUDED.success_rate_pct,
        engaged_rate_pct = EXCLUDED.engaged_rate_pct,
        abandonment_rate_pct = EXCLUDED.abandonment_rate_pct,
        no_results_rate_pct = EXCLUDED.no_results_rate_pct,
        sessions_single_event = EXCLUDED.sessions_single_event,
        sessions_simple = EXCLUDED.sessions_simple,
        sessions_medium = EXCLUDED.sessions_medium,
        sessions_complex = EXCLUDED.sessions_complex,
        sum_sec_search_to_result = EXCLUDED.sum_sec_search_to_result,
        sum_sec_result_to_click = EXCLUDED.sum_sec_result_to_click,
        sum_total_duration_sec = EXCLUDED.sum_total_duration_sec,
        sessions_with_result_timing = EXCLUDED.sessions_with_result_timing,
        sessions_with_click_timing = EXCLUDED.sessions_with_click_timing,
        avg_sec_search_to_result = EXCLUDED.avg_sec_search_to_result,
        avg_sec_result_to_click = EXCLUDED.avg_sec_result_to_click,
        avg_session_duration_sec = EXCLUDED.avg_session_duration_sec,
        avg_searches_per_session = EXCLUDED.avg_searches_per_session,
        timing_bucket_lt_05s = EXCLUDED.timing_bucket_lt_05s,
        timing_bucket_05_1s = EXCLUDED.timing_bucket_05_1s,
        timing_bucket_1_2s = EXCLUDED.timing_bucket_1_2s,
        timing_bucket_2_5s = EXCLUDED.timing_bucket_2_5s,
        timing_bucket_gt_5s = EXCLUDED.timing_bucket_gt_5s,
        timing_bucket_no_result = EXCLUDED.timing_bucket_no_result,
        total_general_clicks = EXCLUDED.total_general_clicks,
        total_news_clicks = EXCLUDED.total_news_clicks,
        total_goto_clicks = EXCLUDED.total_goto_clicks,
        total_people_clicks = EXCLUDED.total_people_clicks,
        sessions_with_reformulation = EXCLUDED.sessions_with_reformulation,
        sessions_with_null_result = EXCLUDED.sessions_with_null_result,
        sessions_recovered_from_null = EXCLUDED.sessions_recovered_from_null,
        sessions_first_time_users = EXCLUDED.sessions_first_time_users,
        sessions_morning = EXCLUDED.sessions_morning,
        sessions_afternoon = EXCLUDED.sessions_afternoon,
        sessions_evening = EXCLUDED.sessions_evening,
        sessions_night = EXCLUDED.sessions_night;

    GET DIAGNOSTICS v_rows_inserted = ROW_COUNT;

    -- Log the operation
    INSERT INTO data_retention_log (table_name, operation_type, date_range_start, date_range_end, rows_affected, notes)
    VALUES ('searches_journeys_daily_agg', 'AGGREGATE', v_min_date, v_max_date, v_rows_inserted,
            'Aggregated from searches_journeys');

    RAISE NOTICE 'Aggregation complete: % day(s) processed in % seconds',
        v_rows_inserted,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_aggregate_journeys_to_daily IS 'Aggregates session-level journeys to daily level before purging.';

-- =============================================================================
-- Procedure: Purge old journeys (keeps last 180 days)
-- IMPORTANT: Run sp_aggregate_journeys_to_daily() FIRST!
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_purge_old_journeys(
    p_retention_days INTEGER DEFAULT 180
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_cutoff_date DATE;
    v_rows_deleted INTEGER;
    v_min_date DATE;
    v_max_date DATE;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    v_cutoff_date := CURRENT_DATE - p_retention_days;

    -- Find date range to be deleted
    SELECT MIN(session_date), MAX(session_date)
    INTO v_min_date, v_max_date
    FROM searches_journeys
    WHERE session_date < v_cutoff_date;

    IF v_min_date IS NULL THEN
        RAISE NOTICE 'No data older than % days to purge', p_retention_days;
        RETURN;
    END IF;

    -- Verify aggregation exists for dates to be deleted
    IF EXISTS (
        SELECT 1
        FROM searches_journeys sj
        WHERE sj.session_date < v_cutoff_date
          AND NOT EXISTS (
              SELECT 1 FROM searches_journeys_daily_agg da
              WHERE da.session_date = sj.session_date
          )
    ) THEN
        RAISE EXCEPTION 'Cannot purge: Some dates have not been aggregated. Run sp_aggregate_journeys_to_daily() first.';
    END IF;

    RAISE NOTICE 'Purging journeys older than % (% to %)...', v_cutoff_date, v_min_date, v_max_date;

    -- Delete old records
    DELETE FROM searches_journeys
    WHERE session_date < v_cutoff_date;

    GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;

    -- Log the operation
    INSERT INTO data_retention_log (table_name, operation_type, date_range_start, date_range_end, rows_affected, notes)
    VALUES ('searches_journeys', 'DELETE', v_min_date, v_max_date, v_rows_deleted,
            FORMAT('Retention: %s days, cutoff: %s', p_retention_days, v_cutoff_date));

    RAISE NOTICE 'Purge complete: % sessions deleted in % seconds',
        v_rows_deleted,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_purge_old_journeys IS 'Purges journeys older than retention period (default 180 days). Aggregation must happen first.';

-- =============================================================================
-- Procedure: Purge old daily aggregations (keeps ~3 years)
-- Retention: 2 full prior years + current YTD
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_purge_old_daily_agg()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_cutoff_date DATE;
    v_rows_deleted INTEGER;
    v_current_year INTEGER;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    v_current_year := EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER;

    -- Keep current year + 2 prior years
    -- Example: In 2026, keep 2024, 2025, 2026 (delete anything before 2024-01-01)
    v_cutoff_date := MAKE_DATE(v_current_year - 2, 1, 1);

    RAISE NOTICE 'Purging daily aggregations before %...', v_cutoff_date;

    DELETE FROM searches_journeys_daily_agg
    WHERE session_date < v_cutoff_date;

    GET DIAGNOSTICS v_rows_deleted = ROW_COUNT;

    IF v_rows_deleted > 0 THEN
        INSERT INTO data_retention_log (table_name, operation_type, date_range_start, date_range_end, rows_affected, notes)
        VALUES ('searches_journeys_daily_agg', 'DELETE', NULL, v_cutoff_date - 1, v_rows_deleted,
                FORMAT('Keeping years: %s-%s', v_current_year - 2, v_current_year));
    END IF;

    RAISE NOTICE 'Purge complete: % days deleted in % seconds',
        v_rows_deleted,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_purge_old_daily_agg IS 'Purges daily aggregations older than 2 prior years + current year.';

-- =============================================================================
-- Procedure: Archive old raw_events partitions
-- Detaches partition without dropping (can be reattached if needed)
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_archive_partition(
    p_year INTEGER,
    p_month INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_partition_name TEXT;
    v_archive_name TEXT;
BEGIN
    v_partition_name := FORMAT('raw_events_%s_%s', p_year, LPAD(p_month::TEXT, 2, '0'));
    v_archive_name := v_partition_name || '_archived';

    -- Check if partition exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables WHERE tablename = v_partition_name
    ) THEN
        RAISE NOTICE 'Partition % does not exist', v_partition_name;
        RETURN;
    END IF;

    -- Detach partition
    EXECUTE FORMAT('ALTER TABLE raw_events DETACH PARTITION %I', v_partition_name);

    -- Rename to archived
    EXECUTE FORMAT('ALTER TABLE %I RENAME TO %I', v_partition_name, v_archive_name);

    -- Log the operation
    INSERT INTO data_retention_log (table_name, operation_type, date_range_start, date_range_end, rows_affected, notes)
    VALUES ('raw_events', 'ARCHIVE', MAKE_DATE(p_year, p_month, 1),
            (MAKE_DATE(p_year, p_month, 1) + INTERVAL '1 month - 1 day')::DATE,
            NULL, FORMAT('Partition archived as %s', v_archive_name));

    RAISE NOTICE 'Partition % archived as %', v_partition_name, v_archive_name;
END;
$$;

COMMENT ON PROCEDURE sp_archive_partition IS 'Archives a raw_events partition by detaching and renaming it.';

-- =============================================================================
-- Procedure: Drop archived partition (permanent deletion)
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_drop_archived_partition(
    p_year INTEGER,
    p_month INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_archive_name TEXT;
    v_row_count INTEGER;
BEGIN
    v_archive_name := FORMAT('raw_events_%s_%s_archived', p_year, LPAD(p_month::TEXT, 2, '0'));

    -- Check if archived partition exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_tables WHERE tablename = v_archive_name
    ) THEN
        RAISE NOTICE 'Archived partition % does not exist', v_archive_name;
        RETURN;
    END IF;

    -- Get row count before dropping
    EXECUTE FORMAT('SELECT COUNT(*) FROM %I', v_archive_name) INTO v_row_count;

    -- Drop the table
    EXECUTE FORMAT('DROP TABLE %I', v_archive_name);

    -- Log the operation
    INSERT INTO data_retention_log (table_name, operation_type, date_range_start, date_range_end, rows_affected, notes)
    VALUES ('raw_events', 'DELETE', MAKE_DATE(p_year, p_month, 1),
            (MAKE_DATE(p_year, p_month, 1) + INTERVAL '1 month - 1 day')::DATE,
            v_row_count, FORMAT('Dropped archived partition %s', v_archive_name));

    RAISE NOTICE 'Dropped archived partition % (% rows)', v_archive_name, v_row_count;
END;
$$;

COMMENT ON PROCEDURE sp_drop_archived_partition IS 'Permanently drops an archived raw_events partition.';

-- =============================================================================
-- Master Procedure: Daily maintenance
-- Run this after daily data load completes
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_daily_maintenance()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Starting daily maintenance at %', v_start_time;
    RAISE NOTICE '========================================';

    -- Step 1: Aggregate any new journey dates to daily
    RAISE NOTICE '';
    RAISE NOTICE '--- Step 1: Aggregating journeys to daily ---';
    CALL sp_aggregate_journeys_to_daily();

    -- Step 2: Purge old journeys (keeps 180 days)
    RAISE NOTICE '';
    RAISE NOTICE '--- Step 2: Purging old journeys (180-day retention) ---';
    CALL sp_purge_old_journeys(180);

    -- Step 3: Purge old daily aggregations (keeps ~3 years)
    RAISE NOTICE '';
    RAISE NOTICE '--- Step 3: Purging old daily aggregations (3-year retention) ---';
    CALL sp_purge_old_daily_agg();

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Daily maintenance complete in % seconds',
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
    RAISE NOTICE '========================================';
END;
$$;

COMMENT ON PROCEDURE sp_daily_maintenance IS 'Master procedure for daily maintenance: aggregate, then purge old data.';

-- =============================================================================
-- View: Data retention status
-- =============================================================================
CREATE OR REPLACE VIEW v_retention_status AS
SELECT
    'searches_journeys' as table_name,
    COUNT(*) as row_count,
    MIN(session_date) as oldest_date,
    MAX(session_date) as newest_date,
    MAX(session_date) - MIN(session_date) + 1 as days_span,
    '180 days' as retention_policy
FROM searches_journeys

UNION ALL

SELECT
    'searches_journeys_daily_agg',
    COUNT(*),
    MIN(session_date),
    MAX(session_date),
    MAX(session_date) - MIN(session_date) + 1,
    '2 prior years + YTD'
FROM searches_journeys_daily_agg

UNION ALL

SELECT
    'raw_events (estimated)',
    (SELECT SUM(n_live_tup) FROM pg_stat_user_tables WHERE relname LIKE 'raw_events_20%'),
    (SELECT MIN(timestamp)::DATE FROM raw_events),
    (SELECT MAX(timestamp)::DATE FROM raw_events),
    NULL,
    'Partitioned by month';

COMMENT ON VIEW v_retention_status IS 'Shows current data retention status for all tables.';

-- =============================================================================
-- View: Partition status
-- =============================================================================
CREATE OR REPLACE VIEW v_partition_status AS
SELECT
    child.relname as partition_name,
    pg_size_pretty(pg_total_relation_size(child.oid)) as size,
    CASE
        WHEN child.relname LIKE '%_archived' THEN 'Archived'
        ELSE 'Active'
    END as status,
    (SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname = child.relname) as estimated_rows
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'raw_events'

UNION ALL

SELECT
    tablename,
    pg_size_pretty(pg_total_relation_size(quote_ident(tablename))),
    'Archived (detached)',
    NULL
FROM pg_tables
WHERE tablename LIKE 'raw_events_%_archived'

ORDER BY partition_name;

COMMENT ON VIEW v_partition_status IS 'Shows status of all raw_events partitions.';

-- =============================================================================
-- Usage Examples
-- =============================================================================
/*
-- Daily maintenance (run after data load):
CALL sp_daily_maintenance();

-- Check retention status:
SELECT * FROM v_retention_status;

-- Check partition status:
SELECT * FROM v_partition_status;

-- View retention log:
SELECT * FROM data_retention_log ORDER BY operation_date DESC LIMIT 20;

-- Manual aggregation for a specific date:
CALL sp_aggregate_journeys_to_daily('2025-06-15'::DATE);

-- Archive an old partition (e.g., January 2024):
CALL sp_archive_partition(2024, 1);

-- Permanently drop archived partition:
CALL sp_drop_archived_partition(2024, 1);

-- Create partitions for next year (run in December):
CALL sp_create_partitions_for_year(2028);
*/
