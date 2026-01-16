-- =============================================================================
-- 3_load_raw_events.sql
-- =============================================================================
-- Purpose: Upsert raw events from staging into raw_events table
-- Usage:   Run after Azure Data Factory loads data into a staging table
--
-- This script handles:
--   1. Column name normalization
--   2. Upsert logic (INSERT ON CONFLICT UPDATE)
--   3. Data type conversions
--
-- Prerequisites:
--   - Data has been loaded into staging_events table by ADF
--   - raw_events table exists (created by 2_create_schema.sql)
-- =============================================================================

-- =============================================================================
-- Option A: Direct UPSERT from ADF staging table
-- =============================================================================
-- Use this if ADF loads data into a staging table called 'staging_events'

-- Create staging table (ADF will write here)
DROP TABLE IF EXISTS staging_events;

CREATE TABLE staging_events (
    timestamp           TIMESTAMP,
    name                VARCHAR(255),
    user_id             VARCHAR(255),
    session_id          VARCHAR(255),
    searchquery         TEXT,
    cp_searchquery      TEXT,
    query               TEXT,
    cp_totalresultcount VARCHAR(50),
    totalresultcount    VARCHAR(50),
    cp_tab              VARCHAR(100),
    tab                 VARCHAR(100),
    cp_resultposition   VARCHAR(50),
    cp_clickedurl       TEXT,
    client_type         VARCHAR(100),
    client_os           VARCHAR(100),
    client_browser      VARCHAR(255)
);

COMMENT ON TABLE staging_events IS 'Temporary staging table for ADF data loads. Truncated after each load.';

-- =============================================================================
-- Upsert procedure: Call this after ADF loads data
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_upsert_raw_events(p_source_file VARCHAR DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted INTEGER;
    v_updated INTEGER;
    v_start_time TIMESTAMP;
BEGIN
    v_start_time := CURRENT_TIMESTAMP;

    -- Count rows in staging
    SELECT COUNT(*) INTO v_inserted FROM staging_events;
    RAISE NOTICE 'Processing % rows from staging...', v_inserted;

    -- Perform upsert with conflict handling
    INSERT INTO raw_events (
        timestamp,
        name,
        user_id,
        session_id,
        search_query,
        cp_search_query,
        query,
        cp_total_result_count,
        total_result_count,
        cp_tab,
        tab,
        cp_result_position,
        cp_clicked_url,
        client_type,
        client_os,
        client_browser,
        loaded_at,
        source_file
    )
    SELECT
        timestamp,
        name,
        user_id,
        session_id,
        searchquery,
        cp_searchquery,
        query,
        cp_totalresultcount,
        totalresultcount,
        cp_tab,
        tab,
        cp_resultposition,
        cp_clickedurl,
        client_type,
        client_os,
        client_browser,
        CURRENT_TIMESTAMP,
        COALESCE(p_source_file, 'ADF_' || TO_CHAR(CURRENT_DATE, 'YYYY_MM_DD'))
    FROM staging_events
    ON CONFLICT (timestamp, user_id, session_id, name)
    DO UPDATE SET
        search_query = EXCLUDED.search_query,
        cp_search_query = EXCLUDED.cp_search_query,
        query = EXCLUDED.query,
        cp_total_result_count = EXCLUDED.cp_total_result_count,
        total_result_count = EXCLUDED.total_result_count,
        cp_tab = EXCLUDED.cp_tab,
        tab = EXCLUDED.tab,
        cp_result_position = EXCLUDED.cp_result_position,
        cp_clicked_url = EXCLUDED.cp_clicked_url,
        client_type = EXCLUDED.client_type,
        client_os = EXCLUDED.client_os,
        client_browser = EXCLUDED.client_browser,
        loaded_at = CURRENT_TIMESTAMP,
        source_file = COALESCE(p_source_file, 'ADF_' || TO_CHAR(CURRENT_DATE, 'YYYY_MM_DD'));

    -- Get counts
    GET DIAGNOSTICS v_inserted = ROW_COUNT;

    -- Truncate staging table
    TRUNCATE TABLE staging_events;

    RAISE NOTICE 'Upsert complete: % rows processed in % seconds',
        v_inserted,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - v_start_time))::NUMERIC(10,2);
END;
$$;

COMMENT ON PROCEDURE sp_upsert_raw_events IS 'Upsert data from staging_events into raw_events. Call after ADF load.';

-- =============================================================================
-- Option B: Direct INSERT for clean loads (no conflict expected)
-- =============================================================================
-- Use this for initial historical data loads where duplicates are pre-filtered

CREATE OR REPLACE PROCEDURE sp_insert_raw_events_bulk(p_source_file VARCHAR DEFAULT NULL)
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted INTEGER;
BEGIN
    INSERT INTO raw_events (
        timestamp,
        name,
        user_id,
        session_id,
        search_query,
        cp_search_query,
        query,
        cp_total_result_count,
        total_result_count,
        cp_tab,
        tab,
        cp_result_position,
        cp_clicked_url,
        client_type,
        client_os,
        client_browser,
        loaded_at,
        source_file
    )
    SELECT
        timestamp,
        name,
        user_id,
        session_id,
        searchquery,
        cp_searchquery,
        query,
        cp_totalresultcount,
        totalresultcount,
        cp_tab,
        tab,
        cp_resultposition,
        cp_clickedurl,
        client_type,
        client_os,
        client_browser,
        CURRENT_TIMESTAMP,
        COALESCE(p_source_file, 'BULK_' || TO_CHAR(CURRENT_DATE, 'YYYY_MM_DD'))
    FROM staging_events;

    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    TRUNCATE TABLE staging_events;

    RAISE NOTICE 'Bulk insert complete: % rows inserted', v_inserted;
END;
$$;

-- =============================================================================
-- Utility: Check for duplicates before loading
-- =============================================================================
CREATE OR REPLACE VIEW v_staging_duplicates AS
SELECT
    s.timestamp,
    s.user_id,
    s.session_id,
    s.name,
    CASE WHEN r.timestamp IS NOT NULL THEN 'EXISTS' ELSE 'NEW' END as status
FROM staging_events s
LEFT JOIN raw_events r ON
    s.timestamp = r.timestamp
    AND s.user_id = r.user_id
    AND s.session_id = r.session_id
    AND s.name = r.name;

-- =============================================================================
-- Utility: Data quality checks
-- =============================================================================
CREATE OR REPLACE VIEW v_raw_events_quality AS
SELECT
    DATE(timestamp) as event_date,
    COUNT(*) as total_events,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions,
    COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_user_count,
    COUNT(CASE WHEN session_id IS NULL THEN 1 END) as null_session_count,
    MIN(timestamp) as earliest_event,
    MAX(timestamp) as latest_event
FROM raw_events
GROUP BY DATE(timestamp)
ORDER BY event_date DESC;

-- =============================================================================
-- Usage Examples
-- =============================================================================
/*
-- After ADF loads data into staging_events:
CALL sp_upsert_raw_events('ADF_2025_01_15');

-- Check data quality:
SELECT * FROM v_raw_events_quality ORDER BY event_date DESC LIMIT 7;

-- Check for duplicates before loading:
SELECT status, COUNT(*) FROM v_staging_duplicates GROUP BY status;
*/
