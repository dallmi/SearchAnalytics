-- =============================================================================
-- 2_create_schema.sql
-- =============================================================================
-- Purpose: Create PostgreSQL schema and base tables for Search Analytics
-- Usage:   Run once during initial setup
--
-- Tables created:
--   - raw_events                    : Partitioned staging table for App Insights data
--   - searches                      : Enriched events with calculated columns
--   - searches_journeys             : Session-level aggregation (180-day retention)
--   - searches_journeys_daily_agg   : Daily aggregated journeys (3-year retention)
--   - searches_daily                : Daily KPI aggregation
--   - searches_terms                : Search term analysis
--
-- Partitioning Strategy:
--   - raw_events is partitioned by month for optimal query performance
--   - Partitions can be dropped/archived independently
--   - New partitions created automatically via maintenance procedure
-- =============================================================================

-- =============================================================================
-- Table: raw_events (Partitioned by Month)
-- =============================================================================
-- This table receives data directly from Azure Data Factory
-- Partitioned by timestamp for query performance and easy archival

DROP TABLE IF EXISTS raw_events CASCADE;

CREATE TABLE raw_events (
    -- Primary key columns
    timestamp           TIMESTAMP NOT NULL,
    name                VARCHAR(255) NOT NULL,
    user_id             VARCHAR(255),
    session_id          VARCHAR(255),

    -- Search context
    search_query        TEXT,
    cp_search_query     TEXT,
    query               TEXT,

    -- Result metrics
    cp_total_result_count   VARCHAR(50),
    total_result_count      VARCHAR(50),

    -- Tab/category context
    cp_tab              VARCHAR(100),
    tab                 VARCHAR(100),

    -- Click context (optional)
    cp_result_position  VARCHAR(50),
    cp_clicked_url      TEXT,

    -- Client info (optional)
    client_type         VARCHAR(100),
    client_os           VARCHAR(100),
    client_browser      VARCHAR(255),

    -- Metadata
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file         VARCHAR(255),

    -- Primary key for deduplication (includes timestamp for partitioning)
    PRIMARY KEY (timestamp, user_id, session_id, name)
) PARTITION BY RANGE (timestamp);

COMMENT ON TABLE raw_events IS 'Partitioned staging table for raw events from App Insights. Partitioned by month.';

-- =============================================================================
-- Create initial partitions (2025-2027)
-- Additional partitions created via sp_create_partitions_for_year()
-- =============================================================================

-- 2025 Partitions
CREATE TABLE raw_events_2025_01 PARTITION OF raw_events FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE raw_events_2025_02 PARTITION OF raw_events FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE raw_events_2025_03 PARTITION OF raw_events FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE raw_events_2025_04 PARTITION OF raw_events FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE raw_events_2025_05 PARTITION OF raw_events FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE raw_events_2025_06 PARTITION OF raw_events FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE raw_events_2025_07 PARTITION OF raw_events FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE raw_events_2025_08 PARTITION OF raw_events FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE raw_events_2025_09 PARTITION OF raw_events FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE raw_events_2025_10 PARTITION OF raw_events FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE raw_events_2025_11 PARTITION OF raw_events FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE raw_events_2025_12 PARTITION OF raw_events FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- 2026 Partitions
CREATE TABLE raw_events_2026_01 PARTITION OF raw_events FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE raw_events_2026_02 PARTITION OF raw_events FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE raw_events_2026_03 PARTITION OF raw_events FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE raw_events_2026_04 PARTITION OF raw_events FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE raw_events_2026_05 PARTITION OF raw_events FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE raw_events_2026_06 PARTITION OF raw_events FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE raw_events_2026_07 PARTITION OF raw_events FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE raw_events_2026_08 PARTITION OF raw_events FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE raw_events_2026_09 PARTITION OF raw_events FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE raw_events_2026_10 PARTITION OF raw_events FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE raw_events_2026_11 PARTITION OF raw_events FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE raw_events_2026_12 PARTITION OF raw_events FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

-- 2027 Partitions
CREATE TABLE raw_events_2027_01 PARTITION OF raw_events FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');
CREATE TABLE raw_events_2027_02 PARTITION OF raw_events FOR VALUES FROM ('2027-02-01') TO ('2027-03-01');
CREATE TABLE raw_events_2027_03 PARTITION OF raw_events FOR VALUES FROM ('2027-03-01') TO ('2027-04-01');
CREATE TABLE raw_events_2027_04 PARTITION OF raw_events FOR VALUES FROM ('2027-04-01') TO ('2027-05-01');
CREATE TABLE raw_events_2027_05 PARTITION OF raw_events FOR VALUES FROM ('2027-05-01') TO ('2027-06-01');
CREATE TABLE raw_events_2027_06 PARTITION OF raw_events FOR VALUES FROM ('2027-06-01') TO ('2027-07-01');
CREATE TABLE raw_events_2027_07 PARTITION OF raw_events FOR VALUES FROM ('2027-07-01') TO ('2027-08-01');
CREATE TABLE raw_events_2027_08 PARTITION OF raw_events FOR VALUES FROM ('2027-08-01') TO ('2027-09-01');
CREATE TABLE raw_events_2027_09 PARTITION OF raw_events FOR VALUES FROM ('2027-09-01') TO ('2027-10-01');
CREATE TABLE raw_events_2027_10 PARTITION OF raw_events FOR VALUES FROM ('2027-10-01') TO ('2027-11-01');
CREATE TABLE raw_events_2027_11 PARTITION OF raw_events FOR VALUES FROM ('2027-11-01') TO ('2027-12-01');
CREATE TABLE raw_events_2027_12 PARTITION OF raw_events FOR VALUES FROM ('2027-12-01') TO ('2028-01-01');

-- Create indexes on partitions (PostgreSQL creates these on each partition)
CREATE INDEX idx_raw_events_date ON raw_events (DATE(timestamp));
CREATE INDEX idx_raw_events_user ON raw_events (user_id);
CREATE INDEX idx_raw_events_session ON raw_events (session_id);
CREATE INDEX idx_raw_events_name ON raw_events (name);

-- =============================================================================
-- Procedure: Create partitions for a new year
-- Run this before each new year starts
-- =============================================================================
CREATE OR REPLACE PROCEDURE sp_create_partitions_for_year(p_year INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    v_month INTEGER;
    v_start_date DATE;
    v_end_date DATE;
    v_partition_name TEXT;
    v_sql TEXT;
BEGIN
    FOR v_month IN 1..12 LOOP
        v_start_date := MAKE_DATE(p_year, v_month, 1);
        v_end_date := v_start_date + INTERVAL '1 month';
        v_partition_name := FORMAT('raw_events_%s_%s', p_year, LPAD(v_month::TEXT, 2, '0'));

        -- Check if partition already exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_tables
            WHERE tablename = v_partition_name
        ) THEN
            v_sql := FORMAT(
                'CREATE TABLE %I PARTITION OF raw_events FOR VALUES FROM (%L) TO (%L)',
                v_partition_name, v_start_date, v_end_date
            );
            EXECUTE v_sql;
            RAISE NOTICE 'Created partition: %', v_partition_name;
        ELSE
            RAISE NOTICE 'Partition already exists: %', v_partition_name;
        END IF;
    END LOOP;
END;
$$;

COMMENT ON PROCEDURE sp_create_partitions_for_year IS 'Creates monthly partitions for raw_events table for a given year.';

-- =============================================================================
-- Table: searches (Enriched Events)
-- =============================================================================
-- Contains all events with calculated columns and window function results

DROP TABLE IF EXISTS searches CASCADE;

CREATE TABLE searches (
    -- Original columns
    timestamp               TIMESTAMP NOT NULL,
    name                    VARCHAR(255) NOT NULL,
    user_id                 VARCHAR(255),
    session_id              VARCHAR(255),

    -- Search context (consolidated)
    search_query            TEXT,
    cp_search_query         TEXT,
    query                   TEXT,

    -- Result metrics
    cp_total_result_count   INTEGER,

    -- Click context
    cp_tab                  VARCHAR(100),
    cp_result_position      INTEGER,
    cp_clicked_url          TEXT,

    -- Timestamp string for Power BI (preserves millisecond precision)
    timestamp_str           VARCHAR(30),
    -- CET timestamp (handles CET/CEST automatically)
    timestamp_cet           TIMESTAMP,
    timestamp_cet_str       VARCHAR(30),

    -- Session identification (CET-based)
    session_date            DATE NOT NULL,
    session_key             VARCHAR(600) NOT NULL,

    -- Event sequence (window function results)
    event_order             INTEGER,
    prev_event              VARCHAR(255),
    prev_timestamp          TIMESTAMP,
    ms_since_prev_event     BIGINT,
    sec_since_prev_event    NUMERIC(10,3),
    time_since_prev_bucket  VARCHAR(20),

    -- Timing reference (for SEARCH_TRIGGERED → RESULT calculation)
    last_search_started_ts  TIMESTAMP,

    -- Search term analysis
    search_term_normalized  TEXT,
    search_term_length      INTEGER,
    search_term_word_count  INTEGER,

    -- Time extraction (CET-based)
    event_hour              INTEGER,
    event_weekday           VARCHAR(10),
    event_weekday_num       INTEGER,

    -- Flags
    is_null_result          BOOLEAN,
    is_clickable_result     BOOLEAN,
    click_category          VARCHAR(20),
    is_success_click        BOOLEAN,
    is_first_search_of_day  BOOLEAN,

    -- Primary key
    PRIMARY KEY (timestamp, user_id, session_id, name)
);

-- Indexes for common query patterns
CREATE INDEX idx_searches_date ON searches (session_date);
CREATE INDEX idx_searches_session ON searches (session_key);
CREATE INDEX idx_searches_user ON searches (user_id);
CREATE INDEX idx_searches_name ON searches (name);
CREATE INDEX idx_searches_term ON searches (search_term_normalized);

COMMENT ON TABLE searches IS 'Enriched events with calculated columns, window functions, and business logic.';

-- =============================================================================
-- Table: searches_journeys (Session-Level - 180-Day Retention)
-- =============================================================================
-- One row per session with timing metrics and journey classification
-- RETENTION: Only last 180 days kept for full granularity

DROP TABLE IF EXISTS searches_journeys CASCADE;

CREATE TABLE searches_journeys (
    -- Session identification
    session_key             VARCHAR(600) PRIMARY KEY,
    session_date            DATE NOT NULL,
    user_id                 VARCHAR(255),
    session_start           TIMESTAMP,
    session_start_str       VARCHAR(30),

    -- Event counts
    total_events            INTEGER,
    search_count_in_session INTEGER,
    result_count            INTEGER,
    click_count             INTEGER,
    unique_search_terms     INTEGER,
    null_result_count       INTEGER,
    max_total_results       INTEGER,

    -- Timing metrics (in seconds)
    sec_search_to_result    NUMERIC(10,2),
    sec_result_to_click     NUMERIC(10,2),
    total_duration_sec      NUMERIC(10,2),

    -- Time of day
    first_event_hour        INTEGER,
    last_event_hour         INTEGER,

    -- Click breakdown
    result_clicks               INTEGER,
    trending_clicks             INTEGER,
    tab_clicks                  INTEGER,
    pagination_clicks           INTEGER,
    pagination_all_clicks       INTEGER,
    pagination_news_clicks      INTEGER,
    pagination_goto_clicks      INTEGER,
    filter_clicks               INTEGER,
    success_click_count         INTEGER,

    -- Flags
    includes_first_search_of_day    BOOLEAN,
    had_null_result                 BOOLEAN,
    recovered_from_null             BOOLEAN,
    had_reformulation               BOOLEAN,
    had_tab_switch                  BOOLEAN,
    is_users_first_session          BOOLEAN,

    -- Classifications
    journey_outcome         VARCHAR(20),
    session_complexity      VARCHAR(20),
    search_to_result_bucket VARCHAR(20),
    result_to_click_bucket  VARCHAR(20),
    session_duration_bucket VARCHAR(25),

    -- Sort order columns (for Power BI)
    journey_outcome_sort        INTEGER,
    session_complexity_sort     INTEGER,
    search_to_result_sort       INTEGER,
    result_to_click_sort        INTEGER,
    session_duration_sort       INTEGER,

    -- User cohort
    user_session_number     INTEGER,
    distinct_click_categories INTEGER
);

CREATE INDEX idx_journeys_date ON searches_journeys (session_date);
CREATE INDEX idx_journeys_user ON searches_journeys (user_id);
CREATE INDEX idx_journeys_outcome ON searches_journeys (journey_outcome);

COMMENT ON TABLE searches_journeys IS 'Session-level aggregation with 180-day retention. Use searches_journeys_daily_agg for historical trends.';

-- =============================================================================
-- Table: searches_journeys_daily_agg (Daily Aggregated - 3-Year Retention)
-- =============================================================================
-- One row per day - aggregated from searches_journeys before data is purged
-- RETENTION: 2 full prior years + current YTD (effectively ~3 years)

DROP TABLE IF EXISTS searches_journeys_daily_agg CASCADE;

CREATE TABLE searches_journeys_daily_agg (
    -- Date (primary key)
    session_date            DATE PRIMARY KEY,

    -- Volume metrics
    total_sessions          INTEGER,
    total_searches          INTEGER,
    total_results           INTEGER,
    total_clicks            INTEGER,
    total_null_results      INTEGER,

    -- Journey outcome counts
    sessions_success        INTEGER,
    sessions_engaged        INTEGER,
    sessions_abandoned      INTEGER,
    sessions_no_results     INTEGER,
    sessions_unknown        INTEGER,

    -- Pre-calculated rates
    success_rate_pct            NUMERIC(5,2),
    engaged_rate_pct            NUMERIC(5,2),
    abandonment_rate_pct        NUMERIC(5,2),
    no_results_rate_pct         NUMERIC(5,2),

    -- Session complexity distribution
    sessions_single_action      INTEGER,
    sessions_simple             INTEGER,
    sessions_medium             INTEGER,
    sessions_complex            INTEGER,

    -- Timing aggregates (for averages, use sum/count pattern)
    sum_sec_search_to_result    NUMERIC(12,2),
    sum_sec_result_to_click     NUMERIC(12,2),
    sum_total_duration_sec      NUMERIC(12,2),
    sessions_with_result_timing INTEGER,
    sessions_with_click_timing  INTEGER,

    -- Pre-calculated averages
    avg_sec_search_to_result    NUMERIC(10,2),
    avg_sec_result_to_click     NUMERIC(10,2),
    avg_session_duration_sec    NUMERIC(10,2),
    avg_searches_per_session    NUMERIC(5,2),

    -- Timing bucket distribution
    timing_bucket_lt_05s        INTEGER,  -- < 0.5s
    timing_bucket_05_1s         INTEGER,  -- 0.5-1s
    timing_bucket_1_2s          INTEGER,  -- 1-2s
    timing_bucket_2_5s          INTEGER,  -- 2-5s
    timing_bucket_gt_5s         INTEGER,  -- > 5s
    timing_bucket_no_result     INTEGER,  -- No result

    -- Click breakdown
    total_result_clicks         INTEGER,
    total_trending_clicks       INTEGER,
    total_tab_clicks            INTEGER,
    total_pagination_clicks     INTEGER,
    total_filter_clicks         INTEGER,
    total_success_clicks        INTEGER,

    -- Behavioral flags aggregates
    sessions_with_reformulation INTEGER,
    sessions_with_null_result   INTEGER,
    sessions_recovered_from_null INTEGER,
    sessions_first_time_users   INTEGER,

    -- Hour distribution (sessions starting in each period)
    sessions_morning            INTEGER,  -- 6-12
    sessions_afternoon          INTEGER,  -- 12-18
    sessions_evening            INTEGER,  -- 18-24
    sessions_night              INTEGER   -- 0-6
);

COMMENT ON TABLE searches_journeys_daily_agg IS 'Daily aggregated journey metrics. 3-year retention for trend analysis.';

-- =============================================================================
-- Table: searches_daily (Daily KPI Aggregation)
-- =============================================================================
-- One row per day with aggregated metrics

DROP TABLE IF EXISTS searches_daily CASCADE;

CREATE TABLE searches_daily (
    -- Date
    date                    DATE PRIMARY KEY,

    -- Volume metrics
    total_events            INTEGER,
    unique_sessions         INTEGER,
    unique_users            INTEGER,
    unique_search_terms     INTEGER,

    -- Event counts
    search_starts           INTEGER,
    result_events           INTEGER,
    click_events            INTEGER,
    null_results            INTEGER,
    result_events_with_results INTEGER,

    -- Session-based metrics
    sessions_with_results   INTEGER,
    sessions_with_clicks    INTEGER,
    sessions_abandoned      INTEGER,

    -- Rate metrics (percentages)
    click_rate_pct              NUMERIC(5,2),
    null_rate_pct               NUMERIC(5,2),
    session_success_rate_pct    NUMERIC(5,2),
    session_abandonment_rate_pct NUMERIC(5,2),

    -- Averages
    avg_searches_per_session    NUMERIC(5,2),
    avg_search_term_length      NUMERIC(5,1),
    avg_search_term_words       NUMERIC(5,1),

    -- For weighted DAX calculations
    sum_search_term_length      INTEGER,
    sum_search_term_words       INTEGER,
    search_term_count           INTEGER,
    first_searches_of_day       INTEGER,

    -- Click breakdown
    clicks_result               INTEGER,
    clicks_trending             INTEGER,
    clicks_tab                  INTEGER,
    clicks_pagination           INTEGER,
    clicks_pagination_all       INTEGER,
    clicks_pagination_news      INTEGER,
    clicks_pagination_goto      INTEGER,
    clicks_filter               INTEGER,
    success_clicks              INTEGER,

    -- Temporal
    day_of_week             VARCHAR(10),
    day_of_week_num         INTEGER,

    -- Time distribution
    searches_morning        INTEGER,
    searches_afternoon      INTEGER,
    searches_evening        INTEGER,
    searches_night          INTEGER,

    -- User cohorts
    new_users               INTEGER,
    returning_users         INTEGER
);

COMMENT ON TABLE searches_daily IS 'Daily aggregated KPIs for dashboard overview.';

-- =============================================================================
-- Table: searches_terms (Search Term Analysis)
-- =============================================================================
-- One row per date + search term combination

DROP TABLE IF EXISTS searches_terms CASCADE;

CREATE TABLE searches_terms (
    -- Composite key
    session_date            DATE NOT NULL,
    search_term             TEXT NOT NULL,

    -- Term characteristics
    word_count              INTEGER,

    -- Volume metrics
    search_count            INTEGER,
    unique_users            INTEGER,
    unique_sessions         INTEGER,

    -- Result metrics
    result_events           INTEGER,
    null_result_count       INTEGER,

    -- Click metrics
    click_count                 INTEGER,
    clicks_result               INTEGER,
    clicks_trending             INTEGER,
    clicks_tab                  INTEGER,
    clicks_pagination           INTEGER,
    clicks_pagination_all       INTEGER,
    clicks_pagination_news      INTEGER,
    clicks_pagination_goto      INTEGER,
    clicks_filter               INTEGER,
    success_click_count         INTEGER,

    -- Timing
    avg_sec_to_click        NUMERIC(10,2),
    clicks_with_timing      INTEGER,
    sum_sec_to_click        NUMERIC(12,2),

    -- Time distribution
    searches_morning        INTEGER,
    searches_afternoon      INTEGER,
    searches_evening        INTEGER,
    searches_night          INTEGER,

    -- Trend detection
    first_seen_date         DATE,
    is_new_term             BOOLEAN,

    -- Primary key
    PRIMARY KEY (session_date, search_term)
);

CREATE INDEX idx_terms_date ON searches_terms (session_date);
CREATE INDEX idx_terms_term ON searches_terms (search_term);
CREATE INDEX idx_terms_count ON searches_terms (search_count DESC);

COMMENT ON TABLE searches_terms IS 'Search term analysis with click-through attribution.';

-- =============================================================================
-- Table: data_retention_log (Audit trail for retention operations)
-- =============================================================================
DROP TABLE IF EXISTS data_retention_log CASCADE;

CREATE TABLE data_retention_log (
    id                  SERIAL PRIMARY KEY,
    operation_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name          VARCHAR(100),
    operation_type      VARCHAR(50),  -- 'DELETE', 'ARCHIVE', 'AGGREGATE'
    date_range_start    DATE,
    date_range_end      DATE,
    rows_affected       INTEGER,
    notes               TEXT
);

COMMENT ON TABLE data_retention_log IS 'Audit log for data retention operations.';

-- =============================================================================
-- Verification
-- =============================================================================
SELECT 'Schema created successfully. Tables:' as status;
SELECT table_name,
       CASE
           WHEN table_name = 'raw_events' THEN 'Partitioned by month'
           WHEN table_name = 'searches_journeys' THEN '180-day retention'
           WHEN table_name = 'searches_journeys_daily_agg' THEN '3-year retention'
           ELSE 'Standard'
       END as notes
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('raw_events', 'searches', 'searches_journeys',
                     'searches_journeys_daily_agg', 'searches_daily', 'searches_terms')
ORDER BY table_name;

-- List partitions
SELECT 'Partitions created:' as status;
SELECT tablename
FROM pg_tables
WHERE tablename LIKE 'raw_events_20%'
ORDER BY tablename;
