# Search Analytics Migration Plan

## From: Manual Local Processing → To: Azure PostgreSQL Pipeline

This document outlines the recommended architecture for migrating from the current manual processing workflow to an automated Azure-based pipeline.

---

## Executive Summary

| Aspect | Current State | Target State |
|--------|---------------|--------------|
| **Data Source** | Manual KQL export from App Insights | Automated extraction via Azure Data Factory |
| **Processing** | Local Python + DuckDB | PostgreSQL with scheduled SQL transformations |
| **Storage** | Parquet files | PostgreSQL tables |
| **BI Connection** | Power BI imports Parquet files | Power BI DirectQuery to PostgreSQL |
| **Refresh Frequency** | Manual (weekly) | Automated (daily) |

---

## Volume Considerations

### Expected Data Volume

| Metric | Monthly | Yearly |
|--------|---------|--------|
| Searches | ~600,000 | ~7.2 million |
| Events per search | ~5 (started, completed, result, clicks) | - |
| Total events | ~3 million | ~36 million |
| Estimated raw data size | ~500 MB | ~6 GB |

### Why PostgreSQL is Sufficient

PostgreSQL easily handles this volume for the following reasons:

1. **Query Performance**: 36M rows is well within PostgreSQL's capabilities with proper indexing
2. **Cost Efficiency**: ~€50-100/month vs. Databricks at ~€300+/month
3. **Simplicity**: No Spark/distributed computing overhead
4. **SQL Compatibility**: Your existing DuckDB queries are 95% compatible with PostgreSQL
5. **Power BI Integration**: Native DirectQuery support

### When to Consider Databricks Instead

| Scenario | Recommendation |
|----------|----------------|
| < 100 million events/year | PostgreSQL |
| 100M - 1B events/year | PostgreSQL with partitioning, or Databricks |
| > 1 billion events/year | Databricks |
| Need ML/AI on search data | Databricks |
| Real-time streaming required | Databricks or Event Hubs + Functions |

---

## Data Retention & Partitioning Strategy

### Partitioning (raw_events)

The `raw_events` table is **partitioned by month** to ensure:
- Fast queries (only relevant months scanned)
- Easy archival (drop old partitions instantly)
- Efficient maintenance (VACUUM runs per-partition)

```
raw_events (partitioned)
├── raw_events_2025_01  (~3M rows)
├── raw_events_2025_02  (~3M rows)
├── ...
└── raw_events_2027_12  (~3M rows)
```

**Maintenance**: Run `CALL sp_create_partitions_for_year(2028)` before each new year.

### Retention Policies

| Table | Retention | Rows (3 years) | Purpose |
|-------|-----------|----------------|---------|
| `raw_events` | Partitioned, archive as needed | ~108M | Raw event storage |
| `searches` | Matches raw_events | ~108M | Enriched events |
| `searches_journeys` | **180 days** | ~1.2M | Full session detail |
| `searches_journeys_daily_agg` | **2 prior years + YTD** | ~1,100 | Historical trends |
| `searches_daily` | Unlimited | ~1,100 | Daily KPIs |
| `searches_terms` | Unlimited | ~2M | Term analysis |

### Power BI Data Sources

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Power BI Reports                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────┐    ┌─────────────────────────────────────┐│
│  │   RECENT DATA (180 days)    │    │   HISTORICAL DATA (3 years)         ││
│  │   Full session granularity  │    │   Daily aggregated                  ││
│  ├─────────────────────────────┤    ├─────────────────────────────────────┤│
│  │ • rpt_searches_journeys     │    │ • rpt_journeys_trend (unified)      ││
│  │ • rpt_journey_types         │    │ • rpt_journeys_daily_agg            ││
│  │   (~1.2M rows)              │    │   (~1,100 rows)                     ││
│  └─────────────────────────────┘    └─────────────────────────────────────┘│
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │   ALWAYS AVAILABLE (full history)                                       ││
│  │   • rpt_searches_daily      • rpt_searches_terms                        ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
```

### Daily Maintenance Pipeline

Run daily after data load:

```sql
-- 1. Load new data via ADF
CALL sp_upsert_raw_events();

-- 2. Transform and aggregate
CALL sp_update_all_incremental(CURRENT_DATE - 1);

-- 3. Aggregate old journeys + apply retention
CALL sp_daily_maintenance();
```

The `sp_daily_maintenance()` procedure:
1. Aggregates any new journey dates to `searches_journeys_daily_agg`
2. Purges `searches_journeys` data older than 180 days
3. Purges `searches_journeys_daily_agg` data older than 3 years

---

## Architecture Comparison

### Current Architecture (Manual)

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   App Insights  │────▶│  Manual KQL     │────▶│  Python Script  │────▶│  Parquet Files  │
│   (raw events)  │     │  Export (CSV)   │     │  + DuckDB       │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                                                  │
                                                                         ┌────────▼────────┐
                                                                         │    Power BI     │
                                                                         │  (Import Mode)  │
                                                                         └─────────────────┘
```

**Drawbacks:**
- Manual intervention required weekly
- No version control on data
- Local processing bottleneck
- Parquet files need manual distribution

### Target Architecture (Automated)

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────────────────┐
│   App Insights  │────▶│  Azure Data     │────▶│           PostgreSQL                │
│   (raw events)  │     │  Factory        │     │                                     │
└─────────────────┘     │  (scheduled)    │     │  ┌─────────────────────────────┐    │
                        └─────────────────┘     │  │ raw_events (staging)        │    │
                                                │  └──────────────┬──────────────┘    │
                                                │                 │ SQL Transform     │
                                                │  ┌──────────────▼──────────────┐    │
                                                │  │ searches (enriched events)  │    │
                                                │  └──────────────┬──────────────┘    │
                                                │                 │ Aggregation       │
                                                │  ┌──────────────▼──────────────┐    │
                                                │  │ Reporting Tables:           │    │
                                                │  │ • searches_journeys         │    │
                                                │  │ • searches_daily            │    │
                                                │  │ • searches_terms            │    │
                                                │  └──────────────┬──────────────┘    │
                                                └─────────────────┼───────────────────┘
                                                                  │
                                                         ┌────────▼────────┐
                                                         │    Power BI     │
                                                         │ (DirectQuery)   │
                                                         └─────────────────┘
```

**Benefits:**
- Fully automated daily refresh
- Version-controlled SQL transformations
- Scalable cloud infrastructure
- Real-time Power BI dashboards (with DirectQuery)
- Historical data preserved

---

## Data Flow: Step by Step

### Step 1: Extract from App Insights

**Tool:** Azure Data Factory with KQL Query Activity

**Frequency:** Daily (recommended) or configurable

**Process:**
1. ADF triggers on schedule (e.g., 6:00 AM UTC)
2. Executes KQL query against App Insights
3. Writes results to PostgreSQL `raw_events` staging table

```
┌─────────────────┐                    ┌─────────────────┐
│   App Insights  │  ──── KQL ────▶    │  raw_events     │
│   (last 24h)    │                    │  (PostgreSQL)   │
└─────────────────┘                    └─────────────────┘
```

### Step 2: Transform Raw Events to Searches

**Tool:** PostgreSQL SQL (can be wrapped in dbt model)

**Process:**
1. Normalize event names (UPPER case)
2. Create session identifiers
3. Calculate window functions (prev_event, time intervals)
4. Add business logic columns (is_null_result, click_category)

```
┌─────────────────┐                    ┌─────────────────┐
│   raw_events    │  ──── SQL ────▶    │    searches     │
│   (staging)     │                    │   (enriched)    │
└─────────────────┘                    └─────────────────┘

Transformations:
• UPPER(name) for case-insensitive matching
• session_key = date || user_id || session_id
• LAG() for prev_event, prev_timestamp
• LAST_VALUE() for last_search_started_ts
• CASE WHEN for click_category, is_null_result
```

### Step 3: Aggregate to Session Level (Journeys)

**Tool:** PostgreSQL SQL

**Process:**
1. Group by session_key
2. Calculate timing metrics (search_to_result, result_to_click)
3. Classify journey outcomes (Success, Abandoned, No Results)
4. Create time buckets for analysis

```
┌─────────────────┐                    ┌─────────────────────┐
│    searches     │  ──── SQL ────▶    │  searches_journeys  │
│   (enriched)    │                    │  (session-level)    │
└─────────────────┘                    └─────────────────────┘

Aggregations:
• COUNT events per session
• MIN/MAX timestamps for duration
• Timing: SEARCH_STARTED → SEARCH_RESULT_COUNT
• Journey outcome classification
```

### Step 4: Aggregate to Daily Level

**Tool:** PostgreSQL SQL

**Process:**
1. Group by date
2. Calculate daily KPIs (success rate, null rate, click rate)
3. Create cohort metrics (new vs returning users)

```
┌─────────────────┐                    ┌─────────────────────┐
│    searches     │  ──── SQL ────▶    │   searches_daily    │
│   (enriched)    │                    │   (daily KPIs)      │
└─────────────────┘                    └─────────────────────┘
```

### Step 5: Aggregate Search Terms

**Tool:** PostgreSQL SQL

**Process:**
1. Group by date + search_term
2. Calculate term-level metrics (searches, clicks, null rate)
3. Track term trends (first_seen_date, is_new_term)

```
┌─────────────────┐                    ┌─────────────────────┐
│    searches     │  ──── SQL ────▶    │   searches_terms    │
│   (enriched)    │                    │  (term analysis)    │
└─────────────────┘                    └─────────────────────┘
```

---

## Query Sequence Overview

| Sequence | File | Purpose | Frequency |
|----------|------|---------|-----------|
| 1 | `1_extract_events.kql` | Extract raw events from App Insights | Daily (ADF) |
| 2 | `2_create_schema.sql` | Create PostgreSQL schema and tables (partitioned) | Once (setup) |
| 3 | `3_load_raw_events.sql` | Insert/upsert raw events from staging | Daily |
| 4 | `4_create_searches.sql` | Transform to enriched searches table | Daily |
| 5 | `5_create_searches_journeys.sql` | Aggregate to session level | Daily |
| 6 | `6_create_searches_daily.sql` | Aggregate to daily KPIs | Daily |
| 7 | `7_create_searches_terms.sql` | Aggregate search terms | Daily |
| 8 | `8_create_reporting_views.sql` | Create Power BI optimized views | Once (setup) |
| 9 | `9_data_retention.sql` | Data retention & maintenance procedures | Daily (after load) |

---

## Transformation Logic Details

### Session Identification

A session is identified by combining:
```sql
session_key = session_date || '_' || user_id || '_' || session_id
```

This ensures unique session identification across days even if session IDs repeat.

### Event Sequence and Timing

Events within a session follow this typical sequence:
```
SEARCH_STARTED → SEARCH_COMPLETED → SEARCH_RESULT_COUNT → [CLICK]
     │                                      │                 │
     └──────── User-perceived latency ──────┘                 │
                                            │                 │
                                            └── Decision time ┘
```

**Key timing calculation:**
- `ms_search_to_result`: Time from SEARCH_STARTED to SEARCH_RESULT_COUNT
- `ms_result_to_click`: Time from SEARCH_RESULT_COUNT to click event

### Window Functions Used

| Function | Purpose |
|----------|---------|
| `ROW_NUMBER()` | Event ordering within session |
| `LAG()` | Previous event name and timestamp |
| `LAST_VALUE(...IGNORE NULLS)` | Carry forward last SEARCH_STARTED timestamp |

### Journey Outcome Classification

| Outcome | Condition |
|---------|-----------|
| **Success** | `click_count > 0` |
| **Abandoned** | `result_count > 0 AND click_count = 0 AND null_result_count < result_count` |
| **No Results** | `result_count > 0 AND null_result_count = result_count` |
| **Unknown** | Other cases |

### Click Categories

| Event Name | Category |
|------------|----------|
| `SEARCH_TAB_CLICK` | General |
| `SEARCH_ALL_TAB_PAGE_CLICK` | All |
| `SEARCH_NEWS_TAB_PAGE_CLICK` | News |
| `SEARCH_GOTO_TAB_PAGE_CLICK` | GoTo |
| Events containing `PEOPLE` | People |

---

## PostgreSQL vs Databricks: Decision Matrix

| Factor | PostgreSQL | Databricks |
|--------|------------|------------|
| **Setup Complexity** | Low (managed service) | Medium (clusters, notebooks) |
| **Monthly Cost** | €50-100 | €300-500+ |
| **Query Language** | SQL (familiar) | SQL + Python/Scala |
| **Window Functions** | Full support | Full support |
| **Power BI Integration** | Native DirectQuery | Via Databricks SQL endpoint |
| **Scaling** | Vertical (upgrade instance) | Horizontal (add nodes) |
| **Learning Curve** | Minimal | Moderate |
| **Your Volume (36M/year)** | ✅ Comfortable | Overkill |

### Recommendation

**Start with PostgreSQL.** Given your volume (~36M events/year), PostgreSQL is:
- Sufficient for performance
- More cost-effective
- Simpler to maintain
- Your SQL knowledge transfers directly

If you later need:
- Machine learning on search data
- Real-time streaming analytics
- Volume exceeds 100M+ events/year

Then consider migrating to Databricks.

---

## Implementation Roadmap

### Phase 1: PostgreSQL Setup (Week 1)

1. Provision Azure Database for PostgreSQL
2. Run schema creation scripts
3. Test connectivity from local machine
4. Import historical data (optional)

### Phase 2: Azure Data Factory Setup (Week 2)

1. Create ADF instance
2. Create linked services (App Insights, PostgreSQL)
3. Create pipeline with KQL query activity
4. Schedule daily trigger

### Phase 3: Transformation Automation (Week 3)

1. Create stored procedures for transformations
2. Add to ADF pipeline (or use dbt)
3. Test end-to-end flow
4. Verify data quality

### Phase 4: Power BI Migration (Week 4)

1. Create PostgreSQL data source in Power BI
2. Update existing reports to use new tables
3. Test DirectQuery performance
4. Switch to production

### Phase 5: Decommission Old Process

1. Run parallel for 2 weeks
2. Validate data matches
3. Document new process
4. Archive old scripts

---

## Files Included in This Migration Package

```
migration/
├── Migration-Plan.md                    # This document
└── queries/
    ├── 1_extract_events.kql             # KQL query for App Insights
    ├── 2_create_schema.sql              # PostgreSQL schema (partitioned tables)
    ├── 3_load_raw_events.sql            # Staging table load procedure
    ├── 4_create_searches.sql            # Event enrichment transformation
    ├── 5_create_searches_journeys.sql   # Session-level aggregation
    ├── 6_create_searches_daily.sql      # Daily KPI aggregation
    ├── 7_create_searches_terms.sql      # Search term analysis
    ├── 8_create_reporting_views.sql     # Power BI optimized views
    └── 9_data_retention.sql             # Data retention & maintenance procedures
```

---

## Appendix: DuckDB to PostgreSQL Syntax Changes

| DuckDB | PostgreSQL | Notes |
|--------|------------|-------|
| `STRFTIME(ts, '%Y-%m-%d')` | `TO_CHAR(ts, 'YYYY-MM-DD')` | Date formatting |
| `DATEDIFF('millisecond', a, b)` | `EXTRACT(EPOCH FROM (b - a)) * 1000` | Time difference |
| `DATE_TRUNC('day', ts)::DATE` | `DATE_TRUNC('day', ts)::DATE` | Same |
| `ISODOW(ts)` | `EXTRACT(ISODOW FROM ts)` | Day of week |
| `DAYNAME(ts)` | `TO_CHAR(ts, 'Day')` | Day name |
| `r.* EXCLUDE(name)` | Explicit column list | No EXCLUDE in PostgreSQL |
| `COPY TO 'file.parquet'` | Not needed | Data stays in PostgreSQL |

---

## Contact & Support

For questions about this migration plan, consult with your Azure/DevOps team or database administrator.
