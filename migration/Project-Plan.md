# Search Analytics Migration - Project Plan

## Project Overview

| Item | Details |
|------|---------|
| **Project** | Migrate Search Analytics from manual Python/DuckDB processing to Azure PostgreSQL pipeline |
| **Current State** | Manual KQL export → Python script (1,045 lines) → DuckDB → Power BI |
| **Target State** | App Insights → Azure Data Factory → PostgreSQL → Power BI DirectQuery |
| **Data Volume** | ~600k searches/month, ~3M events/month, ~36M events/year |
| **Timeline** | 10-12 weeks (realistic estimate) |

---

## Phase 1: Approvals & Provisioning
**Duration: 2-4 weeks**

### 1.1 Internal Approvals
- [ ] Present migration plan to stakeholders
- [ ] Get budget approval for Azure PostgreSQL Flexible Server
- [ ] Get budget approval for Azure Data Factory
- [ ] Security review sign-off
- [ ] Data governance approval (GDPR compliance for user_id handling)

### 1.2 Azure Resource Requests
- [ ] Submit request for Azure PostgreSQL Flexible Server
  - **Recommended SKU**: Standard_D2s_v3 (2 vCores, 8GB RAM)
  - **Storage**: 128GB with auto-grow enabled
  - **Region**: Same as App Insights (reduce latency)
- [ ] Submit request for Azure Data Factory workspace
- [ ] Request networking configuration (VNet integration if required)
- [ ] Request Azure Key Vault for connection strings

### 1.3 Access Provisioning
- [ ] Service account for ADF to PostgreSQL
- [ ] Service account for ADF to App Insights (API access)
- [ ] Developer access to PostgreSQL for deployment
- [ ] Power BI service account for DirectQuery

### Milestone: All approvals received, resources provisioned
**Exit Criteria**: Can connect to empty PostgreSQL instance from local machine

---

## Phase 2: Infrastructure Setup
**Duration: 1-2 weeks**

### 2.1 PostgreSQL Configuration
- [ ] Enable required extensions
  ```sql
  CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
  CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- for text search
  ```
- [ ] Configure connection pooling (PgBouncer if needed)
- [ ] Set up maintenance window (Sunday 02:00-06:00 recommended)
- [ ] Configure backup retention (7 days minimum)
- [ ] Set up monitoring alerts
  - Storage > 80%
  - CPU > 80% sustained
  - Connection count > 80% of max

### 2.2 Azure Data Factory Setup
- [ ] Create Data Factory instance
- [ ] Create Linked Services:
  - [ ] App Insights (REST API)
  - [ ] PostgreSQL (connection string from Key Vault)
  - [ ] Blob Storage (for staging if needed)
- [ ] Create Integration Runtime (if VNet required)
- [ ] Set up Git integration for version control

### 2.3 Networking & Security
- [ ] Configure firewall rules for PostgreSQL
- [ ] Set up private endpoints (if required by policy)
- [ ] Enable SSL/TLS for all connections
- [ ] Store all secrets in Key Vault

### Milestone: Infrastructure ready for schema deployment
**Exit Criteria**: ADF can connect to both App Insights and PostgreSQL

---

## Phase 3: Schema Deployment & Configuration
**Duration: 3-5 days**

### 3.1 Deploy Database Schema
Execute scripts in order:

| Order | Script | Purpose | Validation |
|-------|--------|---------|------------|
| 1 | `2_create_schema.sql` | Tables, partitions, indexes | `\dt search_analytics.*` shows all tables |
| 2 | `3_load_raw_events.sql` | Staging procedures | `\df sp_*` shows procedures |
| 3 | `4_create_searches.sql` | Search enrichment | Procedure exists |
| 4 | `5_create_searches_journeys.sql` | Session aggregation | Procedure exists |
| 5 | `6_create_searches_daily.sql` | Daily metrics | Procedure exists |
| 6 | `7_create_searches_terms.sql` | Term analysis | Procedure exists |
| 7 | `8_create_reporting_views.sql` | Power BI views | `\dv rpt_*` shows views |
| 8 | `9_data_retention.sql` | Maintenance jobs | `sp_daily_maintenance` exists |

### 3.2 Validation Checklist
- [ ] All tables created in `search_analytics` schema
- [ ] Partitions exist for 2025, 2026, 2027
- [ ] All stored procedures compile without error
- [ ] All views are queryable (even if empty)
- [ ] Indexes created on all tables

### 3.3 Build ADF Pipelines

#### Pipeline 1: Daily Extract & Load
```
Trigger: Daily at 06:00 UTC
Steps:
1. Execute KQL query (1_extract_events.kql) for yesterday
2. Copy to staging table (raw_events_staging)
3. Call sp_upsert_raw_events()
4. Call sp_refresh_searches() or sp_update_searches_incremental()
5. Call sp_refresh_searches_journeys() or incremental
6. Call sp_refresh_searches_daily() or incremental
7. Call sp_refresh_searches_terms() or incremental
8. Log completion status
```

#### Pipeline 2: Weekly Maintenance
```
Trigger: Sunday 03:00 UTC
Steps:
1. Call sp_daily_maintenance()
2. VACUUM ANALYZE on all tables
3. Log completion status
```

### Milestone: Schema deployed, pipelines configured
**Exit Criteria**: Can manually trigger pipeline and see data flow through

---

## Phase 4: Testing & Refinement
**Duration: 1 week**

### 4.1 Unit Testing
- [ ] Test KQL extraction for single day
- [ ] Test staging → raw_events upsert (verify deduplication)
- [ ] Test sp_refresh_searches() output matches Python logic
- [ ] Test sp_refresh_searches_journeys() output
- [ ] Test sp_refresh_searches_daily() output
- [ ] Test sp_refresh_searches_terms() output

### 4.2 Validation Queries
Run after each procedure and compare to current DuckDB output:

```sql
-- Row counts should match within 1%
SELECT COUNT(*) FROM searches WHERE session_date = '2025-01-15';
SELECT COUNT(*) FROM searches_journeys WHERE session_date = '2025-01-15';
SELECT COUNT(*) FROM searches_daily WHERE session_date = '2025-01-15';

-- Key metrics should match
SELECT
    SUM(search_count) as total_searches,
    SUM(click_count) as total_clicks,
    ROUND(100.0 * SUM(click_count) / NULLIF(SUM(search_count), 0), 2) as ctr
FROM searches_daily
WHERE session_date = '2025-01-15';
```

### 4.3 Performance Testing
- [ ] Full refresh < 10 minutes for 30 days of data
- [ ] Incremental update < 2 minutes for single day
- [ ] Reporting views respond < 5 seconds for 90-day queries
- [ ] DirectQuery dashboard loads < 10 seconds

### 4.4 Issue Resolution
Document any discrepancies and resolve:
- [ ] SQL syntax differences (DuckDB → PostgreSQL)
- [ ] Data type mismatches
- [ ] Timing/rounding differences
- [ ] Missing edge case handling

### Milestone: Pipeline produces validated data
**Exit Criteria**: 7 consecutive days of matching output between old and new systems

---

## Phase 5: Historical Data Load
**Duration: 2-3 days**

### 5.1 Backfill Strategy
Load data in reverse chronological order:

| Period | Method | Estimated Duration |
|--------|--------|-------------------|
| Last 30 days | Full refresh procedures | 2-3 hours |
| 31-90 days | Batch load by week | 4-6 hours |
| 91-180 days | Batch load by month | 4-6 hours |
| 181+ days | Aggregate to daily only | 2-3 hours |

### 5.2 Backfill Execution
- [ ] Disable daily pipeline trigger
- [ ] Run historical KQL extracts (batch by month)
- [ ] Load each month sequentially
- [ ] Run sp_refresh_* for full dataset
- [ ] Run sp_aggregate_journeys_to_daily() for data > 180 days
- [ ] Verify row counts match expectations
- [ ] Re-enable daily pipeline trigger

### 5.3 Validation
- [ ] Total searches matches historical records
- [ ] Trend lines are continuous (no gaps)
- [ ] Date range covers expected period

### Milestone: All historical data loaded
**Exit Criteria**: PostgreSQL contains same date range as current system

---

## Phase 6: Parallel Run
**Duration: 1-2 weeks**

### 6.1 Setup
- [ ] Keep existing Python/DuckDB process running
- [ ] Run new PostgreSQL pipeline daily
- [ ] Create comparison dashboard showing both sources

### 6.2 Daily Validation
For each day, verify:
- [ ] Row counts match (±1% tolerance)
- [ ] Total searches match
- [ ] Total clicks match
- [ ] CTR matches (±0.5% tolerance)
- [ ] Journey type distribution matches
- [ ] Top 10 search terms match

### 6.3 Discrepancy Log
| Date | Metric | Old Value | New Value | Root Cause | Resolution |
|------|--------|-----------|-----------|------------|------------|
| | | | | | |

### 6.4 Sign-off
- [ ] 5+ consecutive days with no discrepancies
- [ ] Business stakeholder sign-off
- [ ] Technical lead sign-off

### Milestone: Systems produce identical results
**Exit Criteria**: Formal approval to cutover

---

## Phase 7: Power BI Cutover
**Duration: 2-3 days**

### 7.1 Pre-Cutover Checklist
- [ ] Final data validation complete
- [ ] PostgreSQL reporting views tested with Power BI Desktop
- [ ] DirectQuery performance acceptable
- [ ] Users notified of planned maintenance window

### 7.2 Cutover Steps
1. [ ] Export current Power BI report as backup
2. [ ] Update data source connections to PostgreSQL
3. [ ] Update any DAX measures that reference old column names
4. [ ] Test all report pages and visuals
5. [ ] Validate filter interactions work correctly
6. [ ] Test scheduled refresh (if using Import mode for some tables)
7. [ ] Publish updated report to Power BI Service
8. [ ] Verify report works in browser
9. [ ] Test mobile app if applicable

### 7.3 Rollback Plan
If critical issues discovered:
1. Revert Power BI to backup version
2. Re-enable Python/DuckDB pipeline
3. Document issues for resolution
4. Schedule new cutover window

### Milestone: Power BI running on PostgreSQL
**Exit Criteria**: All dashboards functional, users can access

---

## Phase 8: Decommission Old System
**Duration: 1 week**

### 8.1 Monitoring Period
- [ ] 5 business days with no critical issues
- [ ] User feedback collected
- [ ] Performance metrics within targets

### 8.2 Cleanup
- [ ] Disable Python scheduled task
- [ ] Archive Python scripts (don't delete yet)
- [ ] Archive DuckDB database file
- [ ] Document archive locations
- [ ] Update runbook/documentation

### 8.3 Final Documentation
- [ ] Update architecture diagrams
- [ ] Create operations runbook for PostgreSQL pipeline
- [ ] Document troubleshooting procedures
- [ ] Create escalation contacts

### Milestone: Migration complete
**Exit Criteria**: Old system archived, documentation updated

---

## Appendix A: Risk Register

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Approval delays | Schedule slip | High | Start approval process early, escalate blockers |
| PostgreSQL provisioning delays | Schedule slip | Medium | Have backup vendor/SKU options |
| Data discrepancies | Trust issues | Medium | Extensive parallel run period |
| Performance issues | User impact | Low | Right-size PostgreSQL, optimize queries |
| ADF pipeline failures | Data gaps | Low | Alerting, retry logic, manual backfill procedures |

---

## Appendix B: Contacts

| Role | Name | Responsibility |
|------|------|---------------|
| Project Lead | | Overall delivery |
| DBA | | PostgreSQL setup & optimization |
| Data Engineer | | ADF pipeline development |
| BI Developer | | Power BI migration |
| Business Owner | | Sign-off and UAT |

---

## Appendix C: Azure Resource Specifications

### PostgreSQL Flexible Server
- **SKU**: Standard_D2s_v3 (can scale up if needed)
- **Storage**: 128GB, auto-grow enabled
- **Backup**: 7-day retention, geo-redundant
- **High Availability**: Zone redundant (optional, adds cost)
- **Estimated Cost**: ~$150-200/month

### Azure Data Factory
- **Type**: Data Factory V2
- **Integration Runtime**: Azure (auto-resolve)
- **Estimated Cost**: ~$50-100/month (based on activity runs)

### Total Estimated Monthly Cost: ~$200-300/month

---

## Appendix D: Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Data freshness | T+1 day | Dashboard shows yesterday's data by 08:00 |
| Pipeline reliability | 99%+ | Successful runs / total runs |
| Query performance | <5 sec | 90th percentile response time |
| Data accuracy | 100% | Spot checks match source |
| Maintenance effort | <1 hr/week | Manual intervention time |

---

## Appendix E: File Reference

| File | Purpose |
|------|---------|
| `Migration-Plan.md` | Architecture and technical design |
| `Project-Plan.md` | This document - implementation checklist |
| `queries/1_extract_events.kql` | KQL query for App Insights |
| `queries/2_create_schema.sql` | Database schema with partitioning |
| `queries/3_load_raw_events.sql` | Staging and upsert procedures |
| `queries/4_create_searches.sql` | Event enrichment logic |
| `queries/5_create_searches_journeys.sql` | Session aggregation |
| `queries/6_create_searches_daily.sql` | Daily KPI aggregation |
| `queries/7_create_searches_terms.sql` | Search term analysis |
| `queries/8_create_reporting_views.sql` | Power BI optimized views |
| `queries/9_data_retention.sql` | Maintenance and retention procedures |
