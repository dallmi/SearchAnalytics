# Business Requirements Document
## Intranet Search Analytics — Tactical Solution

**Document Version:** 1.0
**Date:** January 6, 2026
**Status:** Draft
**Classification:** Internal Use Only

---

## Document Control

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-06 | [Author Name] | Initial Draft |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Context](#2-business-context)
3. [Project Objectives](#3-project-objectives)
4. [Scope Definition](#4-scope-definition)
5. [Solution Architecture](#5-solution-architecture)
6. [Data Requirements](#6-data-requirements)
7. [Process Flows](#7-process-flows)
8. [Key Performance Indicators](#8-key-performance-indicators)
9. [Stakeholders & Responsibilities](#9-stakeholders--responsibilities)
10. [Assumptions & Constraints](#10-assumptions--constraints)
11. [Risks & Mitigation](#11-risks--mitigation)
12. [Success Criteria](#12-success-criteria)
13. [Future State Roadmap](#13-future-state-roadmap)
14. [Appendix](#14-appendix)

---

## 1. Executive Summary

### 1.1 Purpose

This document defines the business requirements for implementing a **tactical analytics solution** for Intranet Search. The solution will provide senior management with actionable insights into search behavior, performance, and user engagement patterns across the organization.

### 1.2 Background

The organization requires visibility into how employees interact with the Intranet search functionality. Understanding search patterns enables:

- Identification of information gaps and content needs
- Optimization of search relevance and user experience
- Data-driven decisions for content governance
- Measurement of search platform effectiveness

### 1.3 Approach

Given resource constraints and the need for rapid time-to-value, a **tactical solution** will be implemented as Phase 1:

#### Implementation Approach Comparison

| Aspect | Phase 1: Tactical Solution | Phase 2: Strategic Solution |
|--------|---------------------------|----------------------------|
| **Status** | **Current Focus** | Future State |
| Data Extraction | Manual | Automated pipeline |
| Data Source | AppInsights | AppInsights + Click-stream |
| Processing | Flat file (CSV/Excel) | Data Lake integration |
| Visualization | PowerBI dashboards | GMDP platform |
| Refresh | Periodic (weekly) | Real-time |
| Analytics | Core KPIs | Extended KPIs with click data |

---

## 2. Business Context

### 2.1 Current State

- Search telemetry data exists within **Azure Application Insights**
- No consolidated reporting or analytics capability currently available
- Limited visibility into search effectiveness and user behavior
- Manual, ad-hoc analysis performed on request basis only

### 2.2 Problem Statement

Senior management lacks systematic visibility into Intranet search usage patterns, making it difficult to:

1. Understand what employees are searching for
2. Identify content gaps and unmet information needs
3. Measure search quality and relevance
4. Track adoption trends across departments and regions
5. Prioritize content and search improvements

### 2.3 Business Drivers

| Driver | Description |
|--------|-------------|
| **Operational Efficiency** | Reduce time employees spend searching for information |
| **Content Strategy** | Align content creation with actual user needs |
| **User Experience** | Improve search relevance and satisfaction |
| **Investment Justification** | Demonstrate value of search platform investments |

---

## 3. Project Objectives

### 3.1 Primary Objectives

| # | Objective | Success Measure | Data Source |
|---|-----------|-----------------|-------------|
| O1 | Establish baseline search analytics capability | Dashboard operational and accessible to stakeholders | AppInsights |
| O2 | Provide visibility into search query patterns | Top 100 queries identified and categorized | AppInsights |
| O3 | Track search performance metrics | Latency, error rates, and zero-result rates monitored | AppInsights |
| O4 | Monitor adoption across departments and devices | Usage breakdown by department, location, device available | AppInsights |

### 3.2 Tactical Solution Limitations

**Important:** Click-through data (which results users click, engagement with content) resides in **GMDP** and is **out of scope** for the tactical solution. This means:

| Metric Type | Phase 1 (Tactical) | Phase 2 (With GMDP) |
|-------------|-------------------|---------------------|
| Search volume & frequency | Available | Available |
| Query terms & patterns | Available | Available |
| Zero-result queries | Available | Available |
| Search latency & errors | Available | Available |
| User/department/device info | Available | Available |
| **Click-through rate** | **Not Available** | Available |
| **Content engagement** | **Not Available** | Available |
| **Result position effectiveness** | **Not Available** | Available |

### 3.3 Business Questions to Answer

The tactical solution enables stakeholders to answer questions **based on AppInsights data only**:

| Category | Answerable in Phase 1 | Deferred to Phase 2 (Requires GMDP) |
|----------|----------------------|-------------------------------------|
| **Usage & Adoption** | How many searches per day/week? Which departments search most? What devices are used? What are peak usage times? | — |
| **Query Analysis** | What are users searching for? Which queries return no results? How complex are queries? What languages are used? | — |
| **Performance** | How fast are search results? What is the error rate? Are there performance issues at specific times? | — |
| **Trends** | How is search volume trending? Are there seasonal patterns? Which departments show growth/decline? | — |
| **Content & Relevance** | — | What content is most clicked? Are users finding what they need? Which results perform best? |
| **User Success** | — | Do users engage with results? What is the click-through rate? How long do users spend on results? |

---

## 4. Scope Definition

### 4.1 In Scope — Tactical Solution (Phase 1)

| Category | Included Items |
|----------|----------------|
| **Data Source** | Azure Application Insights (AppInsights) |
| **Extraction Method** | Manual export (scheduled periodic extraction) |
| **Data Format** | Flat file (CSV/Excel) |
| **Visualization** | PowerBI dashboards and reports |
| **Distribution** | PowerBI Service publication to senior management |
| **Refresh Frequency** | Weekly or bi-weekly manual refresh |

### 4.2 Out of Scope — Tactical Solution

| Item | Rationale | Data Location | Future Phase |
|------|-----------|---------------|--------------|
| Click-through data & engagement metrics | Click data resides in GMDP, not AppInsights | **GMDP** | Phase 2 |
| GMDP platform integration | Resource and timeline constraints | GMDP | Phase 2 |
| Real-time data streaming | Requires automated pipeline infrastructure | — | Phase 2 |
| Automated alerting | Requires pipeline automation | — | Phase 2 |

**Note:** Click-through rate (CTR), content engagement, and result effectiveness metrics require integration with GMDP where click-stream data is stored. These metrics are explicitly deferred to Phase 2.

### 4.3 Scope Boundaries

#### Phase 1 — In Scope (Tactical Solution)

| Step | Component | Description |
|------|-----------|-------------|
| 1 | AppInsights | Manual data export via KQL queries |
| 2 | Flat File | CSV/Excel intermediate storage |
| 3 | PowerBI | Dashboard creation and publication |

#### Phase 2 — Out of Scope (Future State)

| Step | Component | Description |
|------|-----------|-------------|
| 1 | Automated Pipeline | Scheduled data extraction |
| 2 | Data Lake + Clicks | Integrated storage with click-stream |
| 3 | GMDP Integration | Click data and enterprise platform integration |

---

## 5. Solution Architecture

### 5.1 Tactical Solution Architecture

The tactical solution follows a three-layer architecture:

| Layer | Component | Technology | Responsibility |
|-------|-----------|------------|----------------|
| **Data Source** | Application Insights | Azure AppInsights | Stores raw search telemetry events |
| **Data Processing** | Analyst Workstation | KQL / Excel / Power Query | Export, transform, and clean data |
| **Consumption** | PowerBI Service | PowerBI Desktop & Service | Dashboards, reports, scheduled refresh |
| **End Users** | Senior Management | Web Browser | View dashboards, export reports, decision support |

### 5.2 Component Description

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Data Source** | Azure Application Insights | Stores raw search telemetry events |
| **Extraction** | Kusto Query Language (KQL) / Portal Export | Extract relevant data fields |
| **Processing** | Excel / Power Query | Data cleansing, transformation, enrichment |
| **Storage** | SharePoint / Network Drive | Flat file storage (CSV/Excel) |
| **Visualization** | PowerBI Desktop & Service | Dashboard creation and publication |

### 5.3 Data Flow

| Step | Action | Input | Output | Tool |
|------|--------|-------|--------|------|
| 1 | Execute KQL Query | Query parameters | Raw telemetry data | AppInsights Portal |
| 2 | Export Results | Query results | CSV/Excel file | AppInsights Export |
| 3 | Process & Clean | Raw export file | Cleaned flat file | Excel / Power Query |
| 4 | Refresh PowerBI | Flat file | Updated dashboard | PowerBI Desktop |
| 5 | Publish | Local dashboard | Published report | PowerBI Service |

---

## 6. Data Requirements

### 6.1 Data Model Overview

The tactical solution captures and organizes data across logical groupings. **Phase 1** uses AppInsights data only; **Phase 2** will add GMDP click data:

| Grouping | Key Fields | Purpose | Phase |
|----------|------------|---------|-------|
| **User & Context** | User ID, Department, Location, Role | Identify who is searching | Phase 1 |
| **Query Details** | Search Terms, Language, Length, Type | Understand what they search for | Phase 1 |
| **Session Metadata** | Timestamp, Session ID, Device, Browser | Track when and how they search | Phase 1 |
| **Results Count** | Number of results returned | Measure result availability | Phase 1 |
| **Search Performance** | Latency, Errors, Suggestions | Monitor system health | Phase 1 |
| **Query Outcome** | User Feedback, Query Reformulation | Assess search quality | Phase 1 |
| **Click & Interaction** | Clicked Results, Click Position, Time on Result | Measure engagement depth | **Phase 2 (GMDP)** |
| **Abandonment** | Session abandoned without click | Assess search success | **Phase 2 (GMDP)** |

### 6.2 Detailed Field Specifications

#### 6.2.1 User & Context

| Field Name | Data Type | Description | Example | Priority |
|------------|-----------|-------------|---------|----------|
| `user_id` | String | Anonymized user identifier | `USR_A1B2C3` | Required |
| `department` | String | User's organizational department | `Finance`, `HR`, `IT` | Required |
| `location` | String | User's office location / country | `DE-Munich`, `US-NYC` | Required |
| `role` | String | User's job title or role category | `Manager`, `Analyst` | Optional |

#### 6.2.2 Query Details

| Field Name | Data Type | Description | Example | Priority |
|------------|-----------|-------------|---------|----------|
| `search_terms` | String | The query text entered by user | `expense report form` | Required |
| `query_language` | String | Detected language of query | `EN`, `DE`, `FR` | Optional |
| `query_length_words` | Integer | Number of words in query | `3` | Required |
| `query_length_chars` | Integer | Number of characters in query | `19` | Required |
| `query_type` | String | Classification of query type | `keyword`, `natural_language`, `filtered` | Optional |

#### 6.2.3 Session Metadata

| Field Name | Data Type | Description | Example | Priority |
|------------|-----------|-------------|---------|----------|
| `timestamp` | DateTime | Date and time of search event | `2026-01-06 14:32:15` | Required |
| `session_id` | String | Unique session identifier | `SES_X9Y8Z7` | Required |
| `device_type` | String | Type of device used | `Desktop`, `Mobile`, `Tablet` | Required |
| `browser` | String | Browser name and version | `Chrome 120`, `Edge 119` | Optional |
| `operating_system` | String | Operating system | `Windows 11`, `macOS` | Optional |

#### 6.2.4 Results (Phase 1 — AppInsights)

| Field Name | Data Type | Description | Example | Priority | Source |
|------------|-----------|-------------|---------|----------|--------|
| `results_count` | Integer | Number of results returned | `42` | Required | AppInsights |

#### 6.2.5 Click & Interaction Data (Phase 2 — GMDP)

**Note:** The following fields reside in **GMDP** and are **not available** in Phase 1. They are documented here for Phase 2 planning purposes.

| Field Name | Data Type | Description | Example | Source |
|------------|-----------|-------------|---------|--------|
| `clicked_result_id` | String | Document/page ID of clicked result | `DOC_12345` | GMDP |
| `clicked_result_title` | String | Title of clicked result | `Expense Policy 2026` | GMDP |
| `clicked_result_url` | String | URL of clicked result | `/policies/expense` | GMDP |
| `click_position` | Integer | Position of clicked result (1-based) | `2` | GMDP |
| `time_to_first_click` | Integer | Milliseconds until first click | `3500` | GMDP |
| `time_on_result` | Integer | Seconds spent on clicked page | `45` | GMDP |
| `post_click_action` | String | Action after click | `download`, `share`, `none` | GMDP |

#### 6.2.6 Search Performance (Phase 1 — AppInsights)

| Field Name | Data Type | Description | Example | Priority | Source |
|------------|-----------|-------------|---------|----------|--------|
| `search_latency_ms` | Integer | Time to return results (ms) | `450` | Required | AppInsights |
| `error_type` | String | Type of error if occurred | `timeout`, `no_results`, `null` | Required | AppInsights |
| `has_error` | Boolean | Whether an error occurred | `false` | Required | AppInsights |
| `suggestions_shown` | Boolean | Were query suggestions displayed | `true` | Optional | AppInsights |
| `suggestion_selected` | Boolean | Did user select a suggestion | `false` | Optional | AppInsights |

#### 6.2.7 Feedback & Outcome (Phase 1 — AppInsights)

| Field Name | Data Type | Description | Example | Priority | Source |
|------------|-----------|-------------|---------|----------|--------|
| `user_feedback` | String | Explicit feedback if provided | `thumbs_up`, `thumbs_down`, `null` | Optional | AppInsights |
| `feedback_rating` | Integer | Numeric rating (1-5) if provided | `4` | Optional | AppInsights |
| `query_reformulated` | Boolean | Did user search again immediately | `true` | Required | AppInsights |

### 6.3 Flat File Structure

| Property | Specification |
|----------|---------------|
| **File Format** | CSV (UTF-8 encoding) or Excel (.xlsx) |
| **Delimiter** | Comma (for CSV) |
| **Header Row** | Yes (first row contains column names) |
| **Date Format** | ISO 8601 (YYYY-MM-DD HH:MM:SS) |
| **Null Handling** | Empty string for missing values |
| **Naming Convention** | `search_analytics_YYYYMMDD_YYYYMMDD.csv` |
| **Example** | `search_analytics_20260101_20260107.csv` |

### 6.4 Data Groupings for Analysis

#### Phase 1 — Available Analysis (AppInsights)

| Grouping | Key Question | Analysis Purpose |
|----------|--------------|------------------|
| **User & Context** | "Who is searching and from where?" | Department-level adoption, geographic patterns, role-based behavior |
| **Query Details** | "What are they searching for?" | Top search terms, query complexity, language distribution |
| **Session & Device** | "When and how are they searching?" | Peak usage times, device preferences, session patterns |
| **Performance & Quality** | "How well does the search perform?" | Latency monitoring, error tracking, system health |
| **Zero Results & Refinement** | "Are users finding results?" | Zero-result rate, query refinement patterns |

#### Phase 2 — Deferred Analysis (Requires GMDP)

| Grouping | Key Question | Analysis Purpose |
|----------|--------------|------------------|
| **Results & Engagement** | "What do they do with results?" | Click-through rates, result position effectiveness, engagement depth |
| **User Success** | "Was the search successful?" | Abandonment analysis, content satisfaction |

### 6.5 Benefits of This Grouping Approach

| Benefit | Description | Phase 1 Examples | Phase 2 Examples |
|---------|-------------|------------------|------------------|
| **Pattern Identification** | Enables detection of patterns across dimensions | Teams with high zero-result rates; Mobile users experiencing more errors | Abandonment rates by department |
| **Improvement Opportunities** | Helps spot actionable optimization opportunities | Common queries returning no results; Slow searches at specific times | Low CTR queries needing better results |
| **Cross-Dimensional Analysis** | Allows correlation of metrics across groupings | Performance issues by department; Device-specific error patterns | Click behavior by device type |
| **Targeted Actions** | Supports prioritization of improvements | Focus content on zero-result queries; Address peak-time performance | Optimize result ranking for low-CTR queries |

---

## 7. Process Flows

### 7.1 Data Extraction Process

| Step | Activity | Owner | Output |
|------|----------|-------|--------|
| 1 | Start weekly process | Data Analyst | Process initiated |
| 2 | Access AppInsights Portal | Data Analyst | Portal access confirmed |
| 3 | Execute KQL Query | Data Analyst | Query results displayed |
| 4 | Review Query Results | Data Analyst | Data validated |
| 5 | Export to CSV/Excel | Data Analyst | Raw export file |
| 6 | Process & Clean Data | Data Analyst | Cleaned flat file |
| 7 | Refresh PowerBI Dataset | Data Analyst | Dataset updated |
| 8 | Upload to File Location | Data Analyst | File stored |
| 9 | End Process | Data Analyst | Process complete |

### 7.2 Weekly Operational Cadence

| Day | Activity | Tasks | Owner |
|-----|----------|-------|-------|
| **Monday** | Data Extraction & Processing | Run KQL queries; Export data; Clean & transform | Data Analyst |
| **Tuesday** | Dashboard Update & Validation | Refresh PowerBI; Validate metrics; Publish updates | Data Analyst |
| **Wednesday–Friday** | Stakeholder Access & Analysis | View dashboards; Generate insights; Export reports | Senior Management |

---

## 8. Key Performance Indicators

### 8.1 KPI Framework — Phase 1 (Tactical)

**Available in Phase 1** (AppInsights data only):

| Category | KPIs Included |
|----------|---------------|
| **Usage Metrics** | Total Search Volume, Unique Users, Searches per User, Department Breakdown |
| **Query Quality Metrics** | Zero Result Rate, Query Refinement Rate |
| **Performance Metrics** | Average Latency, P95 Latency, Error Rate |

**Deferred to Phase 2** (Requires GMDP click data):

| Category | KPIs Deferred | Reason |
|----------|---------------|--------|
| **Engagement Metrics** | Click-Through Rate, Abandonment Rate | Click data in GMDP |
| **Content Effectiveness** | Result Position Performance, Time on Result | Click data in GMDP |

### 8.2 KPI Definitions — Phase 1 (Available)

| KPI | Definition | Formula | Target | Source |
|-----|------------|---------|--------|--------|
| **Total Search Volume** | Total number of searches executed | `COUNT(search_events)` | Baseline TBD | AppInsights |
| **Unique Users** | Distinct users performing searches | `COUNT(DISTINCT user_id)` | Baseline TBD | AppInsights |
| **Searches per User** | Average searches per unique user | `Total Searches / Unique Users` | Baseline TBD | AppInsights |
| **Zero Result Rate** | Percentage of searches with no results | `Searches with 0 results / Total Searches` | < 5% | AppInsights |
| **Query Refinement Rate** | Percentage of queries followed by immediate re-search | `Refined queries / Total Searches` | < 20% | AppInsights |
| **Average Latency** | Mean time to return results | `AVG(search_latency_ms)` | < 500ms | AppInsights |
| **P95 Latency** | 95th percentile response time | `PERCENTILE(search_latency_ms, 0.95)` | < 2000ms | AppInsights |
| **Error Rate** | Percentage of searches resulting in errors | `Error searches / Total Searches` | < 1% | AppInsights |

### 8.3 KPI Definitions — Phase 2 (Deferred)

| KPI | Definition | Formula | Target | Source |
|-----|------------|---------|--------|--------|
| **Click-Through Rate** | Percentage of searches with at least one click | `Searches with clicks / Total Searches` | > 60% | GMDP |
| **Abandonment Rate** | Percentage of searches without any click | `Searches without clicks / Total Searches` | < 25% | GMDP |
| **Avg Click Position** | Average position of clicked results | `AVG(click_position)` | < 3 | GMDP |
| **Time to First Click** | Average time until user clicks a result | `AVG(time_to_click)` | < 5s | GMDP |

### 8.4 Dashboard Views — Phase 1

#### Page 1: Executive Summary

| Section | Visualizations |
|---------|----------------|
| KPI Cards (Top Row) | Total Searches, Unique Users, Zero Result Rate, Error Rate |
| Charts (Middle) | Search Volume Trend (Line), Top Search Terms (Bar) |
| Charts (Bottom) | Department Breakdown (Donut), Device Distribution (Donut) |

#### Page 2: Search Behavior Analysis

| Section | Visualizations |
|---------|----------------|
| Heatmap | Search Activity by Hour of Day |
| Charts | Query Length Distribution, Query Refinement Rate Trend |
| Table | Top Zero-Result Queries (sortable, filterable) |

#### Page 3: Performance Metrics

| Section | Visualizations |
|---------|----------------|
| KPI Cards | Avg Latency, P95 Latency, Error Rate, Query Refinement Rate |
| Charts | Latency Trend Over Time, Error Distribution by Type |

#### Page 4: Detailed Data (Drill-through)

| Section | Visualizations |
|---------|----------------|
| Data Table | Filterable table with all search events |
| Actions | Export functionality enabled |

---

## 9. Stakeholders & Responsibilities

### 9.1 Stakeholder Matrix

| Stakeholder | Role | Interest | Involvement |
|-------------|------|----------|-------------|
| Senior Management | Decision Maker | Strategic insights, investment justification | Consumer of dashboards/reports |
| IT Leadership | Sponsor | Platform performance, technical health | Review and approval |
| Content Owners | User | Content gaps, user needs identification | Consumer of insights |
| Search Platform Team | Technical Owner | System performance, improvements | Data source management |
| Data Analyst | Implementer | Data extraction, dashboard creation | Active contributor |

---

## 10. Assumptions & Constraints

### 10.1 Assumptions

| # | Assumption | Impact if Invalid |
|---|------------|-------------------|
| A1 | AppInsights contains required search telemetry data | Project cannot proceed; alternative data source needed |
| A2 | Data can be extracted via KQL queries or portal export | Manual data extraction method must be redesigned |
| A3 | PowerBI licenses available for dashboard creation and sharing | Alternative visualization tool required |
| A4 | Weekly manual refresh cadence is acceptable to stakeholders | More frequent updates require automation (Phase 2) |
| A5 | User data can be anonymized to meet privacy requirements | Additional privacy review and controls needed |

### 10.2 Constraints

| # | Constraint | Mitigation |
|---|------------|------------|
| C1 | No automated data pipeline available | Manual extraction process; automation planned for Phase 2 |
| C2 | Limited resources for development | Tactical approach with minimal custom development |
| C3 | Click-through data not fully instrumented | Partial click data where available; full tracking in Phase 2 |
| C4 | Data latency due to manual process | Set stakeholder expectations; weekly refresh cadence |

---

## 11. Risks & Mitigation

### 11.1 Risk Register

| ID | Risk | Probability | Impact | Mitigation Strategy |
|----|------|-------------|--------|---------------------|
| R1 | Required data fields not available in AppInsights | Medium | High | Early validation of data availability; adjust scope if needed |
| R2 | Data quality issues (missing, inconsistent data) | Medium | Medium | Implement data validation checks; document known issues |
| R3 | Manual process unsustainable long-term | High | Medium | Plan Phase 2 automation; document process for efficiency |
| R4 | Stakeholder expectations exceed tactical capabilities | Medium | Medium | Clear communication of scope; regular expectation management |
| R5 | Privacy/compliance concerns with user data | Low | High | Implement anonymization; engage privacy team for review |
| R6 | Key person dependency for data extraction | Medium | Medium | Document procedures; cross-train backup resources |

### 11.2 Risk Matrix

| | **Low Impact** | **Medium Impact** | **High Impact** |
|---|----------------|-------------------|-----------------|
| **High Probability** | | R3 | |
| **Medium Probability** | R6 | R2, R4 | R1 |
| **Low Probability** | | | R5 |

---

## 12. Success Criteria

### 12.1 Acceptance Criteria

| # | Criterion | Validation Method |
|---|-----------|-------------------|
| AC1 | PowerBI dashboard operational and accessible to defined stakeholders | Access verification |
| AC2 | Dashboard displays all required KPIs as defined in Section 8 | Functional review |
| AC3 | Data refreshed at least weekly | Process execution log |
| AC4 | Data accuracy validated against source | Sample data reconciliation |
| AC5 | Stakeholders can independently access and navigate dashboards | User acceptance testing |

### 12.2 Definition of Done

The tactical solution will be considered complete when:

1. All required data fields are extracted and processed
2. PowerBI dashboard is published to PowerBI Service
3. Stakeholders have appropriate access permissions
4. Initial baseline metrics are established
5. Documentation for ongoing maintenance is complete
6. Handover to operations is complete

---

## 13. Future State Roadmap

### 13.1 Phase Evolution

| Aspect | Phase 1: Tactical (Current) | Phase 2: Automated Pipeline + GMDP |
|--------|----------------------------|-----------------------------------|
| **Data Extraction** | Manual export | Automated scheduled jobs |
| **Data Storage** | Flat files | Data Lake + GMDP integration |
| **Click Tracking** | Not available | Full click-stream integration |
| **Refresh Frequency** | Weekly | Daily/hourly |
| **Analytics** | Core KPIs (AppInsights only) | Extended KPIs incl. CTR, engagement |
| **Capabilities** | Search volume, Zero-result analysis, Performance monitoring | + Click-through analysis, Abandonment tracking, Alerting |

### 13.2 Phase 2 Enhancements (Future)

| Enhancement | Description | Business Value |
|-------------|-------------|----------------|
| Automated Data Pipeline | Scheduled, automated data extraction and loading | Reduced manual effort, fresher data |
| Click-Stream Integration | Full click tracking from search to page engagement | Deeper understanding of search effectiveness |
| Real-Time Dashboards | Near real-time data refresh | Faster issue identification |
| Alerting | Automated alerts for anomalies (e.g., high error rates) | Proactive issue resolution |
| GMDP Integration | Connection to enterprise data platform | Consolidated analytics, broader context |

---

## 14. Appendix

### 14.1 Glossary

| Term | Definition |
|------|------------|
| **AppInsights** | Azure Application Insights — Microsoft's application performance monitoring service |
| **KQL** | Kusto Query Language — query language used in Azure Data Explorer and AppInsights |
| **GMDP** | Global Marketing Data Platform (enterprise data platform) |
| **CTR** | Click-Through Rate — percentage of searches resulting in clicks |
| **Zero Result Rate** | Percentage of searches that return no results |
| **P95 Latency** | 95th percentile response time (95% of requests complete within this time) |
| **Query Refinement** | When a user modifies their search query immediately after initial search |
| **Session** | A group of user interactions within a defined time window |

### 14.2 Sample KQL Query Template

```kusto
// Sample KQL query for search analytics extraction
customEvents
| where timestamp >= ago(7d)
| where name == "SearchQuery"
| extend
    user_id = tostring(customDimensions.userId),
    department = tostring(customDimensions.department),
    search_terms = tostring(customDimensions.searchTerms),
    results_count = toint(customDimensions.resultsCount),
    search_latency_ms = toint(customDimensions.latencyMs),
    device_type = tostring(customDimensions.deviceType),
    session_id = tostring(session_Id)
| project
    timestamp,
    user_id,
    department,
    search_terms,
    results_count,
    search_latency_ms,
    device_type,
    session_id
| order by timestamp desc
```

### 14.3 Reference Documents

| Document | Description |
|----------|-------------|
| AppInsights Access Guide | Instructions for accessing Azure AppInsights portal |
| PowerBI Publishing Guide | Steps for publishing dashboards to PowerBI Service |
| Data Privacy Policy | Corporate guidelines for handling user data |

---

## Document Approval

| Role | Name | Signature | Date |
|------|------|-----------|------|
| Business Owner | | | |
| Technical Lead | | | |
| Data Privacy | | | |

---

*End of Document*
