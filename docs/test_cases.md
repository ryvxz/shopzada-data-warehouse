# ShopZada 2.0 Data Warehouse - Test Cases
## End-to-End Data Warehouse System Validation

**Project:** ShopZada 2.0 – Enterprise Data Warehouse Build  
**Purpose:** Comprehensive test case documentation for system validation  
**Date:** December 2025  

---

## Table of Contents

1. [Testing Overview](#testing-overview)
2. [Test Execution Guidelines](#test-execution-guidelines)
3. [PART A: Essential Test Cases](#part-a-essential-test-cases)
4. [PART B: Extensive Test Cases](#part-b-extensive-test-cases)
5. [Test Data Requirements](#test-data-requirements)
6. [Recording Requirements](#recording-requirements)

---

## Testing Overview

### Objectives

This test suite validates the correctness, robustness, and completeness of:
- **Workflow logic** (Airflow orchestration)
- **Dimensional/Entity-Relationship model** (Kimball star schema)
- **Data transformations** (ETL/ELT processes)
- **Quality handling** (data validation and cleansing)
- **Integration and relationships** (cross-system connectivity)
- **Analytical and dashboard layers** (BI and reporting)

### Test Architecture Alignment

These test cases are **architecture-agnostic** and work regardless of:
- Design pattern (Kimball, Dimensional)
- Tools used (PostgreSQL, Airflow, Metabase)
- Orchestration framework
- Infrastructure setup (Docker, cloud, etc.)

### Grading Rubric Mapping

| Test Category | Rubric Component | Points |
|---------------|------------------|--------|
| Infrastructure & Deployment | Infrastructure & Deployment | 10 pts |
| Data Ingestion & Loading | Workflow Implementation | 10 pts |
| Transformation & Quality | Workflow Implementation | 10 pts |
| Dimensional Model | Design & Modeling | 10 pts |
| Analytics & Dashboards | Analytics & Insights | 10 pts |
| Integration & E2E | All Components | Comprehensive |

---

## Test Execution Guidelines

### General Requirements

> [!IMPORTANT]
> **All test executions MUST be recorded in a continuous screen-capture video showing:**
> - Test data preparation or modification
> - Running the workflow/pipeline
> - Showing required assets/structures
> - Debugging and fixing errors (if they occur)
> - Re-running after fixes
> - Dashboard verification (where applicable)

### Execution Rules

1. **Same Workstation Rule**: Use the same workstation for the entire test sequence
2. **Continuous Recording**: One uninterrupted recording per test scenario
3. **Complete Sequence**: Show initial run, debugging, reruns in single video
4. **Audio Narration**: Provide spoken commentary explaining each step
5. **Timestamps**: Include visible timestamps in recordings

### Before Starting Tests

> [!CAUTION]
> **Do not begin testing until:**
> - Docker environment is fully operational (`docker-compose ps` shows all healthy)
> - Airflow UI accessible at `http://localhost:8080`
> - Databases accessible (staging and DWH)
> - All required test data files prepared
> - Screen recording software configured and tested

### Test Data Location

```
shopzada-data-warehouse/
├── data/
│   ├── test_cases/          # Test-specific datasets
│   │   ├── TC-E-01/         # Test case specific data
│   │   ├── TC-E-02/
│   │   └── ...
│   └── raw/                 # Production datasets
```

---

## PART A: Essential Test Cases

> [!NOTE]
> These test cases are **CRITICAL** and validate core functionality required for project success.
> Failure of any essential test case indicates a fundamental system issue.

### Category 1: Infrastructure & Deployment

#### TC-E-01: Docker Environment Startup
**Objective**: Verify complete system can be deployed with a single command

**Prerequisites**:
- Docker Desktop installed and running
- Project repository cloned
- No existing ShopZada containers running

**Test Steps**:
1. Navigate to project directory
2. Run: `docker-compose -f ./infra/docker-compose.yml up -d`
3. Wait for all services to start (2-5 minutes)
4. Verify all containers healthy: `docker-compose -f ./infra/docker-compose.yml ps`

**Expected Results**:
- All services show "healthy" or "running" status:
  - `airflow-apiserver` (port 8080)
  - `airflow-scheduler`
  - `airflow-worker`
  - `airflow-dag-processor`
  - `airflow-triggerer`
  - `db_dwh` (port 5432)
  - `db_staging` (port 5433)
  - `metabase` (port 3000)
  - `metabase-db`
  - `postgres` (Airflow metadata)
  - `redis`

**Assets to Show**:
- Terminal output showing successful container startup
- `docker-compose ps` output with all services healthy
- Screenshot of Airflow UI login page (`http://localhost:8080`)
- Screenshot of Metabase UI (`http://localhost:3000`)

**Pass Criteria**: All 11 services running and healthy within 5 minutes

---

#### TC-E-02: Database Connectivity Verification
**Objective**: Verify all databases are accessible and properly configured

**Prerequisites**:
- TC-E-01 passed (all containers running)

**Test Steps**:
1. **Test DWH Database**:
   ```bash
   docker exec -it shopzada-db-dwh psql -U postgres -d shopzada_dwh -c "\l"
   ```

2. **Test Staging Database**:
   ```bash
   docker exec -it shopzada-db-staging psql -U postgres -d shopzada_staging -c "\l"
   ```

3. **Test Airflow Metadata Database**:
   ```bash
   docker exec -it infra-postgres-1 psql -U airflow -d airflow -c "\dt"
   ```

4. **Test Database Connections from Airflow**:
   - Login to Airflow UI
   - Navigate to Admin → Connections
   - Verify PostgreSQL connections configured

**Expected Results**:
- All database connections successful
- Expected databases exist (`shopzada_dwh`, `shopzada_staging`, `airflow`, `metabase`)
- No connection errors
- Airflow connections properly configured

**Assets to Show**:
- Terminal output from each `psql` command
- Screenshot of Airflow connections page
- List of databases in each PostgreSQL instance

**Pass Criteria**: All 4 database connections successful, no errors

---

### Category 2: Data Ingestion & Loading

#### TC-E-03: Multi-Format File Ingestion
**Objective**: Verify system can ingest all supported file formats (CSV, JSON, Excel, HTML, Parquet, Pickle)

**Test Data**: Prepare minimal test dataset with one file of each format

Test files to create in `data/test_cases/TC-E-03/`:
- `test_users.csv` (10 rows)
- `test_orders.json` (10 records)
- `test_products.xlsx` (10 rows)
- `test_merchants.html` (table with 5 rows)
- `test_line_items.parquet` (10 rows)
- `test_credit_cards.pkl` (10 records)

**Test Steps**:
1. Place test files in `data/test_cases/TC-E-03/`
2. Update DATA_FOLDER environment variable to point to test directory
3. Run data ingestion via Airflow DAG:
   - Open Airflow UI
   - Trigger `shopzada_data_warehouse` DAG
   - Monitor `source_staging` task group
4. Check `data/preprocessed/` for Parquet output files
5. Verify row counts match input files

**Expected Results**:
- All 6 file formats successfully read
- Parquet files created in `data/preprocessed/` directory
- No format-specific errors in Airflow logs
- Row counts preserved (10 rows each for CSV, JSON, Excel, HTML, Parquet, Pickle)

**Assets to Show**:
- Test data files (show first few rows of each)
- Airflow DAG run graph showing `ingest_all_sources` task SUCCESS
- Directory listing of `data/preprocessed/` with generated Parquet files
- Sample of converted Parquet data (use `parquet-tools` or Python script)
- Airflow task logs showing file processing

**Pass Criteria**: All 6 file formats ingested without errors, Parquet files created

---

#### TC-E-04: Data Quality Validation Framework
**Objective**: Verify data quality checks identify issues in source data

**Test Data**: Create dataset with known quality issues

Create `data/test_cases/TC-E-04/test_quality.csv`:
```csv
user_id,name,email,age,created_date
USER001,John Doe,john@example.com,25,2024-01-01
USER002,Jane Smith,,30,2024-01-02
USER003,Bob Johnson,bob@example.com,,-1-03
INVALID,Alice Williams,alice@example.com,28,2024-01-04
USER001,John Doe,john@example.com,25,2024-01-01
,NULL_NAME,null@example.com,35,2024-01-05
```

Issues present:
- Missing email (row 2)
- Missing age (row 3)
- Invalid date format (row 3)
- Invalid user_id format (row 4)
- Duplicate record (rows 1 and 5)
- Missing user_id (row 6)

**Test Steps**:
1. Place test file in test directory
2. Run Airflow DAG and observe `data_quality_checks_and_report` task
3. Check quality report output in `data/reports/` or Airflow logs
4. Verify all 6 issues are flagged

**Expected Results**:
- Quality check task generates report
- Report identifies all quality issues:
  - Missing values in critical fields (email, age, user_id)
  - Invalid format issues (date, user_id pattern)
  - Duplicates detected
- Task completes with warnings (not failure) to allow investigation

**Assets to Show**:
- Test data file with quality issues highlighted
- Airflow task log showing quality check execution
- Quality report showing detected issues:
  - Count of missing values per column
  - Invalid format examples
  - Duplicate records identified
- Screenshot of Airflow task status

**Pass Criteria**: All 6 known quality issues detected and reported

---

#### TC-E-05: Staging Database Loading
**Objective**: Verify data successfully loads from Parquet files to PostgreSQL staging database

**Prerequisites**:
- TC-E-03 passed (Parquet files generated)

**Test Steps**:
1. Trigger Airflow DAG `shopzada_data_warehouse`
2. Monitor `load_to_staging_db` task in `source_staging` task group
3. After task completion, verify data in staging database:
   ```sql
   docker exec -it shopzada-db-staging psql -U postgres -d shopzada_staging -c "\dt"
   ```
4. Check row counts for each loaded table:
   ```sql
   SELECT 'table_name' as table, COUNT(*) FROM table_name;
   ```
5. Verify schema matches source data

**Expected Results**:
- All expected tables created in `shopzada_staging` database
- Row counts match Parquet file row counts
- Data types correctly inferred
- No data loss during load
- Task completes successfully

**Assets to Show**:
- Airflow task log for `load_to_staging_db`
- PostgreSQL `\dt` output showing all staging tables
- Row count queries with results
- Sample data from staging tables (`SELECT * FROM table LIMIT 5`)
- Airflow task status showing SUCCESS

**Pass Criteria**: All tables loaded, row counts verified, no load errors

---

### Category 3: Workflow Orchestration

#### TC-E-06: Airflow DAG Execution E2E
**Objective**: Verify complete Airflow DAG executes all implemented task groups sequentially

**Prerequisites**:
- Fresh dataset in `data/raw/` directory
- All containers healthy

**Test Steps**:
1. Clear any previous DAG runs in Airflow UI
2. Trigger `shopzada_data_warehouse` DAG manually
3. Monitor execution through all task groups:
   - `source_staging`
   - `transform_and_quality_checks`
   - `load_to_dw`
   - `kimball_dw`
4. Observe task dependencies and execution order
5. Check final DAG status

**Expected Results**:
- DAG runs to completion (all implemented tasks)
- Tasks execute in correct order based on dependencies
- Task group `source_staging` completes before `transform_and_quality_checks`
- All tasks show SUCCESS status (for implemented tasks)
- Placeholder tasks (EmptyOperator) also complete
- Total duration under 10 minutes for test dataset

**Assets to Show**:
- Airflow DAG graph view showing task dependencies
- Airflow grid view showing successful run
- Task group expansion showing individual task statuses
- Gantt chart showing execution timeline
- Final DAG run status (SUCCESS)

**Pass Criteria**: DAG completes successfully, correct task execution order

---

#### TC-E-07: Workflow Error Handling & Retries
**Objective**: Verify workflow correctly handles failures and retries

**Test Data**: Intentionally create a failing scenario

**Test Steps**:
1. Modify staging database to cause connection failure temporarily:
   ```bash
   docker stop shopzada-db-staging
   ```
2. Trigger Airflow DAG
3. Observe `load_to_staging_db` task attempts retries (configured for 3 retries)
4. After observing at least 1 retry, restore database:
   ```bash
   docker start shopzada-db-staging
   ```
5. Verify task eventually succeeds on retry

**Expected Results**:
- Task enters "retry" state when connection fails
- Airflow attempts configured number of retries (3)
- After database restoration, subsequent retry succeeds
- DAG continues execution after successful retry
- Error logged but DAG doesn't fail permanently

**Assets to Show**:
- Docker command stopping staging database
- Airflow UI showing task in "UP FOR RETRY" state
- Task logs showing connection error and retry attempts
- Docker command restarting database
- Task eventually showing SUCCESS after retry
- Complete DAG run showing recovery from failure

**Pass Criteria**: Task retries on failure and succeeds when issue resolved

---

### Category 4: Data Transformation & Quality

#### TC-E-08: Staging to Warehouse Transformation
**Objective**: Verify data transformations correctly process staging data for warehouse

**Prerequisites**:
- Staging database populated with test data
- Transformation scripts implemented

**Test Steps**:
1. Verify data exists in staging:
   ```sql
   SELECT COUNT(*) FROM shopzada_staging.order_data;
   ```
2. Run `transform_and_quality_checks` task group
3. Monitor transformation task execution
4. Verify transformed data in DWH database:
   ```sql
   docker exec -it shopzada-db-dwh psql -U postgres -d shopzada_dwh -c "\dt"
   ```
5. Compare row counts and sample data between staging and DWH

**Expected Results**:
- Transformation tasks execute successfully
- Data appears in DWH database
- Business rules applied correctly
- Data types appropriate for analytics
- No data loss (or expected filtering documented)

**Assets to Show**:
- Staging data sample (before transformation)
- Airflow transformation task logs
- DWH database tables list
- Transformed data sample (after transformation)
- Side-by-side comparison showing transformation effects
- Row count reconciliation

**Pass Criteria**: Data successfully transformed and loaded to DWH

---

#### TC-E-09: Data Quality Post-Transformation
**Objective**: Verify quality checks validate transformed data integrity

**Test Steps**:
1. Run complete ETL pipeline
2. Execute `quality_checks` task in `transform_and_quality_checks` group
3. Review quality check results
4. Verify checks include:
   - Referential integrity (foreign keys valid)
   - Null checks on critical fields
   - Data type validation
   - Range checks (dates, amounts)
   - Business rule validation

**Expected Results**:
- Quality checks execute on transformed data
- Report generated with pass/fail results
- No critical integrity violations
- Any warnings documented and justified

**Assets to Show**:
- Quality check task logs
- Quality report showing all checks performed
- Pass/fail summary for each check type
- Examples of validated records
- Handling of any warnings or non-critical issues

**Pass Criteria**: All critical quality checks pass

---

### Category 5: Dimensional Model

#### TC-E-10: Dimension Table Creation
**Objective**: Verify all dimension tables created with correct structure

**Prerequisites**:
- DWH database initialized
- Dimensional model scripts implemented

**Test Steps**:
1. Run `build_dimensions` task in `kimball_dw` task group
2. Verify dimension tables exist in DWH:
   ```sql
   SELECT table_name FROM information_schema.tables 
   WHERE table_schema = 'public' AND table_name LIKE 'dim_%';
   ```
3. For each dimension table, verify structure:
   ```sql
   \d+ dim_customer
   \d+ dim_product
   \d+ dim_merchant
   \d+ dim_staff
   \d+ dim_campaign
   \d+ dim_date
   ```
4. Check surrogate keys exist and are unique
5. Verify slowly changing dimension (SCD) columns if applicable

**Expected Dimension Tables**:
- `dim_customer` (customer demographics, location, type)
- `dim_product` (product catalog with categories)
- `dim_merchant` (merchant information)
- `dim_staff` (staff details)
- `dim_campaign` (marketing campaign definitions)
- `dim_date` (date dimension with hierarchies)

**Expected Results**:
- All 6 dimension tables created
- Each table has surrogate key (e.g., `customer_key`, `product_key`)
- Natural keys preserved (e.g., `user_id`, `product_id`)
- Descriptive attributes present
- Appropriate constraints (primary keys, not null)

**Assets to Show**:
- SQL query listing all dimension tables
- Table structure for each dimension (`\d+` output)
- Sample rows from each dimension (`SELECT * LIMIT 5`)
- Verification of surrogate key uniqueness
- Row count for each dimension table

**Pass Criteria**: All 6 dimension tables created with correct structure

---

#### TC-E-11: Fact Table Creation
**Objective**: Verify fact tables created with measures and foreign keys to dimensions

**Prerequisites**:
- TC-E-10 passed (dimension tables exist)

**Test Steps**:
1. Run `build_facts` task in `kimball_dw` task group
2. Verify fact tables exist:
   ```sql
   SELECT table_name FROM information_schema.tables 
   WHERE table_schema = 'public' AND table_name LIKE 'fact_%';
   ```
3. Check fact table structure:
   ```sql
   \d+ fact_orders
   ```
4. Verify foreign keys to dimensions exist
5. Check measures (numerical facts) present
6. Validate grain (one row per order line item)

**Expected Fact Tables**:
- `fact_orders` (order line item transactions with measures)

**Expected Results**:
- Fact table created with correct structure
- Foreign keys to all relevant dimensions:
  - `customer_key` → `dim_customer`
  - `product_key` → `dim_product`
  - `merchant_key` → `dim_merchant`
  - `staff_key` → `dim_staff`
  - `campaign_key` → `dim_campaign`
  - `order_date_key` → `dim_date`
- Measures present (quantity, price, subtotal, discount, etc.)
- Degenerate dimensions if applicable (order_id)
- Grain correctly implemented

**Assets to Show**:
- SQL query listing fact tables
- Fact table structure showing all columns
- Foreign key constraints documented
- Sample fact records with dimension keys
- Row count and grain verification query
- Join example showing star schema in action:
  ```sql
  SELECT c.name, p.product_name, f.quantity, f.price
  FROM fact_orders f
  JOIN dim_customer c ON f.customer_key = c.customer_key
  JOIN dim_product p ON f.product_key = p.product_key
  LIMIT 5;
  ```

**Pass Criteria**: Fact table(s) created with correct star schema relationships

---

#### TC-E-12: Star Schema Integrity
**Objective**: Verify referential integrity across star schema (dimensions and facts)

**Test Steps**:
1. Verify all foreign keys in fact table reference valid dimension keys:
   ```sql
   -- Check for orphaned fact records
   SELECT COUNT(*) FROM fact_orders f
   LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
   WHERE c.customer_key IS NULL;
   ```
2. Repeat for all dimension foreign keys
3. Verify no null foreign keys (unless business rule allows)
4. Test cascade behavior if dimension record updated

**Expected Results**:
- Zero orphaned fact records (all foreign keys valid)
- All dimension lookups succeed
- Primary-foreign key relationships enforced
- Star schema navigable in both directions

**Assets to Show**:
- Orphan check queries for each dimension (all returning 0)
- Sample star schema queries demonstrating relationships
- ERD or schema diagram showing connections
- Constraint verification output

**Pass Criteria**: No orphaned records, all referential integrity enforced

---

### Category 6: Analytics & Dashboards

#### TC-E-13: Analytical SQL Views
**Objective**: Verify creation of analytical views supporting business questions

**Prerequisites**:
- Star schema populated with data

**Test Steps**:
1. Execute SQL scripts creating analytical views
2. Verify views exist:
   ```sql
   SELECT table_name FROM information_schema.views 
   WHERE table_schema = 'public';
   ```
3. Query each view to ensure it returns data:
   - View for product performance
   - View for customer segmentation
   - View for campaign effectiveness
   - View for monthly sales trends
   - View for merchant performance
4. Validate view logic answers business questions

**Expected Views** (minimum):
- `v_product_performance` (top products by revenue)
- `v_customer_segmentation` (revenue by customer demographics)
- `v_campaign_roi` (campaign effectiveness metrics)
- `v_monthly_sales_trend` (time series aggregations)
- `v_merchant_performance` (merchant leaderboard)

**Expected Results**:
- All analytical views created successfully
- Views return meaningful data
- Aggregations correct (SUM, COUNT, AVG)
- Joins perform efficiently
- Results answer intended business questions

**Assets to Show**:
- List of created views
- View definition SQL (`\d+ view_name`)
- Sample output from each view
- Query performance metrics
- Business question → View mapping explanation

**Pass Criteria**: All planned analytical views created and functional

---

#### TC-E-14: BI Dashboard Connectivity
**Objective**: Verify BI tool (Metabase) connects to data warehouse

**Prerequisites**:
- Metabase container running
- DWH populated with data

**Test Steps**:
1. Access Metabase UI (`http://localhost:3000`)
2. Complete initial setup if first time
3. Add database connection to ShopZada DWH:
   - Database type: PostgreSQL
   - Host: `db_dwh` (or `shopzada-db-dwh`)
   - Port: `5432`
   - Database name: `shopzada_dwh`
   - Username: `postgres`
   - Password: `shopzada123`
4. Test connection
5. Browse data model in Metabase

**Expected Results**:
- Metabase successfully connects to DWH PostgreSQL
- All tables and views visible in Metabase data browser
- Can preview data from tables/views
- Connection test passes

**Assets to Show**:
- Metabase login screen
- Database connection configuration page
- Connection test success message
- Metabase data model browser showing ShopZada tables
- Sample data preview in Metabase

**Pass Criteria**: Metabase connects successfully to DWH database

---

#### TC-E-15: Dashboard Creation - Business Question 1
**Objective**: Create dashboard answering "What are our top-performing products?"

**Prerequisites**:
- TC-E-14 passed (Metabase connected)
- Product performance view or fact/dimension tables available

**Test Steps**:
1. In Metabase, create new dashboard "Executive Overview"
2. Create visualization:
   - **Title**: "Top 10 Products by Revenue"
   - **Data source**: `v_product_performance` or join query
   - **Metric**: SUM(revenue)
   - **Dimension**: product_name
   - **Sort**: DESC by revenue
   - **Limit**: 10
   - **Chart type**: Bar chart
3. Add to dashboard
4. Apply filters (date range, category)
5. Save and publish dashboard

**Expected Results**:
- Chart displays top 10 products correctly
- Revenue values accurate (cross-check with SQL)
- Bars sorted descending by revenue
- Filters functional (date range, category filtering works)
- Dashboard loads in under 5 seconds

**Assets to Show**:
- Metabase query builder configuration
- Generated SQL query (Metabase shows this)
- Resulting bar chart visualization
- Sample dashboard with chart embedded
- Proof of filter functionality (before/after applying filter)
- SQL verification query showing same results

**Pass Criteria**: Dashboard created, data accurate, answers business question

---

#### TC-E-16: Dashboard Creation - Business Question 2
**Objective**: Create dashboard answering "Which customer segments contribute most to revenue?"

**Test Steps**:
1. Create new visualization in Metabase:
   - **Title**: "Revenue by Customer Segment"
   - **Data source**: Join `fact_orders` with `dim_customer`
   - **Metric**: SUM(quantity * price)
   - **Dimensions**: customer_type, customer_location
   - **Chart type**: Pie chart or stacked bar
2. Add customer demographics breakdown:
   - Age group distribution
   - Geographic revenue map (if supported)
   - Customer type comparison
3. Save to dashboard

**Expected Results**:
- Revenue correctly aggregated by customer segments
- Multiple dimensions analyzed (type, location, demographics)
- Visualizations appropriate for comparison (pie, bar, treemap)
- Insights actionable (clear which segments drive revenue)

**Assets to Show**:
- Query configuration in Metabase
- Multiple visualizations showing different customer segments
- Full dashboard combining customer analytics
- Drill-down capability demonstration (if implemented)

**Pass Criteria**: Dashboards created, customer segmentation clearly visualized

---

#### TC-E-17: Dashboard Creation - Business Question 3
**Objective**: Create dashboard answering "What marketing campaigns drive highest order volume?"

**Test Steps**:
1. Create visualization:
   - **Title**: "Campaign Performance Comparison"
   - **Data source**: `v_campaign_roi` or join with `dim_campaign`
   - **Metrics**: 
     - Order count
     - Total revenue
     - Total discount given
     - ROI calculation
   - **Dimension**: campaign_name
   - **Chart type**: Table or multi-metric bar chart
2. Add time series showing campaign performance over time
3. Include ROI calculation: `(revenue - discount) / discount`

**Expected Results**:
- All campaigns listed with metrics
- ROI correctly calculated
- Clear identification of best-performing campaigns
- Time-based analysis shows campaign trends

**Assets to Show**:
- Campaign performance table/chart
- ROI calculation methodology
- Time series chart showing campaign impact over time
- Ability to compare multiple campaigns side-by-side

**Pass Criteria**: Campaign effectiveness clearly visualized, ROI calculated

---

### Category 7: Integration & End-to-End

#### TC-E-18: Complete E2E Pipeline Test
**Objective**: Verify entire pipeline from raw files to dashboard in one continuous run

**Test Data**: Fresh, complete test dataset

**Test Steps**:
1. **Preparation**:
   - Stop all containers
   - Clean all Docker volumes
   - Prepare fresh test dataset in `data/raw/`
2. **Execution**:
   - Start containers: `docker-compose up -d`
   - Wait for all services healthy
   - Trigger Airflow DAG
   - Monitor complete execution
3. **Verification**:
   - Check staging database populated
   - Verify DWH star schema created
   - Confirm dashboards display data
   - Validate end-to-end data lineage

**Expected Results**:
- Zero manual intervention required
- Pipeline completes in under 15 minutes (for test dataset)
- Data flows through all layers (raw → staging → DWH → dashboard)
- Dashboards display correct, up-to-date information
- All quality checks pass

**Assets to Show**:
- Initial state (empty databases)
- Complete Airflow DAG execution recording
- Staging data verification
- DWH data verification
- Dashboard showing final results
- Timeline showing E2E execution duration

**Pass Criteria**: Complete pipeline executes successfully without manual intervention

---

## PART B: Extensive Test Cases

> [!TIP]
> These test cases ensure **completeness and robustness** beyond core functionality.
> They validate edge cases, error handling, performance, and advanced scenarios.

### Category 8: Edge Cases & Error Handling

#### TC-X-01: Empty Dataset Handling
**Objective**: Verify system handles empty input files gracefully

**Test Data**: Empty CSV, JSON files

**Test Steps**:
1. Create empty files for each supported format:
   - `empty.csv` (header only, no data rows)
   - `empty.json` (empty array: `[]`)
   - `empty.xlsx` (sheet with headers, no data)
2. Run ingestion pipeline
3. Verify system behavior

**Expected Results**:
- System doesn't crash on empty files
- Warning logged about empty datasets
- Zero rows loaded to staging
- Pipeline continues execution
- Quality report flags empty datasets

**Pass Criteria**: System handles empty data gracefully without failures

---

#### TC-X-02: Data Type Mismatch Handling
**Objective**: Verify system handles data type inconsistencies

**Test Data**: CSV with mixed data types in same column

Example `bad_types.csv`:
```csv
user_id,age,created_date
USER001,25,2024-01-01
USER002,thirty,2024-01-02
USER003,35,not-a-date
```

**Test Steps**:
1. Place file in data directory
2. Run ingestion
3. Observe quality check behavior
4. Verify error handling

**Expected Results**:
- Invalid data types flagged by quality checks
- Options: reject row, coerce to null, or type cast with warning
- Error report includes specific rows with type issues
- Processing continues (doesn't halt on type errors)

**Pass Criteria**: Type mismatches detected and handled per business rules

---

#### TC-X-03: Large File Handling
**Objective**: Verify system can process large datasets (100MB+ files)

**Test Data**: Generate large CSV with 1 million rows

**Test Steps**:
1. Generate test file using script:
   ```python
   import pandas as pd
   import numpy as np
   
   df = pd.DataFrame({
       'user_id': [f'USER{i:06d}' for i in range(1000000)],
       'name': [f'User {i}' for i in range(1000000)],
       'amount': np.random.randint(10, 1000, 1000000)
   })
   df.to_csv('data/test_cases/TC-X-03/large_file.csv', index=False)
   ```
2. Run ingestion pipeline
3. Monitor resource usage (CPU, Memory)
4. Verify completion time

**Expected Results**:
- File processes successfully (even if slower)
- Memory usage stays within container limits
- No out-of-memory errors
- Row count verified: 1 million
- Processing time documented (baseline for performance)

**Pass Criteria**: 1M row file processes successfully without crashes

---

#### TC-X-04: Duplicate Key Handling in Dimensions
**Objective**: Verify dimension loading handles duplicate natural keys correctly

**Test Data**: Customer data with duplicate user_ids

**Test Steps**:
1. Create test data with intentional duplicates:
   ```csv
   user_id,name,email
   USER001,John Doe,john@example.com
   USER001,John Doe Updated,john_new@example.com
   ```
2. Load to dimension table
3. Verify behavior: Is it insert, update, or SCD Type 2?

**Expected Results**:
- System detects duplicate natural key
- Behavior follows documented SCD strategy:
  - **Type 1**: Update existing record
  - **Type 2**: Create new version with date range
  - **Type 3**: Add previous value column
- No primary key violations
- Audit trail if applicable

**Pass Criteria**: Duplicates handled according to SCD strategy

---

#### TC-X-05: Missing Foreign Key Reference
**Objective**: Verify fact loading handles missing dimension references

**Test Data**: Fact record with invalid customer_id

Create scenario where fact references customer that doesn't exist in dimension:
```csv
order_id,user_id,product_id,quantity
ORD001,USER999,PROD001,5
```
(Assuming USER999 not in `dim_customer`)

**Test Steps**:
1. Load fact data with invalid foreign key
2. Observe behavior during fact loading
3. Check referential integrity

**Expected Results**:
- System detects missing dimension reference
- Options per business rule:
  - Reject fact record with error log
  - Create "Unknown" dimension record
  - Load to error table for manual review
- Referential integrity maintained
- Error logged with details

**Pass Criteria**: Missing references handled per data governance policy

---

#### TC-X-06: Special Characters & Encoding
**Objective**: Verify system handles special characters and multiple encodings

**Test Data**: Files with Unicode characters, emojis, special symbols

```csv
user_id,name,city
USER001,François,Montréal
USER002,李明,北京
USER003,José García,São Paulo
USER004,Emoji User 😀,Tokyo
```

**Test Steps**:
1. Create files with various encodings (UTF-8, UTF-16, Latin-1)
2. Include special characters, accents, non-Latin scripts
3. Run ingestion
4. Verify character preservation

**Expected Results**:
- All characters preserved correctly
- No encoding errors or mojibake
- Unicode characters display correctly in database
- Dashboard shows special characters properly

**Pass Criteria**: All special characters preserved throughout pipeline

---

#### TC-X-07: Concurrent DAG Execution Prevention
**Objective**: Verify only one instance of DAG runs at a time

**Test Steps**:
1. Trigger Airflow DAG manually
2. While first run is in progress, attempt to trigger again
3. Observe Airflow behavior (DAG configured with `max_active_runs=1`)

**Expected Results**:
- Second trigger queued, not executed immediately
- First run completes before second starts
- No data corruption from concurrent writes
- Message indicates DAG already running

**Pass Criteria**: Concurrent execution prevented, runs queued

---

### Category 9: Performance & Scalability

#### TC-X-08: Query Performance Benchmarking
**Objective**: Establish performance benchmarks for analytical queries

**Test Steps**:
1. Execute standard analytical queries on populated DWH:
   - Product performance aggregation
   - Customer segmentation
   - Time-series trend queries
2. Measure execution time using `EXPLAIN ANALYZE`
3. Document results

**Expected Results**:
- Simple aggregations: under 1 second
- Complex joins (5+ tables): under 5 seconds
- Time series queries: under 3 seconds
- Dashboard refresh: under 10 seconds

**Assets to Show**:
- `EXPLAIN ANALYZE` output for key queries
- Execution time summary table
- Identification of slow queries for optimization

**Pass Criteria**: All queries meet performance targets, slow queries identified

---

#### TC-X-09: Index Effectiveness
**Objective**: Verify indexes improve query performance

**Test Steps**:
1. Run analytical query without indexes:
   ```sql
   EXPLAIN ANALYZE SELECT ...
   ```
2. Note execution time and plan
3. Create indexes on foreign keys and frequently filtered columns:
   ```sql
   CREATE INDEX idx_fact_customer ON fact_orders(customer_key);
   CREATE INDEX idx_fact_date ON fact_orders(order_date_key);
   ```
4. Re-run same query
5. Compare performance

**Expected Results**:
- Indexed queries significantly faster (50%+ improvement)
- `EXPLAIN ANALYZE` shows index scans instead of sequential scans
- Join performance improved

**Pass Criteria**: Indexes demonstrably improve query performance

---

#### TC-X-10: Data Volume Scalability Test
**Objective**: Verify system handles growth to larger datasets

**Test Steps**:
1. Baseline: Current test dataset (~10K-100K rows)
2. Generate 10x larger dataset (1M+ rows)
3. Run complete pipeline
4. Compare performance metrics:
   - Ingestion time
   - Transformation time
   - Query performance
   - Dashboard load time

**Expected Results**:
- System completes successfully with 10x data
- Performance degrades gracefully (linear, not exponential)
- No memory or disk space errors
- Resource usage documented

**Pass Criteria**: System scales to 10x data volume without failure

---

### Category 10: Data Lineage & Traceability

#### TC-X-11: Source-to-Target Data Lineage
**Objective**: Trace a specific data point from source file to dashboard

**Test Steps**:
1. Select specific record from source CSV file:
   - Example: `USER001` order for Product `PROD123`
2. Trace through pipeline:
   - Find in Parquet intermediate file
   - Locate in staging database
   - Find in dimension tables (customer, product)
   - Locate in fact table
   - Verify in analytical view
   - Confirm displayed in dashboard
3. Document lineage path

**Expected Results**:
- Record traceable through all layers
- Transformations documented
- No data loss or corruption
- Values match at each stage (or transformations explained)

**Assets to Show**:
- Source record in CSV
- Same record in each layer (staging, dims, facts, view)
- Dashboard showing final aggregated value
- Lineage documentation diagram

**Pass Criteria**: Complete source-to-target lineage documented

---

#### TC-X-12: Audit Trail for Dimension Changes
**Objective**: Verify changes to dimension data are tracked

**Test Steps (if SCD Type 2 implemented)**:
1. Load initial customer dimension
2. Modify source customer data (e.g., customer changes address)
3. Re-run dimension load
4. Query dimension history:
   ```sql
   SELECT * FROM dim_customer 
   WHERE customer_id = 'USER001'
   ORDER BY effective_date;
   ```
5. Verify historical versions preserved

**Expected Results**:
- Historical records preserved (if SCD Type 2)
- Effective dates show valid time ranges
- Current record flagged (is_current = true)
- All versions queryable

**Pass Criteria**: Dimension history tracked according to SCD design

---

### Category 11: Documentation & Metadata

#### TC-X-13: Data Dictionary Completeness
**Objective**: Verify comprehensive data dictionary exists

**Test Steps**:
1. Review data dictionary document
2. Verify includes for each table/view:
   - Table name and description
   - Column names
   - Data types
   - Business definitions
   - Sample values
   - Constraints
   - Relationships
3. Cross-reference with actual database schema

**Expected Results**:
- Data dictionary covers all tables (staging, DWH, views)
- Descriptions clear for business users
- Matches actual implementation
- Includes ERD diagrams

**Pass Criteria**: Data dictionary complete and accurate

---

#### TC-X-14: README and Setup Guide Validation
**Objective**: Verify new user can setup system following documentation

**Test Steps**:
1. Fresh workstation (or Docker Desktop reset)
2. Follow README.md step-by-step
3. Attempt to:
   - Clone repository
   - Run `docker-compose up`
   - Access Airflow UI
   - Trigger DAG
   - View dashboards
4. Document any unclear steps or errors

**Expected Results**:
- README provides complete setup instructions
- All prerequisites listed
- Commands work as documented
- Screenshots aid understanding
- Troubleshooting section helps resolve common issues

**Pass Criteria**: New user successfully deploys system using only README

---

### Category 12: Advanced Analytics

#### TC-X-15: Time-Series Analysis
**Objective**: Verify date dimension enables time-based analytics

**Test Steps**:
1. Query using date dimension hierarchies:
   ```sql
   SELECT 
       d.year, d.quarter, d.month,
       SUM(f.quantity * f.price) as revenue
   FROM fact_orders f
   JOIN dim_date d ON f.order_date_key = d.date_key
   GROUP BY d.year, d.quarter, d.month
   ORDER BY d.year, d.quarter, d.month;
   ```
2. Create year-over-year comparison
3. Identify seasonal trends
4. Visualize in dashboard

**Expected Results**:
- Date dimension provides rich time hierarchies
- Year/Quarter/Month/Week/Day aggregations work
- Year-over-year growth calculable
- Time-series charts show trends clearly

**Pass Criteria**: Time-based analytics functional, trends identifiable

---

#### TC-X-16: Multi-Dimensional Analysis (OLAP)
**Objective**: Verify support for slice, dice, drill-down operations

**Test Steps**:
1. **Slice**: Filter to specific dimension value
   - Example: Sales for "Electronics" category only
2. **Dice**: Filter on multiple dimensions
   - Example: Electronics sales in Q4 2024 in North America
3. **Drill-Down**: Navigate hierarchy
   - Example: Category → Sub-category → Product
4. **Roll-Up**: Aggregate to higher level
   - Example: Daily sales → Monthly sales
5. Perform in Metabase or via SQL

**Expected Results**:
- All OLAP operations supported
- Queries performant
- Star schema enables flexible analysis
- Dashboards allow interactive exploration

**Pass Criteria**: OLAP operations (slice, dice, drill) functional

---

### Category 13: Security & Access Control

#### TC-X-17: Database Access Permissions
**Objective**: Verify appropriate database security

**Test Steps**:
1. Review database user permissions:
   ```sql
   \du  -- List users and roles
   ```
2. Verify:
   - Airflow user has full access to staging and DWH
   - Metabase user has read-only access to DWH
   - No unnecessary superuser accounts
3. Test read-only constraint:
   - Login to Metabase
   - Attempt to modify data via SQL (should fail)

**Expected Results**:
- Least privilege principle applied
- Metabase cannot write to DWH
- Airflow can read/write as needed for ETL
- Production would use stronger authentication

**Pass Criteria**: Appropriate access controls in place

---

#### TC-X-18: Sensitive Data Masking (Bonus)
**Objective**: Verify PII (Personally Identifiable Information) protected

**Test Steps**:
1. Identify PII fields (email, credit card numbers)
2. Verify masking in staging or DWH:
   - Credit card: Show last 4 digits only
   - Email: Show domain only or hash
3. Create view for analysts with masked data

**Expected Results**:
- PII masked or encrypted in DWH
- Analysts see masked version
- Original data preserved in secure staging
- Compliance with data privacy regulations

**Pass Criteria**: PII appropriately protected (if implemented)

---

### Category 14: Disaster Recovery & Backup

#### TC-X-19: Database Backup & Restore
**Objective**: Verify data can be backed up and restored

**Test Steps**:
1. Backup DWH database:
   ```bash
   docker exec shopzada-db-dwh pg_dump -U postgres shopzada_dwh > backup.sql
   ```
2. Simulate disaster (drop database)
3. Restore from backup:
   ```bash
   docker exec -i shopzada-db-dwh psql -U postgres -d shopzada_dwh < backup.sql
   ```
4. Verify data integrity post-restore

**Expected Results**:
- Backup creates successfully
- Backup file size appropriate
- Restore completes without errors
- Row counts match pre-disaster state
- Dashboards functional post-restore

**Pass Criteria**: Database successfully backed up and restored

---

#### TC-X-20: Pipeline Re-Executability
**Objective**: Verify pipeline can be re-run without corrupting data

**Test Steps**:
1. Run complete pipeline (initial load)
2. Without clearing data, re-run pipeline
3. Verify behavior:
   - Idempotent operations (running twice yields same result)
   - OR proper handling of duplicates

**Expected Results**:
- Pipeline handles re-runs gracefully
- Options:
  - Data replaced (truncate/load)
  - Upserts (insert or update)
  - Duplicate detection
- No data duplication errors
- Referential integrity maintained

**Pass Criteria**: Pipeline re-runnable without data corruption

---

## Test Data Requirements

### Test Dataset Specifications

#### Minimal Test Dataset
For quick smoke tests (TC-E-03 to TC-E-06):
- **Orders**: 100 rows
- **Customers**: 50 unique users
- **Products**: 20 products
- **Merchants**: 10 merchants
- **Campaigns**: 5 campaigns
- **Time Range**: 1 month

#### Standard Test Dataset
For full functional tests (TC-E-07 to TC-E-18):
- **Orders**: 10,000 rows
- **Customers**: 5,000 unique users
- **Products**: 500 products
- **Merchants**: 100 merchants
- **Campaigns**: 20 campaigns
- **Time Range**: 1 year (2024)

#### Large-Scale Test Dataset
For performance tests (TC-X-08 to TC-X-10):
- **Orders**: 1,000,000 rows
- **Customers**: 50,000 unique users
- **Products**: 5,000 products
- **Merchants**: 1,000 merchants
- **Campaigns**: 100 campaigns
- **Time Range**: 3 years (2022-2024)

### Data Generator Script

```python
# scripts/generate_test_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_test_dataset(size='minimal'):
    """
    Generate test datasets for ShopZada DWH
    size: 'minimal', 'standard', or 'large'
    """
    sizes = {
        'minimal': {'orders': 100, 'customers': 50, 'products': 20, 'merchants': 10, 'campaigns': 5},
        'standard': {'orders': 10000, 'customers': 5000, 'products': 500, 'merchants': 100, 'campaigns': 20},
        'large': {'orders': 1000000, 'customers': 50000, 'products': 5000, 'merchants': 1000, 'campaigns': 100}
    }
    
    config = sizes[size]
    output_dir = f'data/test_cases/dataset_{size}/'
    
    # Generate customers
    customers = pd.DataFrame({
        'user_id': [f'USER{i:06d}' for i in range(config['customers'])],
        'name': [f'Customer {i}' for i in range(config['customers'])],
        'email': [f'user{i}@example.com' for i in range(config['customers'])],
        'age': np.random.randint(18, 80, config['customers']),
        'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], config['customers'])
    })
    customers.to_csv(f'{output_dir}customers.csv', index=False)
    
    # Similar for products, merchants, campaigns, orders...
    print(f"Generated {size} dataset in {output_dir}")

if __name__ == '__main__':
    generate_test_dataset('standard')
```

---

## Recording Requirements

### Video Recording Specifications

#### Technical Requirements
- **Resolution**: Minimum 1080p (1920x1080)
- **Frame Rate**: 30 FPS minimum
- **Format**: MP4 or WebM
- **Audio**: Clear narration (spoken explanation of steps)
- **Duration**: No artificial time limits, record complete sequence

#### Recording Tools (Options)
- **Windows**: OBS Studio, Camtasia, ShareX
- **macOS**: QuickTime, ScreenFlow, OBS Studio
- **Linux**: SimpleScreenRecorder, OBS Studio
- **Browser-based**: Loom (for web UI portions)

#### What to Record

**Essential Elements in Every Recording:**
1. **Introduction**:
   - State test case ID and name
   - Brief objective explanation
2. **Setup**:
   - Show initial state (clean database, file location)
   - Prepare test data (show file contents)
3. **Execution**:
   - Run commands (visible in terminal)
   - Trigger workflows (show Airflow UI)
   - Monitor progress (logs, status updates)
4. **Verification**:
   - Query databases (show results)
   - Check dashboards (data displayed correctly)
   - Validate metrics (row counts, aggregations)
5. **Debugging** (if errors occur):
   - Show error messages
   - Explain issue
   - Apply fix
   - Re-run to demonstrate success
6. **Conclusion**:
   - Summarize pass/fail result
   - Note any observations

#### Audio Narration Script Template

```
[Introduction]
"This is test case <TC-ID>: <Test Name>.
The objective is to verify <objective description>.

[Setup]
I'm starting with <initial state>.
Here's the test data I've prepared: <show file/data>.

[Execution]
Now I'm running <command/triggering workflow>.
As you can see, the system is <describe what's happening>.

[Verification]
Let me verify the results by <querying database/checking dashboard>.
The expected result was <X>, and we can see <actual result>.

[Conclusion]
This test case <PASSES/FAILS> because <reason>.
```

### File Organization

```
recordings/
├── essential/
│   ├── TC-E-01_Docker_Environment_Startup.mp4
│   ├── TC-E-02_Database_Connectivity.mp4
│   └── ...
└── extensive/
    ├── TC-X-01_Empty_Dataset_Handling.mp4
    ├── TC-X-02_Data_Type_Mismatch.mp4
    └── ...
```

### Submission Format

**Each test case recording should include:**
- Video file (MP4)
- Spoken narration explaining steps
- Visible timestamps
- Clear demonstration of pass/fail result

**Naming Convention**: `<TC-ID>_<Test_Name>.mp4`

---

## Summary

This comprehensive test suite covers:

### Part A: Essential Test Cases (18 tests)
Core functionality that MUST work for project success:
- ✅ Infrastructure deployment (3 tests)
- ✅ Data ingestion & loading (3 tests)
- ✅ Workflow orchestration (2 tests)
- ✅ Data transformation (2 tests)
- ✅ Dimensional model (3 tests)
- ✅ Analytics & dashboards (4 tests)
- ✅ End-to-end integration (1 test)

### Part B: Extensive Test Cases (20 tests)
Robustness, edge cases, and completeness:
- ✅ Edge cases & error handling (7 tests)
- ✅ Performance & scalability (3 tests)
- ✅ Data lineage & traceability (2 tests)
- ✅ Documentation & metadata (2 tests)
- ✅ Advanced analytics (2 tests)
- ✅ Security & access control (2 tests)
- ✅ Disaster recovery (2 tests)

**Total: 38 comprehensive test cases**

---

## Appendix: Quick Reference

### Test Execution Checklist

Before starting test execution:
- [ ] Docker Desktop running
- [ ] All containers healthy (`docker-compose ps`)
- [ ] Airflow UI accessible
- [ ] Metabase UI accessible
- [ ] Test data prepared
- [ ] Screen recording configured
- [ ] Sufficient disk space (10GB+)
- [ ] Internet connection stable

### Common Commands Reference

**Docker Operations:**
```bash
# Start all services
docker-compose -f ./infra/docker-compose.yml up -d

# Check status
docker-compose -f ./infra/docker-compose.yml ps

# View logs
docker-compose -f ./infra/docker-compose.yml logs -f <service_name>

# Stop all services
docker-compose -f ./infra/docker-compose.yml down

# Clean volumes (reset databases)
docker-compose -f ./infra/docker-compose.yml down -v
```

**Database Access:**
```bash
# Access DWH database
docker exec -it shopzada-db-dwh psql -U postgres -d shopzada_dwh

# Access staging database
docker exec -it shopzada-db-staging psql -U postgres -d shopzada_staging

# Run SQL from command line
docker exec -it shopzada-db-dwh psql -U postgres -d shopzada_dwh -c "SELECT COUNT(*) FROM dim_customer;"
```

**Airflow Access:**
- URL: `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`

**Metabase Access:**
- URL: `http://localhost:3000`
- Initial setup required on first access

---

**END OF TEST CASE DOCUMENTATION**
