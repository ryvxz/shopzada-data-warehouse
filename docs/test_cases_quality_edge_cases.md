# ShopZada Data Warehouse - Quality & Edge Case Test Suite
## Comprehensive Data Quality Testing Framework

**Date:** December 2025  
**Purpose:** Validate data quality handling, edge cases, and schema evolution capabilities

---

## Table of Contents

1. [Overview](#overview)
2. [Test Data Generation](#test-data-generation)
3. [Quality Test Cases (TC-Q Series)](#quality-test-cases-tc-q-series)
4. [Schema Drift Test Cases (TC-S Series)](#schema-drift-test-cases-tc-s-series)
5. [Integration with Existing Tests](#integration-with-existing-tests)

---

## Overview

### Purpose

This test suite extends the essential test cases (TC-E series) to comprehensively validate the data warehouse's ability to:

- **Detect data quality issues** (nulls, invalid types, duplicates)
- **Handle edge cases** (boundary values, extreme data)
- **Adapt to schema changes** (column additions, renames, type changes)
- **Report issues accurately** (comprehensive quality reporting)
- **Implement proper error handling** (graceful degradation vs. hard failures)

### Test Philosophy

> [!IMPORTANT]
> **These tests use "chaos engineering" principles** - intentionally injecting known issues to validate that:
> 1. Quality checks **detect** the problems
> 2. Pipeline **handles** them appropriately
> 3. Issues are **reported** clearly
> 4. Valid data continues to **process** successfully

---

## Test Data Generation

### Generating Edge Case Data

Use the enhanced test data generator to create datasets with intentional quality issues:

```bash
# Generate all edge case scenarios
python scripts/generate_test_data_edge_cases.py --scenario all --output data/test_cases/quality_suite/

# Generate specific scenario types
python scripts/generate_test_data_edge_cases.py --scenario nulls --output data/test_cases/TC-Q-01/
python scripts/generate_test_data_edge_cases.py --scenario invalid_types --output data/test_cases/TC-Q-02/
python scripts/generate_test_data_edge_cases.py --scenario duplicates --output data/test_cases/TC-Q-03/
python scripts/generate_test_data_edge_cases.py --scenario boundary_values --output data/test_cases/TC-Q-04/
python scripts/generate_test_data_edge_cases.py --scenario schema_drift --output data/test_cases/TC-Q-05/
```

### Generated Issue Summary

Each test run generates a `QUALITY_ISSUES_REPORT.csv` documenting all intentionally injected issues:

| Issue Category | Count | Severity Levels | Detection Method |
|---------------|-------|-----------------|------------------|
| NULL_VALUES | ~25 | CRITICAL, HIGH, MEDIUM | Null checks, required field validation |
| TYPE_MISMATCH | ~35 | CRITICAL, HIGH | Type validation, parsing errors |
| DUPLICATES | ~40 | CRITICAL, HIGH, MEDIUM | Primary key constraints, deduplication |
| BOUNDARY_VALUE | ~15 | CRITICAL, HIGH, MEDIUM | Range checks, business rule validation |
| SCHEMA_DRIFT | ~300 | CRITICAL, HIGH, LOW | Schema comparison, column mapping |

---

## Quality Test Cases (TC-Q Series)

> [!NOTE]
> All TC-Q test cases should be run with the edge case test data generator and **must demonstrate**:
> 1. Issue detection (quality check identifies the problem)
> 2. Appropriate handling (fail, warn, clean, or skip)
> 3. Accurate reporting (issue logged with context)

---

### TC-Q-01: NULL/Blank Value Detection

**Objective**: Verify pipeline detects and handles NULL/blank values in critical fields

**Test Data**: Generate with `--scenario nulls`

**Intentional Issues Injected**:
- 10% NULL `user_id` (primary keys)
- 12.5% NULL `email` addresses
- 14% blank/empty `name` fields
- 6.7% NULL `age` values
- 5% NULL date values
- String representations of null: "NULL", "null", "N/A", "None", ""

**Test Steps**:

1. Generate test data:
   ```bash
   python scripts/generate_test_data_edge_cases.py --scenario nulls --output data/test_cases/TC-Q-01/
   ```

2. Review the quality report to see injected issues:
   ```bash
   cat data/test_cases/TC-Q-01/QUALITY_ISSUES_REPORT.csv
   ```

3. Configure pipeline to use test data directory

4. Run Airflow DAG with test data

5. Monitor `data_quality_checks_and_report` task execution

6. Verify quality report identifies issues:
   ```sql
   -- Check if quality report table exists
   SELECT * FROM data_quality_report WHERE check_date = CURRENT_DATE;
   
   -- Verify null detection
   SELECT 
       column_name,
       null_count,
       total_rows,
       (null_count::float / total_rows * 100) as null_percentage
   FROM column_quality_metrics
   WHERE null_count > 0;
   ```

**Expected Results**:

✅ **Detection**:
- Quality check identifies ~10 NULL user_ids (CRITICAL)
- Quality check identifies ~13 NULL emails (HIGH)
- Quality check identifies ~14 blank names (MEDIUM)
- Report shows null counts match injected numbers (±1)

✅ **Handling**:
- Records with NULL primary keys → REJECTED/QUARANTINED
- Records with NULL email → WARNED but may process (depending on requirements)
- Blank names → CLEANED (set to default) or WARNED
- Summary shows: X records rejected, Y records cleaned, Z records processed

✅ **Reporting**:
- Quality report generated with detailed null analysis
- Each field's null count and percentage documented
- Severity assigned correctly (PK nulls = CRITICAL)
- Examples of problematic records included in report

**Pass Criteria**: 
- All 6 NULL issue types detected
- CRITICAL nulls (user_id) cause record rejection
- Quality report accurately reflects injected issue counts (within ±2 records)
- Valid records (non-null) process successfully

**Assets to Show**:
- `QUALITY_ISSUES_REPORT.csv` showing injected issues
- Airflow quality check task logs
- Quality report from pipeline
- SQL queries showing rejected/quarantined records
- Comparison: injected count vs. detected count

---

### TC-Q-02: Invalid Data Type Detection

**Objective**: Verify pipeline detects type mismatches and parsing errors

**Test Data**: Generate with `--scenario invalid_types`

**Intentional Issues Injected**:
- 10% string values in numeric `total_amount` field ("NOT_A_NUMBER")
- 12% invalid date formats ("2024-13-45")
- 8% numeric values in string `order_id` field
- 7% boolean values where status string expected
- 14% strings in `quantity` field ("five")
- 11% currency strings in `price` field ("19.99USD")
- Floats where integers expected (3.14159 for quantity)

**Test Steps**:

1. Generate test data:
   ```bash
   python scripts/generate_test_data_edge_cases.py --scenario invalid_types --output data/test_cases/TC-Q-02/
   ```

2. Run ingestion with type validation enabled

3. Monitor parsing errors and type coercion attempts

4. Verify failed type conversions are logged:
   ```sql
   -- Check for type conversion errors
   SELECT * FROM data_quality_errors 
   WHERE error_type = 'TYPE_MISMATCH'
   ORDER BY severity DESC;
   ```

**Expected Results**:

✅ **Detection**:
- "NOT_A_NUMBER" in amount field → parsing error detected
- Invalid dates "2024-13-45" → date validation fails
- Type mismatches logged with specific field and value

✅ **Handling**:
- Records with unparseable amounts → REJECTED
- Invalid dates → REJECTED or set to NULL with warning  
- String quantities → Attempt conversion, fail if non-numeric
- Currency strings → Strip currency symbols, convert to decimal

✅ **Reporting**:
- Type error report includes: field name, expected type, actual value, row number
- Grouped by error type for easy analysis
- Examples of each type error preserved

**Pass Criteria**:
- All 7 type mismatch scenarios detected
- Parsing errors logged accurately
- Type coercion attempted where safe (e.g., "19.99USD" → 19.99)
- Hard failures for critical type errors (amounts, dates)

**Assets to Show**:
- Sample data showing type mismatches
- Airflow logs showing type conversion attempts/failures
- Type error report from pipeline
- Rejected records table with rejection reason

---

### TC-Q-03: Duplicate Record Detection

**Objective**: Verify pipeline identifies and handles duplicate records

**Test Data**: Generate with `--scenario duplicates`

**Intentional Issues Injected**:
- 10 exact duplicate rows (all fields identical)
- 5 duplicate primary keys with different data (UPDATE scenario)
- 8 near-duplicates (same name/attributes, different ID)
- 15 duplicate `order_id` entries in transaction log

**Test Steps**:

1. Generate test data:
   ```bash
   python scripts/generate_test_data_edge_cases.py --scenario duplicates --output data/test_cases/TC-Q-03/
   ```

2. Run pipeline with deduplication logic

3. Check for duplicate detection:
   ```sql
   -- Find exact duplicates
   SELECT product_id, product_name, COUNT(*) as duplicate_count
   FROM staging.products
   GROUP BY product_id, product_name
   HAVING COUNT(*) > 1;
   
   -- Find PK duplicates with different data
   SELECT product_id, array_agg(DISTINCT product_name) as different_names
   FROM staging.products
   GROUP BY product_id
   HAVING COUNT(DISTINCT product_name) > 1;
   ```

4. Verify deduplication strategy applied

**Expected Results**:

✅ **Detection**:
- 10 exact duplicates identified
- 5 primary key conflicts detected (CRITICAL)
- 15 duplicate order IDs flagged
- Near-duplicates identified via fuzzy matching (optional advanced feature)

✅ **Handling**:
- Exact duplicates → Keep one, discard rest (deterministic selection)
- PK duplicates with diff data → FAIL or apply merge/update logic
- Duplicate order IDs → Investigate (possible system error)
- Deduplication report shows: X duplicates found, Y records kept, Z records discarded

✅ **Reporting**:
- Duplicate detection report lists all duplicate groups
- For each group: which record was kept, which were discarded
- Reason for selection (e.g., "most recent timestamp", "highest data completeness")

**Pass Criteria**:
- All 3 duplicate types detected (exact, PK conflict, near-duplicate)
- Deduplication logic applied consistently
- Duplicate groups clearly documented
- Final dataset has no duplicate primary keys

**Assets to Show**:
- Pre-deduplication row count
- Duplicate detection queries showing matches
- Deduplication report
- Post-deduplication row count (matches expected after removing duplicates)
- Sample of kept vs. discarded records

---

### TC-Q-04: Boundary Value Handling

**Objective**: Verify pipeline handles extreme and edge case values appropriately

**Test Data**: Generate with `--scenario boundary_values`

**Intentional Issues Injected**:

**Price/Amount Boundaries**:
- 1 product with price = $0.00 (free product?)
- 1 product with negative price (-$10.50)
- 1 product with extremely high price ($999,999.99)
- 1 product where cost > price (negative margin)

**Quantity Boundaries**:
- 1 order with quantity = 0 (empty order)
- 1 order with negative quantity (-1, potential return)
- 1 order with unrealistic quantity (999,999)

**Stock Boundaries**:
- 1 product with stock = 0 (out of stock)
- 1 product with negative stock (-5)

**Discount Boundaries**:
- 1 order with 100% discount (free)
- 1 order with >100% discount (150%, INVALID)

**Date/Age Boundaries**:
- 1 customer with future registration date (2099-12-31)
- 1 customer with ancient date (1900-01-01)
- 1 customer age < 13 (underage)
- 1 customer age = 0
- 1 customer age = 150 (unrealistic)
-  1 customer with order_date < registration_date (time paradox)

**Test Steps**:

1. Generate test data:
   ```bash
   python scripts/generate_test_data_edge_cases.py --scenario boundary_values --output data/test_cases/TC-Q-04/
   ```

2. Run pipeline with business rule validation

3. Check boundary value detection:
   ```sql
   -- Negative or zero prices
   SELECT product_id, product_name, price 
   FROM staging.products 
   WHERE price <= 0;
   
   -- Unrealistic ages
   SELECT user_id, name, age 
   FROM staging.customers 
   WHERE age < 13 OR age > 120 OR age = 0;
   
   -- Cost exceeds price
   SELECT product_id, price, cost, (price - cost) as margin
   FROM staging.products
   WHERE cost > price;
   
   -- Temporal anomalies
   SELECT user_id, registration_date, last_order_date
   FROM staging.customers
   WHERE last_order_date < registration_date;
   ```

**Expected Results**:

✅ **Detection**:
- All boundary violations flagged by business rule checks
- Range violations categorized by severity
- Boundary report shows: field, value, expected range, severity

✅ **Handling**:
- Zero prices → WARN (may be valid promo) or REJECT (depends on business rules)
- Negative prices → REJECT (CRITICAL error)
- Negative quantities → Interpret as returns or REJECT
- Discount > 100% → REJECT (invalid)
- Future dates → REJECT
- Age < 13 → REJECT or FLAG for review (COPPA compliance)
- Temporal anomalies → REJECT (data integrity violation)

✅ **Reporting**:
- Boundary violation report with all detected cases
- Business rule validation summary (X rules checked, Y violations found)
- Recommendations for each boundary case

**Pass Criteria**:
- All 15 boundary scenarios detected
- CRITICAL boundaries (negative prices, >100% discount, future dates) cause rejection
- MEDIUM boundaries (zero stock, high prices) logged with warnings
- Business rules enforced consistently

**Assets to Show**:
- Boundary value test data samples
- Business rule validation queries
- Boundary violation report
- Comparison: before/after business rule application

---

## Schema Drift Test Cases (TC-S Series)

> [!WARNING]
> Schema drift tests simulate **real-world scenarios** where source systems change their data structure without notice. Your pipeline must handle these gracefully or fail informatively.

---

### TC-S-01: Column Rename Detection

**Objective**: Verify pipeline handles column renames in source data

**Test Data**: `customers_schema_v2_renamed_column.csv`
- Original column: `name`
- Renamed to: `full_name`

**Test Steps**:

1. Generate schema drift data:
   ```bash
   python scripts/generate_test_data_edge_cases.py --scenario schema_drift --output data/test_cases/TC-S-01/
   ```

2. First, load original schema (v1):
   ```bash
   # Use customers_schema_v1.csv
   cp data/test_cases/TC-S-01/customers_schema_v1.csv data/raw/customers.csv
   ```

3. Run pipeline, verify successful load

4. Replace with renamed schema (v2):
   ```bash
   cp data/test_cases/TC-S-01/customers_schema_v2_renamed_column.csv data/raw/customers.csv
   ```

5. Run pipeline again

6. Monitor schema validation task

**Expected Results**:

✅ **Detection**:
- Schema change detected: column `name` not found
- New column `full_name` identified
- Schema diff report generated showing: "Column 'name' removed, column 'full_name' added"

✅ **Handling** (Choose appropriate strategy):
- **Strategy A**: FAIL with clear error message ("Expected column 'name' not found")
- **Strategy B**: Auto-map renamed column if obvious (name→full_name)
- **Strategy C**: Load to separate staging table for review

✅ **Reporting**:
- Schema change alert raised
- Diff report shows before/after schemas
- Recommendation: update column mapping configuration

**Pass Criteria**:
- Schema change detected before data loading
- Pipeline fails gracefully OR successfully maps renamed column
- Clear error message/report indicating the schema mismatch
- No data corruption (partial loads rolled back)

**Assets to Show**:
- Schema manifest showing v1 vs v2 differences
- Schema validation log
- Error message or mapping report
- Resolution: column mapping configuration update

---

### TC-S-02: Column Dropped Detection

**Objective**: Verify pipeline handles missing columns in source data

**Test Data**: `customers_schema_v3_dropped_column.csv`
- Dropped column: `age`

**Test Steps**:

1. Load data with dropped column
2. Verify schema validation detects missing field
3. Check handling of missing column data

**Expected Results**:

✅ **Detection**:
- Missing column `age` detected
- Schema validation reports: "Expected column 'age' not found in source"

✅ **Handling**:
- If `age` is REQUIRED → FAIL load
- If `age` is OPTIONAL → Load with NULL/default value for age
- Warning logged regardless

✅ **Reporting**:
- Missing column report
- Impact analysis (e.g., "Age-based analytics will be affected")

**Pass Criteria**:
- Missing column detected during schema validation
- Appropriate action based on column criticality
- Downstream impacts documented

---

### TC-S-03: New Column Addition Detection

**Objective**: Verify pipeline handles unexpected new columns

**Test Data**: `customers_schema_v4_new_columns.json`
- New columns: `country`, `loyalty_tier`

**Test Steps**:

1. Load data with additional columns
2. Verify new columns handling

**Expected Results**:

✅ **Detection**:
- New columns `country` and `loyalty_tier` detected
- Schema validation reports: "Unexpected columns found"

✅ **Handling**:
- **Permissive mode**: Load all columns, add to staging table dynamically
- **Strict mode**: Ignore unexpected columns, load only expected fields
- Warning logged for new columns

✅ **Reporting**:
- New column discovery report
- Data sample for new columns
- Recommendation: update schema definition if columns valuable

**Pass Criteria**:
- New columns detected
- Data loads successfully (unexpected columns handled gracefully)
- New columns documented for review

---

### TC-S-04: Data Type Change Detection

**Objective**: Verify pipeline detects column type changes

**Test Data**: `customers_schema_v5_type_changes.xlsx`
- `user_id`: string → integer
- `age`: integer → string

**Test Steps**:

1. Load data with type changes
2. Monitor type validation

**Expected Results**:

✅ **Detection**:
- Type change detected for `user_id` (string → int)
- Type change detected for `age` (int → string)
- Type compatibility check performed

✅ **Handling**:
- `user_id` as int → May succeed if convertible, but breaks convention
- `age` as string → Type coercion attempted ("25" → 25)
- Incompatible type changes → FAIL

✅ **Reporting**:
- Type change report with before/after types
- Coercion success/failure logged

**Pass Criteria**:
- Type changes detected via schema comparison
- Safe coercions succeed (string "25" → int 25)
- Unsafe changes fail with clear error
- Type mismatch report generated

---

### TC-S-05: Column Reordering Handling

**Objective**: Verify pipeline handles column order changes

**Test Data**: `customers_schema_v6_reordered.csv`
- Same columns, different order: `[email, user_id, city, age, name]`

**Test Steps**:

1. Load data with reordered columns
2. Verify correct column mapping

**Expected Results**:

✅ **Detection**:
- Column order change detected (informational)

✅ **Handling**:
- Columns mapped by NAME, not position → SUCCESS
- Data loaded correctly regardless of order

✅ **Reporting**:
- Informational log: "Column order changed (handled gracefully)"

**Pass Criteria**:
- Pipeline handles column reordering without issues
- Data maps correctly by column name
- No data in wrong columns (email data doesn't end up in user_id field)

**Assets to Show**:
- Schema comparison showing order change
- Sample records validating correct mapping
- SQL query: `SELECT user_id, email, name FROM staging.customers LIMIT 5` (verify data is correct)

---

### TC-S-06: Breaking Schema Change Detection

**Objective**: Verify pipeline properly fails on incompatible schema

**Test Data**: `customers_schema_v7_completely_different.parquet`
- Completely different schema: `[customer_identifier, personal_info, contact_email, location, member_since, status]`

**Test Steps**:

1. Attempt to load completely different schema
2. Verify graceful failure

**Expected Results**:

✅ **Detection**:
- Massive schema mismatch detected
- No matching columns found (or <50% match)
- Schema validation FAILS

✅ **Handling**:
- Pipeline FAILS load operation
- Clear error message: "Schema incompatible - expected columns [A,B,C], found [X,Y,Z]"
- No partial data loaded (transaction rolled back)

✅ **Reporting**:
- Schema incompatibility report
- Side-by-side expected vs. actual schema
- Recommendation: "Manual intervention required - source schema changed dramatically"

**Pass Criteria**:
- Pipeline FAILS (does not attempt to load)
- Error message clearly describes incompatibility
- No data corruption (staging table unchanged)
- Schema diff report available for troubleshooting

**Assets to Show**:
- Error message from schema validation
- Schema comparison report
- Verification that staging table is unmodified

---

## Integration with Existing Tests

### Combining Quality Tests with E2E Tests

The quality test cases (TC-Q and TC-S series) should be integrated into your complete testing workflow:

```
┌─────────────────────────────────────────┐
│  PHASE 1: Infrastructure (TC-E-01, 02)  │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  PHASE 2: Clean Data Pipeline (TC-E-03  │
│  through TC-E-11) - Validate with       │
│  standard test data                     │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  PHASE 3: Quality Testing (TC-Q-01      │
│  through TC-Q-04) - Validate with       │
│  edge case data                         │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  PHASE 4: Schema Evolution (TC-S-01     │
│  through TC-S-06) - Validate schema     │
│  drift handling                         │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  PHASE 5: Dashboards & Analytics        │
│  (TC-E-13 through TC-E-17)              │
└─────────────────────────────────────────┘
```

### Testing Checklist

Use this checklist to track your quality testing progress:

#### Data Quality Tests
- [ ] TC-Q-01: NULL/Blank value detection and handling
- [ ] TC-Q-02: Invalid data type detection
- [ ] TC-Q-03: Duplicate record detection and deduplication
- [ ] TC-Q-04: Boundary value validation

#### Schema Drift Tests
- [ ] TC-S-01: Column rename detection
- [ ] TC-S-02: Column drop detection
- [ ] TC-S-03: New column addition handling
- [ ] TC-S-04: Data type change detection
- [ ] TC-S-05: Column reordering handling
- [ ] TC-S-06: Breaking schema change failure

#### Quality Reporting
- [ ] Quality reports generated for each test
- [ ] Issue counts match injected problems (±2 tolerance)
- [ ] Severity classification correct (CRITICAL, HIGH, MEDIUM, LOW)
- [ ] Rejected/quarantined records tracked
- [ ] Clean records processed successfully

---

## Test Execution Best Practices

### 1. Isolation

Run quality tests in isolated environment:
```bash
# Stop existing containers
docker-compose -f infra/docker-compose.yml down

# Clear test data
rm -rf data/test_cases/TC-Q-*/
rm -rf data/preprocessed/*

# Restart fresh
docker-compose -f infra/docker-compose.yml up -d
```

### 2. Incremental Testing

Test one scenario at a time:
```bash
# Test nulls first
python scripts/generate_test_data_edge_cases.py --scenario nulls --output data/test_cases/TC-Q-01/
# Run pipeline, verify results

# Then test invalid types
python scripts/generate_test_data_edge_cases.py --scenario invalid_types --output data/test_cases/TC-Q-02/
# Run pipeline, verify results

# Continue...
```

### 3. Baseline Comparison

Establish baseline with clean test data first:
```bash
# Generate clean baseline
python scripts/generate_test_data.py --size minimal --output data/test_cases/baseline/

# Run pipeline - should have 100% success rate
# Record: processing time, record counts, no quality issues

# Then introduce edge cases and compare
```

### 4. Documentation

For each test, document:
- **Test data used** (which scenario, how generated)
- **Issues injected** (number and type)
- **Issues detected** (from quality report)
- **Detection rate** (detected count / injected count * 100%)
- **Handling action** (rejected, cleaned, warned, passed)
- **Screenshots/logs** (evidence of detection and handling)

---

## Success Metrics

### Overall Quality Testing Success Criteria

To fully pass the quality test suite, your data warehouse must achieve:

✅ **Detection Accuracy**: >95% of injected issues detected
- NULL values: 100% detection for CRITICAL fields (PK, FK)
- Type mismatches: 100% detection
- Duplicates: >98% detection (some near-duplicates may be challenging)
- Boundary violations: >90% detection (business rule dependent)

✅ **Appropriate Handling**:
- CRITICAL issues → Records rejected/quarantined (100%)
- HIGH issues → Records warned or cleaned (>90%)
- MEDIUM issues → Logged, attempt cleanup (best effort)
- LOW issues → Handled gracefully, informational logging

✅ **Complete Reporting**:
- Quality report generated for every run
- All detected issues documented with: field, value, issue type, severity, action taken
- Summary statistics: total records, clean records, rejected records, cleaned records
- Issue categorization by type and severity

✅ **Schema Evolution Handling**:
- Column renames → Detected and either failed gracefully or mapped correctly
- Column drops → Detected, fail if required, default if optional
- New columns → Detected, handled per configuration (strict vs. permissive)
- Type changes → Detected, safe coercions succeed, unsafe fail
- Column reorder → Handled transparently (map by name)
- Breaking changes → Hard failure with clear diagnostics

✅ **No Data Corruption**:
- Clean records process successfully (100% of valid data loaded)
- Rejected records properly quarantined (not silently dropped)
- No partial loads on error (transaction integrity)
- Idempotent reruns (same result if run multiple times)

---

## Appendix: Testing Tools

### Useful SQL Queries for Quality Verification

```sql
-- Overall quality metrics
SELECT 
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE status = 'VALID') as clean_records,
    COUNT(*) FILTER (WHERE status = 'REJECTED') as rejected_records,
    COUNT(*) FILTER (WHERE status = 'CLEANED') as cleaned_records,
    COUNT(*) FILTER (WHERE status = 'WARNED') as warned_records
FROM data_quality_staging;

-- Null analysis by column
SELECT 
    column_name,
    COUNT(*) FILTER (WHERE column_value IS NULL) as null_count,
    COUNT(*) as total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE column_value IS NULL) / COUNT(*), 2) as null_percentage
FROM (
    SELECT user_id as column_value, 'user_id' as column_name FROM staging.customers
    UNION ALL
    SELECT email, 'email' FROM staging.customers
    UNION ALL
    SELECT name, 'name' FROM staging.customers
) t
GROUP BY column_name
ORDER BY null_percentage DESC;

-- Duplicate detection
WITH duplicate_check AS (
    SELECT 
        product_id,
        COUNT(*) as occurrence_count
    FROM staging.products
    GROUP BY product_id
    HAVING COUNT(*) > 1
)
SELECT 
    p.*,
    dc.occurrence_count
FROM staging.products p
JOIN duplicate_check dc ON p.product_id = dc.product_id
ORDER BY p.product_id, p.product_name;

-- Boundary violations
SELECT 'Negative Prices' as issue, COUNT(*) as count
FROM staging.products WHERE price < 0
UNION ALL
SELECT 'Zero Prices', COUNT(*) FROM staging.products WHERE price = 0
UNION ALL
SELECT 'Negative Stock', COUNT(*) FROM staging.products WHERE stock < 0
UNION ALL
SELECT 'Cost > Price', COUNT(*) FROM staging.products WHERE cost > price
UNION ALL
SELECT 'Discount > 100%', COUNT(*) FROM staging.orders WHERE discount > 100;

-- Schema comparison
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'staging'
    AND table_name = 'customers'
ORDER BY ordinal_position;
```

### Python Validation Script

Create `scripts/validate_quality_tests.py`:

```python
import pandas as pd
import json

def compare_injected_vs_detected(issues_report_path, quality_report_path):
    """Compare injected issues with detected issues"""
    
    injected = pd.read_csv(issues_report_path)
    detected = pd.read_csv(quality_report_path)
    
    comparison = []
    for _, issue in injected.iterrows():
        category = issue['category']
        injected_count = issue['count']
        
        detected_match = detected[detected['issue_type'] == category]
        detected_count = detected_match['count'].sum() if not detected_match.empty else 0
        
        detection_rate = (detected_count / injected_count * 100) if injected_count > 0 else 0
        
        comparison.append({
            'category': category,
            'injected': injected_count,
            'detected': detected_count,
            'detection_rate': f"{detection_rate:.1f}%",
            'status': '✅ PASS' if detection_rate >= 95 else '❌ FAIL'
        })
    
    df_comparison = pd.DataFrame(comparison)
    print(df_comparison.to_markdown(index=False))
    
    return df_comparison

# Usage
compare_injected_vs_detected(
    'data/test_cases/TC-Q-01/QUALITY_ISSUES_REPORT.csv',
    'data/reports/quality_report_latest.csv'
)
```

---

## Conclusion

This quality and edge case test suite provides **comprehensive validation** of your data warehouse's robustness. By intentionally injecting known issues, you can verify that your quality checks, error handling, and schema validation logic work correctly.

> [!TIP]
> **Start with one scenario** (e.g., nulls), verify detection works, then expand to other scenarios. Quality testing is iterative - refine your checks based on what you learn from each test run.

**Remember**: The goal is not to have zero issues in production, but to have **comprehensive detection and appropriate handling** when issues occur. These tests help you build confidence that your data warehouse will behave predictably when faced with real-world data quality problems.
