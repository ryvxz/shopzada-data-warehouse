# ShopZada Data Warehouse
## Project Presentation

**Presented by:** Paul Aldrich Pimentel  
**Date:** December 2025  
**Methodology:** Kimball Dimensional Modeling

---

<!-- slide 1 -->
# Slide 1: Title Slide

## ShopZada Data Warehouse Project

**Building an Enterprise-Grade Analytical Platform**

### Project Team
- Paul Aldrich Pimentel - *Project Manager*
- Ken Fajardo - *Workflow Orchestration Engineer*
- Joseph Daniel Mamangun - *Data Architect*
- Jodimeer Ammang - *Infrastructure Engineer*
- Aaron Cuevas - *ETL Engineer*
- Justin Lloyd Floro - *Data Engineer*
- Nigel Anunciacion - *BI Developer*

*December 2025*

---

<!-- slide 2 -->
# Slide 2: Agenda

## Presentation Overview

1. **Project Overview** - What & Why
2. **Business Problem** - The Challenge
3. **Solution Architecture** - Our Approach
4. **Technical Infrastructure** - Technology Stack
5. **Data Pipeline** - ETL Process
6. **Data Model** - Dimensional Design
7. **Current Progress** - What's Done
8. **Dashboards & Analytics** - Business Value
9. **Timeline & Milestones** - Project Plan
10. **Next Steps** - Future Work
11. **Q&A** - Questions

---

<!-- slide 3 -->
# Slide 3: Project Overview

## What is ShopZada Data Warehouse?

An **enterprise-grade analytical data platform** that consolidates, transforms, and analyzes ShopZada's e-commerce business data.

### Key Objectives
✅ **Consolidate** disparate data sources into a single source of truth  
✅ **Enable** historical trend analysis and reporting  
✅ **Support** business intelligence and analytics  
✅ **Improve** data quality and consistency  
✅ **Accelerate** time-to-insight for stakeholders

### Methodology
**Kimball Dimensional Modeling** - Industry-standard approach for data warehouse design using star schemas

---

<!-- slide 4 -->
# Slide 4: The Business Problem

## Challenges ShopZada Faces

### Data Fragmentation 🔍
- **20+ raw data files** in 7 different formats (CSV, JSON, Excel, HTML, Parquet, Pickle)
- Data scattered across customer, operations, marketing, and enterprise domains
- No unified view of business performance

### Limited Analytics 📊
- Hard to answer simple questions like "What are our top products?"
- No historical trend analysis
- Manual data compilation is time-consuming and error-prone

### Data Quality Issues ⚠️
- Inconsistent formats and standards
- Missing data validation
- No single source of truth

---

<!-- slide 5 -->
# Slide 5: The Solution

## Our Data Warehouse Approach

### Transform This...

**Before:**
```
❌ 20+ files in different formats
❌ Manual Excel analysis
❌ Hours to answer basic questions
❌ Inconsistent data quality
❌ No historical tracking
```

### Into This...

**After:**
```
✅ Single consolidated database
✅ Automated dashboards
✅ Real-time insights in seconds
✅ Validated, high-quality data
✅ Full historical analysis (2020-2024)
```

---

<!-- slide 6 -->
# Slide 6: Solution Architecture (High-Level)

## End-to-End Data Platform

```
[Raw Data] → [ETL Pipeline] → [Data Warehouse] → [Dashboards]
```

### Architecture Layers

1. **Source Layer** - Raw business data files
2. **Ingestion Layer** - Automated data extraction
3. **Staging Layer** - Data landing zone
4. **Transformation Layer** - Business logic & cleansing
5. **Warehouse Layer** - Kimball dimensional model
6. **Presentation Layer** - Dashboards & reports

**Design Pattern:** ELT (Extract-Load-Transform)  
**Storage:** PostgreSQL databases (Staging + Warehouse)  
**Orchestration:** Apache Airflow

---

<!-- slide 7 -->
# Slide 7: Technical Infrastructure

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 3.1.3 | Workflow automation |
| **Data Warehouse** | PostgreSQL 16 | Analytical database |
| **Staging DB** | PostgreSQL 16 | Raw data landing |
| **Message Queue** | Redis 7.2 | Task distribution |
| **Container Platform** | Docker Compose | Infrastructure deployment |
| **ETL Scripts** | Python 3.x | Data processing |
| **BI Tool** | *To Be Decided* | Visualization |

### Why These Technologies?
- **Open Source** - No licensing costs
- **Proven** - Industry-standard tools
- **Scalable** - Can grow with business
- **Portable** - Runs anywhere with Docker

---

<!-- slide 8 -->
# Slide 8: Infrastructure Diagram

## Docker Container Architecture

```
┌─────────────────────────────────────────────────┐
│          Apache Airflow Services                │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐    │
│  │ Web UI   │  │ Scheduler │  │  Worker  │    │
│  │  :8080   │  │           │  │  (Celery)│    │
│  └──────────┘  └───────────┘  └──────────┘    │
└─────────────────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
┌──────────────┐ ┌─────────────┐ ┌──────────────┐
│  PostgreSQL  │ │ PostgreSQL  │ │    Redis     │
│  (Staging)   │ │    (DWH)    │ │   (Queue)    │
│   :5433      │ │   :5432     │ │              │
└──────────────┘ └─────────────┘ └──────────────┘
```

**Network:** All services on `shopzada-net` private network  
**Persistence:** Docker volumes for data durability  
**Access:** Web UI at `http://localhost:8080`

---

<!-- slide 9 -->
# Slide 9: Data Sources

## What Data Do We Have?

### 📊 Data Inventory

| Domain | Records | Time Range | Formats |
|--------|---------|------------|---------|
| **Orders** | 1.5M+ | 2020-2024 | CSV, JSON, Excel, HTML, Parquet |
| **Line Items** | 1.5M+ | 2020-2024 | CSV, Parquet |
| **Customers** | 50K+ | 2021-Present | JSON, CSV, Pickle |
| **Products** | 20K+ | Current | Excel |
| **Merchants** | 10K+ | 2020-Present | HTML, CSV |
| **Staff** | 30K+ | 2020-Present | HTML |
| **Campaigns** | 50K+ | 2023-Present | CSV |

### 💾 Total Data Volume
- **Files:** 20+ raw data files
- **Formats:** 7 different file types
- **Time Span:** 4+ years of historical data

---

<!-- slide 10 -->
# Slide 10: ETL Pipeline - Overview

## Data Flow Architecture

```
Step 1: EXTRACT
  ↓ Read raw files (CSV, JSON, Excel, HTML, Parquet, Pickle)
  ↓ Automated file detection
  
Step 2: LOAD (to Staging)
  ↓ Convert to standardized Parquet format
  ↓ Data quality validation
  ↓ Load to PostgreSQL staging database
  
Step 3: TRANSFORM
  ↓ Apply business logic
  ↓ Build dimensional model (THIS PART IS STILL MISSING)
  
Step 4: LOAD (to Warehouse)
  ↓ Load dimensions & facts (THIS PART IS STILL MISSING)
  
Step 5: PRESENT
  ↓ Create data marts
  ↓ Build dashboards (THIS PART IS STILL MISSING)
```

**Status:** Steps 1-2 complete ✅ | Steps 3-5 in progress 🚧

---

<!-- slide 11 -->
# Slide 11: ETL Pipeline - Airflow DAGs

## Workflow Orchestration

### Main DAG: `shopzada_data_warehouse`

**Task Groups:**

1. ✅ **source_staging** - Ingest raw data to staging DB (WORKING)
   - Ingest all sources → Quality checks → Load to staging
   
2. 🚧 **transform_and_quality_checks** - Transform data (IN PROGRESS)
   - Transform data → Quality checks → Clean files
   
3. 🚧 **load_to_dw** - Load physical model (PLANNED)
   
4. 🚧 **kimball_dw** - Build star schema (PLANNED)
   - Build dimensions → Build facts
   
5. 🚧 **datamarts_and_views** - Create analytics layer (PLANNED)

6. 🚧 **analytics** - Run analytical queries (PLANNED)

7. 🚧 **presentation** - Load BI layer (PLANNED)

---

<!-- slide 12 -->
# Slide 12: Data Pipeline Implementation

## Current ETL Scripts (Working)

### ✅ Data Ingestion Scripts
**Location:** `scripts/ingestion/`

- **load_to_parquet.py** - Multi-format file reader
  - Supports: CSV, JSON, Excel, HTML, Parquet, Pickle
  - Auto-detects file types and routes to appropriate reader
  - Outputs standardized Parquet files

- **data_quality_checks.py** - Validation framework
  - Schema validation
  - Null value detection
  - Pattern matching
  - Data type verification

- **load_to_staging.py** - Database loader
  - Reads Parquet files
  - Loads to PostgreSQL staging database
  - Creates tables automatically

---

<!-- slide 13 -->
# Slide 13: Data Model (Kimball)

## Dimensional Modeling Approach

### Star Schema Design

**THIS PART IS STILL MISSING** - Currently being designed by Data Architect (Joseph)

### Planned Structure

**Dimension Tables** (Descriptive attributes):
- 🚧 `dim_customer` - Customer demographics
- 🚧 `dim_product` - Product catalog
- 🚧 `dim_merchant` - Merchant details
- 🚧 `dim_staff` - Staff information
- 🚧 `dim_campaign` - Marketing campaigns
- 🚧 `dim_date` - Calendar dimension

**Fact Tables** (Metrics & measurements):
- 🚧 `fact_orders` - Order transactions
- 🚧 `fact_campaign_performance` - Campaign metrics

**Status:** Design in progress by Data Architect

---

<!-- slide 14 -->
# Slide 14: Dimensional Model Diagram

## Star Schema Visualization

**THIS PART IS STILL MISSING** - Dimensional model design in progress

### What We'll Build:

```
           ┌──────────────┐
           │ dim_customer │
           └──────┬───────┘
                  │
    ┌─────────────┼─────────────┐
    │             │             │
┌───▼────┐   ┌───▼─────┐   ┌───▼────────┐
│dim_date│   │fact_    │   │dim_product │
│        │───│orders   │───│            │
└────────┘   └─────────┘   └────────────┘
                  │
         ┌────────┴────────┐
         │                 │
    ┌────▼──────┐   ┌─────▼──────┐
    │dim_       │   │dim_merchant│
    │campaign   │   │            │
    └───────────┘   └────────────┘
```

**Coming Soon:** Full ER diagram with all attributes and relationships

---

<!-- slide 15 -->
# Slide 15: Staging Database Schema

## Current Data in Staging Layer

### Available Staging Tables (Loaded ✅)

| Table | Source | Key Columns | Purpose |
|-------|--------|-------------|---------|
| `user_data` | user_data.json | user_id, name, demographics | Customer master |
| `user_credit_card` | .pickle | user_id, card_number, bank | Payment info |
| `user_job` | .csv | user_id, job_title, job_level | Employment |
| `order_data_*` | Multiple | order_id, user_id, date | Order headers |
| `line_item_*` | Multiple | order_id, product_id, price, qty | Order details |
| `product_list` | .xlsx | product_id, name, type, price | Product catalog |
| `merchant_data` | .html | merchant_id, name, address | Merchant master |
| `staff_data` | .html | staff_id, name, job_level | Staff records |
| `campaign_*` | .csv | campaign_id, order_id, discount | Campaign data |

**Status:** All raw data successfully staged in PostgreSQL

---

<!-- slide 16 -->
# Slide 16: Current Progress

## Project Completion Status

### ✅ Completed (40%)

| Component | Status | Details |
|-----------|--------|---------|
| Infrastructure Setup | ✅ 100% | Docker Compose with all services |
| Database Deployment | ✅ 100% | Staging + DWH PostgreSQL databases |
| Data Ingestion | ✅ 100% | Multi-format readers working |
| Airflow Orchestration | ✅ 80% | Basic DAG structure complete |
| Staging Pipeline | ✅ 100% | Raw → Parquet → Staging DB |
| Data Quality Framework | ✅ 60% | Initial validation checks |

### 🚧 In Progress (30%)

- **Dimensional Model Design** (Data Architect)
- **SQL Schema Scripts** (Data Engineer)
- **Transformation Logic** (ETL Engineer)
- **Documentation** (Project Manager)

### ⏳ Not Started (30%)

- Dimension/Fact ETL Scripts
- Data Marts & Views
- BI Dashboards
- Production Deployment

---

<!-- slide 17 -->
# Slide 17: Team Roles & Contributions

## Who's Doing What?

| Team Member | Role | Current Tasks |
|-------------|------|---------------|
| **Paul Aldrich Pimentel** | Project Manager | Documentation, coordination, presentations |
| **Ken Fajardo** | Workflow Orchestration | Airflow DAG development & monitoring |
| **Joseph Daniel Mamangun** | Data Architect | Dimensional model design ⭐ |
| **Jodimeer Ammang** | Infrastructure Engineer | Docker, database management |
| **Aaron Cuevas** | ETL Engineer | Building dimension & fact ETL scripts |
| **Justin Lloyd Floro** | Data Engineer | Data quality, marts, optimization |
| **Nigel Anunciacion** | BI Developer | Dashboard design & development |

⭐ = Critical path - other work depends on this

---

<!-- slide 18 -->
# Slide 18: Business Value & Use Cases

## What Can We Do With This?

### 📊 Business Intelligence Use Cases

1. **Sales Analytics**
   - Top-selling products by revenue
   - Sales trends over time (daily, weekly, monthly)
   - Regional performance analysis

2. **Customer Analytics**
   - Customer demographics breakdown
   - Customer lifetime value
   - Purchase behavior patterns
   - Customer segmentation

3. **Campaign Effectiveness**
   - Campaign ROI analysis
   - Discount impact on sales
   - Campaign performance comparison

4. **Operational Metrics**
   - Order fulfillment performance
   - Delivery delay analysis
   - Merchant/staff performance

---

<!-- slide 19 -->
# Slide 19: Sample Analytics (Mock-ups)

## Dashboard Previews

**THIS PART IS STILL MISSING** - Dashboards to be built after warehouse completion

### Planned Dashboards

**1. Executive Dashboard**
- KPIs: Total Revenue, Orders, Customers, Products
- Trend charts: Sales over time
- Top performers: Products, Merchants, Campaigns

**2. Sales Performance**
- Revenue by product category
- Geographic distribution
- Time-based analysis (hourly, daily, weekly)

**3. Customer Insights**
- Customer demographics
- Purchase frequency distribution
- Customer retention metrics

**4. Marketing ROI**
- Campaign performance comparison
- Discount effectiveness
- Customer acquisition cost

---

<!-- slide 20 -->
# Slide 20: Sample Queries

## Analytical SQL Examples

### Query 1: Top 10 Products by Revenue
```sql
SELECT 
    p.product_name,
    SUM(f.quantity * f.unit_price) as total_revenue,
    SUM(f.quantity) as units_sold
FROM fact_orders f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2024
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 10;
```

### Query 2: Monthly Sales Trend
```sql
SELECT 
    d.year,
    d.month_name,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.quantity * f.unit_price) as monthly_revenue
FROM fact_orders f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month_number, d.month_name
ORDER BY d.year, d.month_number;
```

**Note:** These queries will work once dimensional model is implemented

---

<!-- slide 21 -->
# Slide 21: Project Timeline

## 9-Week Implementation Plan

```
Week 1-2: PLANNING ✅ (Complete)
  ├─ Project setup
  ├─ Infrastructure deployment
  └─ Team coordination

Week 2-3: DESIGN 🚧 (In Progress)
  ├─ Dimensional model design
  ├─ SQL schema creation
  └─ Data mapping

Week 4-5: ETL DEVELOPMENT ⏳ (Upcoming)
  ├─ Dimension ETL scripts
  ├─ Fact ETL scripts
  └─ Data quality checks

Week 6-7: TESTING & ANALYTICS ⏳ (Upcoming)
  ├─ End-to-end pipeline testing
  ├─ SQL query development
  └─ Dashboard design

Week 8-9: DELIVERY ⏳ (Upcoming)
  ├─ Dashboard development
  ├─ Documentation finalization
  └─ Final presentation
```

**Current Status:** Week 2 (Design Phase)

---

<!-- slide 22 -->
# Slide 22: Key Milestones

## Project Checkpoints

| Milestone | Target Date | Status |
|-----------|-------------|--------|
| Infrastructure Setup | Week 1 | ✅ Complete |
| Data Ingestion Pipeline | Week 2 | ✅ Complete |
| Dimensional Model Design | Week 2 | 🚧 In Progress |
| Database Schema Deployed | Week 3 | ⏳ Planned |
| First Dimension Table Loaded | Week 4 | ⏳ Planned |
| First Fact Table Loaded | Week 5 | ⏳ Planned |
| Full Pipeline Working | Week 6 | ⏳ Planned |
| Dashboards Complete | Week 8 | ⏳ Planned |
| Final Presentation | Week 9 | ⏳ Planned |

🎯 **Next Critical Milestone:** Dimensional Model Design (This Week!)

---

<!-- slide 23 -->
# Slide 23: Technical Highlights

## Notable Achievements

### 🏆 What We Did Well

1. **Multi-Format Data Ingestion**
   - Built flexible readers for 7 different file formats
   - Automated file detection and routing
   - 100% of raw data successfully staged

2. **Robust Infrastructure**
   - Fully containerized with Docker
   - Production-ready orchestration with Airflow
   - Scalable architecture design

3. **Data Quality Focus**
   - Built-in validation framework
   - Quality checks at every stage
   - Comprehensive error handling

4. **Kimball Methodology**
   - Following industry best practices
   - Star schema for optimal query performance
   - Business-focused dimensional design

---

<!-- slide 24 -->
# Slide 24: Challenges & Solutions

## Problems We Solved

### Challenge 1: Multiple File Formats
**Problem:** Data scattered across CSV, JSON, Excel, HTML, Parquet, Pickle  
**Solution:** Built modular reader system with Python that auto-detects and processes any format

### Challenge 2: Data Volume
**Problem:** 1.5M+ records across 20+ files  
**Solution:** Efficient Parquet intermediate format + PostgreSQL for scalability

### Challenge 3: Data Quality
**Problem:** Inconsistent schemas and missing values  
**Solution:** Comprehensive validation framework with automated quality reports

### Challenge 4: Orchestration Complexity
**Problem:** Many interdependent ETL steps  
**Solution:** Apache Airflow DAGs with clear task dependencies and retry logic

---

<!-- slide 25 -->
# Slide 25: Risks & Mitigation

## Project Risk Management

| Risk | Impact | Mitigation | Status |
|------|--------|------------|--------|
| **Dimensional model delays** | High | Daily standups, clear dependencies | 🟡 Monitoring |
| **Data quality issues** | Medium | Automated validation, early testing | 🟢 Controlled |
| **Technical skills gap** | Medium | Documentation, peer reviews | 🟢 Controlled |
| **Scope creep** | Medium | MVP approach, prioritization | 🟢 Controlled |
| **Infrastructure failures** | Low | Docker health checks, backups | 🟢 Controlled |

### Risk Response Plan
- **High impact risks:** Daily monitoring by Project Manager
- **Blockers:** Escalate immediately to Data Architect
- **Schedule delays:** Re-prioritize features (MVP first)

---

<!-- slide 26 -->
# Slide 26: Next Steps (Immediate)

## What's Happening Now?

### This Week (Week 2)
✅ **Data Architect** (Joseph)
- Complete dimensional model design
- Create ER diagram and bus matrix
- Document in architecture.md

⏳ **ETL Engineer** (Aaron)
- Review dimensional model
- Begin SQL DDL script writing
- Plan transformation logic

⏳ **Infrastructure Engineer** (Jodimeer)
- Prepare for schema deployment
- Set up database monitoring

⏳ **Project Manager** (Paul)
- Complete technical documentation ✅
- Complete presentation slides ✅
- Track team progress

---

<!-- slide 27 -->
# Slide 27: Next Steps (Short-term)

## Coming in Weeks 3-5

### Week 3: Database Schema
- Deploy dimension and fact table schemas
- Set up indexes and constraints
- Test database connectivity

### Week 4: Dimension ETL
- Implement ETL for all dimension tables
- Load historical dimension data
- Validate dimension data quality

### Week 5: Fact ETL
- Implement fact table ETL
- Load fact data with dimension foreign keys
- Run end-to-end pipeline test

### Week 6: Testing
- Full integration testing
- Performance optimization
- Bug fixes and refinements

---

<!-- slide 28 -->
# Slide 28: Long-term Vision

## Future Enhancements (Post-MVP)

### Phase 2 Features (Optional)

1. **Real-time Data Streaming**
   - Move from batch to real-time ingestion
   - Use Apache Kafka or similar

2. **Advanced Analytics**
   - Machine learning models (customer churn prediction)
   - Forecasting and predictive analytics

3. **Cloud Migration**
   - Deploy to AWS/GCP/Azure
   - Leverage managed services (Redshift, BigQuery)

4. **Self-Service BI**
   - Enable business users to build own reports
   - Data catalog and documentation

5. **Data Governance**
   - Data lineage tracking
   - Audit logging and compliance

---

<!-- slide 29 -->
# Slide 29: Success Metrics

## How We Measure Success

### Technical Metrics
✅ **Pipeline Reliability:** 99% DAG success rate  
✅ **Data Quality:** <1% error rate in validations  
✅ **Performance:** Queries return in <5 seconds  
✅ **Coverage:** 100% of source data integrated

### Business Metrics
✅ **Time-to-Insight:** From hours to seconds  
✅ **Dashboard Adoption:** Active users in first month  
✅ **Report Automation:** 80% of manual reports eliminated  
✅ **Stakeholder Satisfaction:** Positive feedback from business users

### Delivery Metrics
✅ **On-time Delivery:** Completed within 9 weeks  
✅ **Budget:** Within allocated resources  
✅ **Documentation:** Complete and comprehensive  
✅ **Knowledge Transfer:** Team can maintain independently

---

<!-- slide 30 -->
# Slide 30: Lessons Learned

## What We Learned

### ✅ What Worked Well
- **Docker containerization** - Made deployment seamless
- **Airflow for orchestration** - Clear visibility into pipeline
- **Team role clarity** - Everyone knows their responsibilities
- **Documentation-first** - Architecture docs guide implementation

### 🔧 What We'd Improve
- **Earlier dimensional model design** - Should start Week 1
- **More frequent integration testing** - Catch issues earlier
- **Better time estimates** - Some tasks took longer than planned

### 💡 Key Takeaways
- **Design before code** - Spend time on architecture
- **Start with MVP** - Build core features first, optimize later
- **Communication is critical** - Daily standups keep team aligned
- **Quality over speed** - Better to build it right than fast

---

<!-- slide 31 -->
# Slide 31: Conclusion

## Project Summary

### What We Built
✅ Enterprise data warehouse infrastructure  
✅ Automated ETL pipeline with Airflow  
✅ Multi-format data ingestion system  
✅ Staging database with 1.5M+ records  
✅ Foundation for dimensional analytics

### What We're Building
🚧 Kimball star schema (dimensions + facts)  
🚧 Transformation & quality framework  
🚧 Business intelligence dashboards  
🚧 Analytical SQL queries

### Impact
🎯 **Single source of truth** for ShopZada data  
🎯 **Historical analysis** from 2020-2024  
🎯 **Real-time insights** via dashboards  
🎯 **Scalable platform** for future growth

**Thank you for your attention!**

---

<!-- slide 32 -->
# Slide 32: Q&A

## Questions & Answers

### Common Questions

**Q: When will dashboards be ready?**  
A: Week 8 (after dimensional model and ETL are complete)

**Q: Can we add more data sources later?**  
A: Yes! Our ingestion pipeline is extensible

**Q: How do we access the data warehouse?**  
A: Via dashboards (business users) or SQL queries (analysts)

**Q: What if we need changes to the model?**  
A: Version 1 is MVP - we can iterate in future versions

**Q: How is data quality ensured?**  
A: Automated validation at every pipeline stage

---

### 🙋 Open Floor for Questions

**Contact Information:**  
Paul Aldrich Pimentel - Project Manager  
Email: [Your Email]  
GitHub: shopzada-data-warehouse

---

<!-- slide 33 -->
# Slide 33: Appendix - Technical Details

## For Technical Stakeholders

### Infrastructure Specifications
- **Containers:** 8 services on Docker Compose
- **Databases:** PostgreSQL 16-alpine
- **Orchestration:** Airflow 3.1.3 (CeleryExecutor)
- **Resource Requirements:** 4GB RAM, 10GB disk

### Database Details
- **Staging DB:** Port 5433, ~2GB data
- **DWH DB:** Port 5432, TBD size
- **Backup Strategy:** Docker volumes + pg_dump

### ETL Performance (Current)
- **Ingestion:** ~5 minutes for all files
- **Quality Checks:** ~2 minutes
- **Staging Load:** ~3 minutes
- **Total Runtime:** ~10 minutes end-to-end

### Code Repository
- **GitHub:** shopzada-data-warehouse
- **Languages:** Python, SQL
- **Lines of Code:** ~2,000+ (and growing)

---

<!-- slide 34 -->
# Slide 34: Appendix - Resources

## Additional Information

### Documentation
- 📄 `README.md` - Project overview
- 📄 `docs/architecture.md` - Technical architecture (THIS FILE)
- 📄 `docs/DWH Stack Operations Guide.md` - Operations manual
- 📄 `docs/raw_data_summary.txt` - Data inventory

### Code Locations
- 📁 `scripts/ingestion/` - ETL scripts
- 📁 `workflows/dags/` - Airflow DAGs
- 📁 `infra/` - Docker configurations
- 📁 `sql/` - SQL scripts (TO BE ADDED)

### Access Points
- 🌐 Airflow UI: http://localhost:8080
- 🗄️ DWH Database: localhost:5432
- 🗄️ Staging DB: localhost:5433

### Learning Resources
- Kimball Group: kimballgroup.com
- Airflow Docs: airflow.apache.org
- PostgreSQL Tutorial: postgresqltutorial.com

---

# END OF PRESENTATION

**Thank You!**

---

## Presentation Notes

**Total Slides:** 34  
**Estimated Duration:** 30-45 minutes  
**Recommended Format:** PowerPoint, Google Slides, or Reveal.js

### Conversion Instructions

To convert this markdown to slides:

**Option 1: Pandoc (PowerPoint)**
```bash
pandoc presentation_slides.md -o shopzada_presentation.pptx
```

**Option 2: Marp (PDF/HTML)**
```bash
marp presentation_slides.md -o shopzada_presentation.pdf
```

**Option 3: Google Slides**
1. Copy content slide by slide
2. Use slide separators (`---`) as breaks
3. Add your preferred theme and styling

**Option 4: Reveal.js (Web)**
Use reveal-md to create interactive web slides:
```bash
reveal-md presentation_slides.md
```

### Customization Tips
- Add company/school logo to title slide
- Use consistent color scheme matching ShopZada branding
- Add screenshots from Airflow UI for visual interest
- Include actual data samples where appropriate
- Update "(THIS PART IS STILL MISSING)" as features complete
