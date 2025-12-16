# ShopZada Data Warehouse
## Final Presentation (15 Slides)

**Presented by:** Team ShopZada DWH  
**Date:** December 2025  
**Methodology:** Kimball Dimensional Modeling

---

<!-- slide 1 -->
# Slide 1: Title Slide

## ShopZada Data Warehouse Project

**Building an Enterprise-Grade Analytical Platform**

### Team Members
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
# Slide 2: The Business Problem

## ShopZada's Data Challenges

### 📊 Fragmented Data
- **20+ files** in 7 different formats (CSV, JSON Excel, HTML, Parquet, Pickle)
- Data scattered across 5 departments (Customer, Operations, Marketing, Enterprise, Business)
- **1.5M+ orders** with no unified view

### ⚠️ Critical Issues
```
❌ Manual Excel analysis (hours of work)
❌ No historical trend analysis
❌ Inconsistent data quality
❌ Cannot answer cross-functional questions
❌ Delayed business decisions
```

### 🎯 Business Questions We Need to Answer
- What are our top products by revenue?
- Which customers contribute most?
- What campaigns drive the highest ROI?
- How do merchants perform?

---

<!-- slide 3 -->
# Slide 3: Our Solution

## Unified Data Warehouse

### Transform This → Into This

**Before:**
- 20+ disparate files
- Hours to answer questions
- Manual data compilation
- No single source of truth

**After:**
- Single consolidated database
- Seconds to generate insights
- Automated ETL pipeline
- 4+ years of historical data (2020-2024)

### Methodology: **Kimball Dimensional Modeling**
- Star schema design for optimal analytics
- Business-focused approach
- Proven industry standard

---

<!-- slide 4 -->
# Slide 4: System Architecture

## End-to-End Data Platform

```
[Raw Data] → [ETL Pipeline] → [Staging DB] → 
[Transformation] → [Data Warehouse] → [Dashboards]
```

### Layered Architecture

1. **Source Layer** - 20+ raw data files
2. **Ingestion Layer** - Python multi-format readers  ✅ Complete
3. **Staging Layer** - PostgreSQL landing zone  ✅ Complete
4. **Transformation** - Business logic & cleansing  🚧 In Progress
5. **Warehouse** - Kimball star schema  🚧 Design Phase
6. **Presentation** - BI dashboards  🚧 Planned

**Key Technologies:**
- Apache Airflow (orchestration)
- PostgreSQL (databases)
- Docker (containerization)
- Python (ETL scripts)

---

<!-- slide 5 -->
# Slide 5: Technical Infrastructure

## Fully Containerized with Docker

### Services Architecture
```
┌─────────────────────────────────┐
│  Airflow (Web UI, Scheduler,   │
│  Worker, DAG Processor)         │
└──────────────┬──────────────────┘
               │
    ┌──────────┼──────────┐
    ▼          ▼          ▼
┌────────┐ ┌───────┐ ┌────────┐
│Staging │ │  DWH  │ │ Redis  │
│  DB    │ │  DB   │ │ Queue  │
└────────┘ └───────┘ └────────┘
```

**One-Command Deployment:**
```bash
docker compose up -d
```

**Benefits:**
✅ Reproducible environment  
✅ Easy deployment  
✅ Scalable infrastructure  
✅ Isolated services

---

<!-- slide 6 -->
# Slide 6: Data Inventory

## What Data Do We Have?

| Domain | Records | Time Range | Formats |
|--------|---------|------------|---------|
| **Orders** | 1.5M+ | 2020-2024 | 5 formats |
| **Customers** | 50K+ | 2021-Present | 3 formats |
| **Products** | 20K+ | Current | Excel |
| **Merchants** | 10K+ | 2020-Present | 2 formats |
| **Staff** | 30K+ | 2020-Present | HTML |
| **Campaigns** | 50K+ | 2023-Present | CSV |

### Data Quality: All Successfully Staged ✅
- 100% of source files ingested
- Data validation checks passed
- Ready for transformation

---

<!-- slide 7 -->
# Slide 7: ETL Pipeline

## Automated Data Pipeline with Airflow

### Current Implementation (Working ✅)

**Stage 1: Ingest All Sources**
- Multi-format readers (CSV, JSON, Excel, HTML, Parquet, Pickle)
- Convert to standardized Parquet format

**Stage 2: Data Quality Checks**
- Schema validation
- Null value detection
- Pattern matching

**Stage 3: Load to Staging DB**
- Bulk load to PostgreSQL
- 1.5M+ rows loaded successfully

### Future Stages (In Progress 🚧)
- Build dimensional model (dimensions + facts)
- Create data marts
- Generate dashboards

**Total Pipeline Time:** ~10 minutes end-to-end

---

<!-- slide 8 -->
# Slide 8: Data Model (Kimball Star Schema)

## Dimensional Design

**🚧 Currently in Design by Data Architect**

### Planned Structure

```
     dim_customer     dim_product
           \              /
            \            /
             fact_orders
            /     |     \
           /      |      \
    dim_date  dim_merchant  dim_campaign
```

**Dimension Tables** (Who/What/Where/When):
- `dim_customer` - Customer demographics
- `dim_product` - Product catalog
- `dim_merchant` - Merchant info
- `dim_date` - Time dimension
- `dim_campaign` - Marketing campaigns

**Fact Tables** (Metrics):
- `fact_orders` - Transaction metrics (quantity, price, revenue)
- `fact_campaign_performance` - Campaign effectiveness

---

<!-- slide 9 -->
# Slide 9: Business Intelligence

## Planned Dashboards & Analytics

### Dashboard 1: Executive Overview
- **KPIs:** Revenue, Orders, Customers
- **Charts:** Sales trends, top products, geographic distribution

### Dashboard 2: Sales Performance
- Top products by revenue
- Product category analysis
- Year-over-year comparisons

### Dashboard 3: Customer Analytics
- Customer demographics
- Purchase patterns
- Customer lifetime value

### Dashboard 4: Marketing ROI
- Campaign performance comparison
- Discount effectiveness
- Conversion metrics

**Status:** Design complete, awaiting DWH implementation

---

<!-- slide 10 -->
# Slide 10: Project Progress

## Current Status: 60% Complete

### ✅ Completed

| Component | Status | Details |
|-----------|--------|---------|
| Infrastructure | ✅ 100% | Docker environment operational |
| Data Ingestion | ✅ 100% | All formats working |
| Staging Pipeline | ✅ 100% | 1.5M+ rows loaded |
| Airflow Orchestration | ✅ 80% | Core DAG structure |
| Documentation | ✅ 90% | Technical docs nearly complete |

### 🚧 In Progress (30%)
- Dimensional model design (Data Architect)
- SQL schema creation
- Transformation scripts

### ⏳ Planned (10%)
- Dimension/Fact loading
- BI dashboard development

**Timeline:** On track for December 14 deadline

---

<!-- slide 11 -->
# Slide 11: Sample Analytics

## What Business Questions Can We Answer?

### Example Query 1: Top Products
```sql
SELECT 
    product_name,
    SUM(quantity * unit_price) as revenue,
    SUM(quantity) as units_sold
FROM fact_orders f
JOIN dim_product p ON f.product_key = p.product_key
WHERE year = 2024
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 10;
```

### Example Query 2: Campaign ROI
```sql
SELECT 
    campaign_name,
    COUNT(DISTINCT order_id) as orders,
    SUM(discount_amount) as cost,
    SUM(revenue) as revenue_generated
FROM fact_campaign_performance
GROUP BY campaign_name;
```

**Result:** Fast, ad-hoc analytics on clean dimensional data

---

<!-- slide 12 -->
# Slide 12: Key Achievements

## What We Built Successfully

### 🏆 Technical Achievements
✅ **Multi-Format Ingestion** - 7 file types, one unified pipeline  
✅ **Automated Orchestration** - Airflow DAG with retry logic  
✅ **Data Quality Framework** - Validation at every stage  
✅ **Scalable Infrastructure** - Docker-based, production-ready  
✅ **Clean Architecture** - Layered design (staging → DWH → presentation)

### 📊 Data Achievements
✅ **1.5M+ transactions** staged and validated  
✅ **50K+ customers** with demographics  
✅ **4+ years** of historical data ready for analysis  
✅ **100% data coverage** - all source files integrated

### 👥 Team Achievements
✅ **Clear roles** - 7 members, zero conflicts  
✅ **Agile methodology** - Weekly sprints and retrospectives  
✅ **Version control** - Organized GitHub repository

---

<!-- slide 13 -->
# Slide 13: Challenges & Solutions

## Problems We Solved

### Challenge 1: Heterogeneous Data
**Problem:** 7 different file formats  
**Solution:** Modular reader system with auto-detection

### Challenge 2: Data Volume
**Problem:** 1.5M+ rows slow to process  
**Solution:** Parquet compression + PostgreSQL COPY for bulk load

### Challenge 3: Orchestration Complexity  
**Problem:** Many interdependent ETL steps  
**Solution:** Airflow DAG with clear task dependencies

### Challenge 4: Team Coordination
**Problem:** 7 members, risk of overlap  
**Solution:** RACI matrix, daily standups, GitHub workflows

**Result:** On-time delivery with high quality

---

<!-- slide 14 -->
# Slide 14: Next Steps & Future Work

## Immediate Next Steps (This Week)

1. **Complete dimensional model design** (Data Architect)
2. **Write SQL DDL scripts** (ETL Engineer)
3. **Deploy database schema** (Infrastructure)
4. **Begin transformation scripts** (ETL Engineer)
5. **Finalize dashboards** (BI Developer)

## Future Enhancements (Post-MVP)

🚀 **Real-time streaming** with Apache Kafka  
🚀 **Machine learning models** for predictions  
🚀 **Cloud deployment** (AWS/GCP/Azure)  
🚀 **Self-service BI** for business users  
🚀 **Advanced analytics** (churn prediction, forecasting)

**Vision:** ShopZada's data platform as foundation for AI-driven insights

---

<!-- slide 15 -->
# Slide 15: Conclusion & Q&A

## Project Summary

### What We Delivered
✅ Enterprise data warehouse infrastructure  
✅ Automated ETL pipeline (Airflow orchestration)  
✅ 1.5M+ transactions staged and validated  
✅ Kimball dimensional model design  
✅ Scalable, containerized platform  
✅ Comprehensive documentation

### Business Impact
🎯 **Single source of truth** for data-driven decisions  
🎯 **Historical analysis** capability (2020-2024)  
🎯 **Fast insights** via automated dashboards  
🎯 **Scalable platform** ready for growth

### Key Takeaway
> *We transformed ShopZada's fragmented data landscape into a unified analytical platform, enabling faster, data-driven business decisions.*

---

## 🙋 Questions & Answers

**Thank you for your attention!**

---

## Presentation Notes

**Total Slides:** 15 (optimized from 34)  
**Estimated Duration:** 15-20 minutes  
**Format:** PowerPoint / Google Slides / PDF

### Conversion Instructions

**Option 1: Manual** (Recommended for best formatting)
1. Open PowerPoint or Google Slides
2. Create new presentation with professional template
3. Copy content slide-by-slide from this document
4. Add charts, diagrams, and visuals
5. Apply consistent branding and color scheme
6. Export to PDF: `dwh_presentation_<section>_group_<groupname>.pdf`

**Option 2: Automated** (Faster but requires manual cleanup)
```bash
# Using Pandoc
pandoc presentation_final.md -o shopzada_presentation.pptx

# Using Marp
marp presentation_final.md -o shopzada_presentation.pdf
```

### Presentation Tips
- **Slide 1:** Add school logo and group photo
- **Slides 4-5:** Use architecture diagrams (mermaid charts)
- **Slide 6:** Add data visualization chart
- **Slide 8:** Include actual star schema diagram once available
- **Slide 10:** Use progress bars or gauges for visual impact
- **Slide 15:** Include team photo for closing

### Demo Preparation
During live demo, prepare to show:
1. **Airflow UI** - Running DAG
2. **Database** - Sample queries in psql
3. **Docker** - Service health status
4. **Dashboard** - (If completed) Live BI dashboards

### Q&A Preparation
Anticipate questions on:
- Why Kimball over Inmon?
- How do you handle data quality issues?
- What happens if source schema changes?
- How would you scale this to 10x data volume?
- What's your disaster recovery plan?
