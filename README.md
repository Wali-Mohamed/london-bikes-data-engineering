# tfl-batch-data-pipeline
An end-to-end batch data pipeline orchestrating 30M+ rows of historical TfL Santander Cycles data using Apache Airflow, PySpark, dbt, Terraform, and GCP to optimize fleet rebalancing.

# 🚲 TfL Santander Cycles Data Engineering Pipeline

## Project Description

Transport for London (TfL) generates **millions of rows of Santander Cycles trip data each year**.  
However, the data is published as **fragmented, historically versioned CSV files** on a public web server.

This creates several engineering challenges:

- Data is **spread across multiple CSV files**
- Total volume reaches **tens of millions of rows**
- The **schema evolves over time** (e.g., the introduction of E-bikes in 2022)
- Raw files are **not directly queryable at scale**

To solve this, I built a **fully automated batch data pipeline** that ingests, processes, and models **three years of post-pandemic TfL data (2023–2025)** using modern data engineering tools.

---

# 🏗 Pipeline Architecture

The pipeline automates the full lifecycle from raw ingestion to analytics-ready datasets.

### Infrastructure as Code
- Provisioned the **Google Cloud Storage (GCS) Data Lake** and **BigQuery Data Warehouse**
- Infrastructure defined using **Terraform** for reproducibility

### Workflow Orchestration
- Built **Apache Airflow DAGs** to automate:
  - Extraction of CSV files from the public TfL web server
  - Loading raw data into the **GCS Data Lake**

### Distributed Processing
- Used **PySpark** to:
  - Clean and standardize trip records
  - Handle schema changes (including E-bike introduction)
  - Convert raw CSVs into **compressed Parquet files**
  - Partition datasets for efficient downstream querying

### Data Warehouse & Modeling
- Loaded curated Parquet datasets into **BigQuery**
- Built a **Kimball-style Star Schema** using **dbt**

Tables include:

- **fact_trips** — trip-level transactional data  
- **dim_stations** — docking station metadata

This modeling approach enables **fast analytical queries** and scalable reporting.

---

# 📊 Business Impact

The pipeline enables actionable analytics through a **Looker Studio dashboard**.

Key insights include:

### 🚴 Bike Rebalancing Analysis
- Calculates **hourly Net Flow of bikes**
- Identifies **stations that drain empty during morning rush hour**
- Helps TfL optimize **bike redistribution logistics**

### ⚡ E-Bike Adoption Tracking
- Monitors **usage and growth of the E-bike fleet**
- Tracks **return on investment (ROI)** for the new system
- Supports strategic decisions about **future fleet expansion**

---

# 🛠 Tech Stack

| Category | Tools |
|--------|------|
| Infrastructure | Terraform |
| Orchestration | Apache Airflow |
| Processing | PySpark |
| Data Lake | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Transformations | dbt |
| Visualization | Looker Studio |

---

# 📈 Outcome

This project demonstrates how modern data engineering tools can transform **raw, fragmented public data into a scalable analytics platform**, enabling **data-driven decision making for urban mobility systems**.
