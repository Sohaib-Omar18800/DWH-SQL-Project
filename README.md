# 📊 End-to-End Data Warehouse Project (SQL Server)

![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![T-SQL](https://img.shields.io/badge/T--SQL-0078D4?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)

## 🚀 Project Overview
This project demonstrates the design and implementation of a modern Data Warehouse using the **Medallion Architecture**. I transformed raw CRM and ERP data into a refined **Star Schema** optimized for business intelligence.
| WORKFLOW |
| :---: |
| ![WORKFLOW](Docs/Data%20Architecture.png) |

The pipeline handles:
- **Data Ingestion** (Bronze)
- **Data Cleansing & Standardization** (Silver)
- **Business Logic & Dimensional Modeling** (Gold)

---

## 📂 Project Structure
Below is the organization of the repository and the purpose of each directory:

```text
DWH-SQL-Project/
├── Datasets/               # Source CSV files (CRM & ERP data)
├── Screenshots/            # Visual documentation of the pipeline
├── scripts/
│   ├── bronze/             # Schema creation & Bulk Loading
│   │   └── proc_load_bronze.sql
│   ├── silver/             # Data cleaning & Standardization
│   │   └── proc_load_silver.sql
│   └── gold/               # Final Fact & Dimension Views
│       └── ddl_gold.sql
└── init_database.sql       # Initial Database & Schema setup
```

## 🏗️ The Medallion Pipeline
### 🥉 Bronze Layer (Raw)
Goal: Ingest raw data as-is.

Process: Created schemas and tables for CRM and ERP data. Used BULK INSERT for high-speed data loading.

Logic: Includes a stored procedure proc_load_bronze to automate the truncation and loading of raw files.

### 🥈 Silver Layer (Cleansed)
Goal: Cleanse and standardize data.

Key Transformations:

Data Healing: Replaced 'Unidentified' gender values by cross-referencing datasets.

Validation: Handled nulls, trimmed extra spaces, and standardized date formats.

Quality Control: Removed duplicates and invalid business keys.
### 🥇 Gold Layer (Reporting)
Goal: Business-ready analytics.

Modeling: Implemented a Star Schema with a central Fact table and supporting Dimensions.

Optimization: Used Surrogate Keys (_sk) for efficient joins and performance.

✨ Developed by **Sohaib Omar**
Feel free to reach out for collaborations or questions!

**_LinkedIn_**:
🔗
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/sohaib-omar-188oo)
