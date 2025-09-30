# Data Warehouse with Medallion Architecture and Analysis
This project demonstrates the implementation of a Data Warehouse in SQL Server using the Medallion Architecture (Bronze, Silver, Gold layers).
The goal of this architecture is to efficiently ingest, clean, and transform data from raw sources into business-ready datasets for Analytics & Business Intelligence (BI).

# ⚙️ Architecture Layers
🔹 Bronze Layer – Raw Data
    Source: CSV files
    Object Type: Tables
    Load Type: Batch Processing, Full Load, Truncate and Insert
    Transformations: None (raw data stored as-is)
Purpose: Preserve raw ingested data in its original form for traceability and auditing.

🔸 Silver Layer – Standardized Data
    Object Type: Tables
    Load Type: Batch Processing, Full Load, Truncate and Insert
    Transformations Applied:
        Data Cleaning (handling nulls, removing duplicates, correcting formats)
        Standardization (consistent naming conventions, units, formats)
        Normalization (structured into relational tables)
        Derived Columns (new fields generated for analytics needs)
Purpose: Provide clean, reliable, and standardized data ready for downstream transformations.

🟡 Gold Layer – Business Ready Data
    Object Type: Views
    Load Type: No additional loads (built on Silver layer)
    Transformations Applied: Business Logic for reporting & analytics
    Data Models Used:
        Star Schemas
        Flat / Aggregated Tables
Purpose: Deliver business-ready views optimized for ad-hoc queries and BI tools like Tableau and Power BI.

## Consumers
The Gold Layer serves as the consumption-ready data layer, enabling:
    Ad-Hoc SQL Queries for exploration
    Analytics & BI Dashboards (e.g., Tableau, Power BI)

## Tech Stack 
    Database: Microsoft SQL Server
    Data Sources: CSV Files
    ETL Approach: Batch Load, Truncate and Insert
    BI Integration: PowerBI / Tableau

## Key Highlights
    Implements Medallion architecture.
    Ensures Data quality and governance through structured transformations.
    Supports both data tracebility(Bronze Layer) and analytics readiness(Gold Layer).

## Folder Structure
📂 data-warehouse-sales
│── 📜 README.md                # Main project documentation
│
├── 📂 datasets
│   ├── 📂 source_crm
│   │     ├── 📜 cust_info.csv
│   │     ├── 📜 prd_info.csv
│   │     ├── 📜 sales_details.csv
│   │
│   ├── 📂 source_erp
│   │     ├── 📜 cust_az12.csv
│   │     ├── 📜 loc_a101.csv
│   │     ├── 📜 px_cat_g1v2.csv
│
├── 📂 docs
│   ├── 📜 Warehouse_Architecture.png       # Detailed explanation of architecture
│   ├── 📜 data_flow_diagram.png            # Flow of data within layers
│   ├── 📜 data_integration.png             # Data Integration Guide
│   └── 📜 sales_data_mart.png              # Star Schema for Gold Layer
│
├── 📂 scripts
|   ├── 📜 init_database.sql                        # Initial sql file to create database and schemas
│   ├── 📂 bronze
│   │   ├── 📜 bronze_schemas.sql                   # SQL Script to create tables for bronze layer
|   |   └── 📜 proc_bulk_insert_bronze_layer.sql    # Stored Procedure to perform bulk insert from csv files
│   │ 
│   ├── 📂 silver
│   │   ├── 📜 silver_schemas.sql                   # SQL Script to create tables for silver layer
|   |   └── 📜 proc_insert_silver_layer.sql         # Stored Procedure to perform data cleaning and inserting data in silver layer 
|   |
|   ├── 📂 gold
│   │   ├── 📜 gold_views.sql                       # SQL Script to create views based on the designed star schema.
|   |   ├── 📜 customer_report.sql                  # Script to create a customer data mart ready for analysis and BI.
|   |   ├── 📜 product_report.sql                   # Script to create a product data mart ready for analysis and BI.
└── └── 📜 SalesAnalysis.sql                        # Ad-hoc SQL queries used in creating data marts.
