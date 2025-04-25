
# 🏦 Customer Transactions & Loan Data Pipeline (Databricks + ADLS + Power BI)

This project implements an **end-to-end ETL pipeline** using **Azure Databricks**, **Azure Data Lake Storage (ADLS)**, and **Power BI**. The goal is to process **customer transactions and loan data** through a **Lakehouse architecture** (Bronze, Silver, Gold layers), ensuring data quality, historical tracking (SCD1), and insightful reporting.

## 📂 Folder Structure

```
project2/
├── bronze/               # Raw data ingested from on-prem (partitioned by yyyy/MM/dd)
├── silver/               # Cleaned and deduplicated data in Parquet format
├── gold/                 # Business-ready Delta tables (SCD1 applied)
├── backup/               # Backup of processed files
└── reject/               # Dirty records (nulls/invalid primary keys)
```

## 🏗️ Architecture Overview

1. **Data Ingestion:**
   - Raw CSV files (`Accounts`, `Customers`, `Loans`, `Loan_Payments`, `Transactions`) are ingested into the **bronze layer** in **ADLS**, organized by date (`/bronze/yyyy/MM/dd/`).
   - Secure connection to **ADLS** via **Service Principal Authentication** and **Azure Key Vault**.

2. **Data Validation & Cleansing (Bronze ➡️ Silver):**
   - Files are checked for availability based on the **current UTC date**.
   - Data cleansing:
     - Drop rows with **null primary IDs** (e.g., customer_id, account_id).
     - Fill nulls in non-key columns with defaults.
   - Output is saved in **Parquet format** in the **silver layer**.

3. **Data Transformation & Merge (Silver ➡️ Gold):**
   - **SCD Type 1 (Slowly Changing Dimension)** logic is applied to maintain the latest data.
   - Data is merged into **Delta Lake tables** in the **gold layer**.

4. **Data Consolidation:**
   - Silver layer parquet files are **joined across entities**:
     - **Primary keys** (e.g., customer_id, account_id)
     - **Transaction columns** (e.g., transaction_amount, payment_amount)
     - **Date columns** (e.g., transaction_date, payment_date)
   - The joined dataset creates a **unified view** of customers, accounts, loans, transactions, and payments.

5. **Power BI Integration:**
   - The consolidated **Parquet dataset** is connected directly to **Power BI**.
   - Power BI dashboards visualize **customer transactions, loan status, payments**, and more.

## ⚙️ Key Technologies

- **Azure Databricks** (ETL, Spark, Delta Lake)
- **Azure Data Lake Storage (ADLS)** (Data storage)
- **Azure Key Vault** (Secrets management)
- **Power BI** (Data visualization)
- **Parquet & Delta formats** (Efficient data storage & querying)

## 🚀 Pipeline Features

- ✅ **Dynamic data detection** (based on current date)
- ✅ **Secure authentication** via **Service Principal** & **Key Vault**
- ✅ **Data cleansing & validation** with defined schemas
- ✅ **SCD Type 1** implementation in the **gold layer**
- ✅ **Consolidated reporting dataset** (joined on primary keys, transactions, and date columns)
- ✅ **Interactive dashboards** in **Power BI**

## 🔐 Security Highlights

- Uses **Azure Key Vault** to store sensitive credentials (Client ID, Secret).
- Securely mounts **ADLS** in **Databricks** using **Service Principal Authentication**.

## 📈 Reporting

- The final consolidated dataset (in **Parquet format**) powers **Power BI dashboards**.
- Dashboards include insights into **transactions**, **loan payments**, and **customer activities**.

---

### 📬 Contact

*Aniket Gupta*  
[LinkedIn](https://www.linkedin.com/in/aniketgupta00077/) | aniketgupta00077@gmail.com
