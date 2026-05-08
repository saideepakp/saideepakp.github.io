# 🏭 End-to-End Data Engineering Project — FMCG Domain

A real-world data engineering project built on **Databricks Free Edition**, simulating a retail acquisition scenario where a parent company acquires a child company and consolidates their data into a unified **Lakehouse architecture**.

---

## 📌 Business Scenario

A large retail company (parent) acquires a smaller sports nutrition company (child — Sports Bar). The goal is to build a unified data pipeline that consolidates historical data from both companies and ingests day-to-day transactional data from the child company into a single source of truth.

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Databricks** (Free Edition) | Data processing & orchestration |
| **Apache Spark / PySpark** | Data transformation |
| **Delta Lake** | Storage format (ACID transactions) |
| **Amazon S3** | Raw data storage |
| **Medallion Architecture** | Bronze → Silver → Gold layers |
| **SQL** | Data validation & view creation |
| **Power BI** | Dashboard & reporting |
| **Databricks Workflows** | Pipeline orchestration |

---

## 🏗️ Architecture

```
Amazon S3 / Databricks Volumes
            ↓
        BRONZE LAYER
    (Raw data as-is)
            ↓
        SILVER LAYER
    (Cleaned & transformed)
            ↓
         GOLD LAYER
    (Unified & enriched)
            ↓
  vw_fact_orders_enriched
    (Denormalized view)
            ↓
        Power BI
     (Dashboard)
```

---

## 📂 Project Structure

```
de_project_/
├── parent_historical_data_transformation.ipynb
├── child_company_historical_data_transformation.ipynb
├── gold_validation_and_merge.ipynb
├── gold_layer_transformation.ipynb
└── incremental_load_orders.ipynb
```

---

## 📊 Data Scope

| Company | Historical Data | Day-to-Day Data |
|---|---|---|
| **Parent Company** | ✅ Processed | ❌ Out of scope |
| **Child Company** | ✅ Processed | ✅ Incremental pipeline |

---

## 🗂️ Medallion Architecture

### Bronze Layer
Raw data ingested as-is from source systems with minimal transformations:
- File metadata (`read_timestamp`, `file_name`, `file_size`)
- No business logic applied
- Append mode for transactional data, overwrite for dimension data

### Silver Layer
Cleaned and standardized data ready for business use:
- Duplicate removal
- Data type standardization
- Schema alignment between parent and child companies
- Data quality fixes (typos, invalid values, date format normalization)
- Fallback values for invalid records (`999999` for unknown customers)

### Gold Layer
Unified, analytics-ready data combining both companies:

| Table | Description |
|---|---|
| `customers` | Unified customer dimension |
| `products` | Unified product dimension |
| `gross_price` | Yearly price per product |
| `orders` | Unified fact table (monthly grain) |
| `dim_date` | Date dimension (Jan 2024 — Dec 2025) |
| `vw_fact_orders_enriched` | Denormalized view for Power BI |

---

## 📓 Notebooks

### 1. `parent_historical_data_transformation`
Ingests historical data for the parent company from Databricks Volumes:
- Reads 4 CSV files: `dim_customers`, `dim_gross_price`, `dim_products`, `fact_orders`
- Writes to Bronze schema
- Applies minimal transformations (data type casting)
- Writes to Silver schema
- Creates `dim_date` table in Silver

### 2. `child_company_historical_data_transformation`
Ingests historical data for the child company (Sports Bar) from Amazon S3:
- **Customers** — fixes city typos, null cities, title casing, adds `market`, `platform`, `channel` attributes
- **Products** — fixes "Protien" typo, generates `product_code` (MD5 hash), extracts `variant`, adds `division`
- **Gross Price** — normalizes date formats, fixes invalid prices, aggregates to yearly best price
- **Orders** — cleans dates, handles invalid IDs, joins with products for `product_code`

### 3. `gold_validation_and_merge`
Validates and aligns schemas before merging into Gold:
- Prints schemas of all parent and child Silver tables side by side
- Drops unnecessary columns (`product_id`, `order_id`)
- Aligns data types between parent and child (`price_inr` → integer, `sold_quantity` → integer)
- Final schema validation before Gold layer

### 4. `gold_layer_transformation`
Builds the unified Gold layer:
- Merges parent and child Silver tables using `unionByName`
- Aggregates child orders from daily → monthly grain to match parent
- Moves `dim_date` from Silver to Gold
- Creates denormalized view `vw_fact_orders_enriched`
- Adds dummy customer record for invalid orders (`customer_code = 999999`)

### 5. `incremental_load_orders`
Handles day-to-day transactional data for the child company:
- Reads new CSV files from S3 `landing/` folder
- Appends to Bronze, creates staging table for incremental batch
- Moves processed files to `processed/` folder
- Applies same Silver transformations as historical pipeline
- MERGEs into `silver.orders_child`
- Re-aggregates affected months and MERGEs into `gold.orders`
- Cleans up staging tables after completion

---

## 🔄 Incremental Pipeline Logic

The incremental pipeline uses a **staging table pattern** to process only new data efficiently:

```
New CSV files in S3 landing/
        ↓
Read → Bronze (append) + bronze.staging_orders (overwrite)
        ↓
Move files landing/ → processed/
        ↓
Transform → Silver MERGE + silver.staging_orders (overwrite)
        ↓
Find affected months from staging
        ↓
Re-aggregate ALL daily rows for affected months from Silver
        ↓
MERGE recalculated monthly totals into Gold
        ↓
DROP staging tables
```

**Why monthly re-aggregation?**
The Gold table stores data at monthly grain. When new daily orders arrive for a month that already has data in Gold, simply appending would cause double counting. Instead, we pull all daily rows for affected months from Silver and recalculate the monthly total from scratch — ensuring Gold is always 100% accurate.

---

## 🔍 Data Quality Decisions

| Issue | Decision | Justification |
|---|---|---|
| Invalid `customer_id` values | Replaced with `999999`, added `Unknown` dummy record | Retains valid order data while flagging data quality issue for business visibility |
| Invalid `product_id` values | Replaced with `99999999` | Avoids losing fact records, maintains join consistency |
| "Protien" typo in product names | Fixed using `regexp_replace` before MD5 generation | Ensures consistent `product_code` across historical and incremental loads |
| City name typos | Fixed using explicit mapping | Business team confirmed correct city values |
| Negative gross prices | Converted to positive | Assumed data entry error |
| Mixed date formats | Normalized using `coalesce` + `try_to_date` | Handles multiple source formats gracefully |

---

## 📐 Denormalized View

The `vw_fact_orders_enriched` view joins all Gold tables into a single analytics-ready dataset for Power BI:

```sql
SELECT
    -- Keys
    fo.date, fo.product_code, fo.customer_code,
    -- Date attributes
    dd.year, dd.month_name, dd.quarter, dd.year_quarter,
    -- Customer attributes
    dc.customer, dc.market, dc.platform, dc.channel,
    -- Product attributes
    dp.division, dp.category, dp.product, dp.variant,
    -- Metrics
    fo.sold_quantity, gp.price_inr,
    -- Derived metric
    (fo.sold_quantity * gp.price_inr) AS total_amount_inr
FROM gold.orders fo
LEFT JOIN gold.dim_date dd    ON fo.date = dd.month_start_date
LEFT JOIN gold.customers dc   ON fo.customer_code = dc.customer_code
LEFT JOIN gold.products dp    ON fo.product_code = dp.product_code
LEFT JOIN gold.gross_price gp ON fo.product_code = gp.product_code
                              AND YEAR(fo.date) = gp.year
```

---

## ⚙️ Orchestration

Two Databricks Jobs are set up for orchestration:

### Historical Job (run once)
```
Task 1: parent_historical_data_transformation
Task 2: child_company_historical_data_transformation
        ↓ (both feed into)
Task 3: gold_validation_and_merge
        ↓
Task 4: gold_layer_transformation
```
Tasks 1 and 2 run in **parallel**. Task 3 depends on both completing successfully.

### Incremental Job (scheduled daily)
```
Task 1: incremental_load_orders
```
Scheduled to run daily to pick up new order files from S3 landing folder.

---

## 📊 Power BI Dashboard

The dashboard connects directly to `vw_fact_orders_enriched` via the Databricks connector in Power BI Desktop.

**Connection steps:**
1. Open Power BI Desktop → Get Data → Databricks
2. Enter Server Hostname and HTTP Path from Databricks SQL Warehouse
3. Authenticate using Personal Access Token
4. Select `catalog` → `gold_schema` → `vw_fact_orders_enriched`
5. Load and build visuals

### Dashboard Screenshots

> **Sales Overview**
![Sales Overview Dashboard](Databricks\Screenshot 2026-05-08 090328.png)
*Replace with actual screenshot after dashboard is built*



---

## 🚀 How to Run

### Prerequisites
- Databricks Free Edition account
- AWS S3 bucket with child company data
- Databricks Volumes with parent company historical data
- Power BI Desktop (for dashboard)

### Step 1 — Set up S3 bucket structure
```
s3://de-project-child-company-data/
├── customers/
│   └── customers.csv
├── products/
│   └── products.csv
├── gross_price/
│   └── gross_price.csv
└── orders/
    ├── landing/    ← drop new daily files here
    └── processed/  ← files moved here after processing
```

### Step 2 — Set up Databricks Volumes
```
/Volumes/de_project_/bronze/raw_historical_data_parent_company/
├── dim_customers.csv
├── dim_gross_price.csv
├── dim_products.csv
└── fact_orders.csv
```

### Step 3 — Run Historical Job
Trigger the historical job manually in Databricks Workflows. All 4 tasks will run and build the complete Gold layer.

### Step 4 — Run Incremental Job
Drop new daily order CSV files into `s3://{path}` and trigger the incremental job (or wait for the scheduled run).

### Step 5 — Connect Power BI
Connect Power BI Desktop to `{catalog}.{gold_schema}.vw_fact_orders_enriched` using the Databricks connector.

---

## 📁 Catalog Structure

```
de_project_ (catalog)
├── bronze (schema)
│   ├── customers_parent
│   ├── products_parent
│   ├── gross_price_parent
│   ├── orders_parent
│   ├── customers_child
│   ├── products_child
│   ├── gross_price_child
│   └── orders_child
├── silver (schema)
│   ├── customers_parent
│   ├── products_parent
│   ├── gross_price_parent
│   ├── orders_parent
│   ├── customers_child
│   ├── products_child
│   ├── gross_price_child
│   ├── orders_child
│   └── dim_date
└── gold (schema)
    ├── customers
    ├── products
    ├── gross_price
    ├── orders
    ├── dim_date
    └── vw_fact_orders_enriched (view)
```

---

## 👤 Author

> Add your name and LinkedIn/GitHub profile here

---

## 📄 License

This project is open source and available for learning purposes.

---

## 📌 Disclaimer

This project is made by reference from [Add original course/tutorial/resource link here]. All credit for the original concept, dataset design, and project structure goes to the respective creator(s). I do not claim any ownership or copyright over the original project idea or datasets. This repository is solely for learning and portfolio purposes.
