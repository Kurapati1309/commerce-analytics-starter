#                                                                   Moonlight Aura Data Warehouse

**🧠 Powered by Moonlight Aura — Secure Data Engineering for Modern Businesses**

[![dbt](https://img.shields.io/badge/dbt-v1.10.13-brightgreen)](https://www.getdbt.com/)
[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-embedded%20OLAP-orange)](https://duckdb.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 🏗️ What is This Project?

**Moonlight Aura Data Warehouse** is a **complete end-to-end data engineering and analytics platform**, built to simulate how real companies manage, transform, and analyze their business data — securely and efficiently.

This project demonstrates how to design a **modern, lightweight, cloud-ready data warehouse** using **Python**, **DuckDB**, and **dbt (Data Build Tool)** — similar to how enterprise systems use Snowflake, BigQuery, or Redshift.

Think of it as the **core engine** behind e-commerce or SaaS analytics — processing millions of transactions, sales, and returns, and turning them into dashboards or KPIs for decision-making.

---

## 🧩 Key Purpose

✅ Build and operate a secure, scalable **data warehouse** for any industry (retail, hospitality, SaaS, etc.)
✅ Transform raw data (CSV/API) into structured analytics models
✅ Automate reporting and business insights
✅ Create a foundation for BI tools like **Power BI** or **Tableau**
✅ Showcase end-to-end enterprise-grade data engineering skills

---

## ⚙️ Tech Stack

| Layer               | Technology            | Purpose                                                 |
| ------------------- | --------------------- | ------------------------------------------------------- |
| **Data Lake**       | DuckDB                | Embedded OLAP engine storing all raw + analytics tables |
| **Transformation**  | dbt (Data Build Tool) | SQL modeling, lineage, and orchestration                |
| **Scripting**       | Python 3.12           | Automation, ETL orchestration, and validation           |
| **Storage Format**  | Parquet + CSV         | Compressed columnar storage for analytics               |
| **Version Control** | Git + GitHub          | Code collaboration and deployment                       |

---

## 🧱 Architecture Overview

### 🔹 Medallion Data Flow

Raw → Staging → Marts (Facts + Dimensions) → KPIs

```text
 ┌────────────┐
 │   Raw CSV  │     →  Raw layer (landing zone)
 └─────┬──────┘
       │
 ┌─────▼──────┐
 │  Staging   │  → Cleaned & standardized data
 └─────┬──────┘
       │
 ┌─────▼──────┐
 │   Marts    │  → Facts, dimensions, analytics joins
 └─────┬──────┘
       │
 ┌─────▼──────┐
 │   KPIs     │  → Dashboards, reports, visual analytics
 └────────────┘
```

---

## 📊 Comparison — Moonlight Aura vs Major Data Warehouses

| Feature       | **Moonlight Aura Data Warehouse (DuckDB)** | **Snowflake**       | **BigQuery**     | **Redshift**     |
| ------------- | ------------------------------------------ | ------------------- | ---------------- | ---------------- |
| Hosting       | Local / Cloud Embedded                     | Fully Cloud         | Fully Cloud      | Fully Cloud      |
| Cost          | 💸 Free / Open Source                      | Paid                | Paid             | Paid             |
| Scalability   | Medium (ideal for SMBs/startups)           | Very High           | Very High        | High             |
| Performance   | High for analytical queries                | Excellent           | Excellent        | Good             |
| Data Formats  | Parquet, CSV                               | Proprietary storage | Proprietary      | Proprietary      |
| Security      | Local control + Encryption                 | Enterprise-level    | Enterprise-level | Enterprise-level |
| Ease of Setup | ⚡ Instant (no cloud setup)                 | Complex setup       | Easy (cloud)     | Medium           |
| Best Use Case | In-house data analytics, small-mid clients | Large enterprises   | ML/AI workloads  | Cloud data marts |

---

## 🔐 Security and Scalability

**Security:**

* All client data resides inside your controlled DuckDB database file (`lake/commerce.duckdb`).
* Uses local encryption, access permissions, and private schema separation (e.g., each motel/client can have isolated access).
* Works completely offline — no cloud exposure unless connected.

**Scalability:**

* Handles millions of rows efficiently using columnar Parquet storage.
* Ideal for startups, small-to-medium-scale apps, or prototype analytics systems.
* Can later migrate seamlessly to Snowflake, BigQuery, or Databricks.

---

## 🧮 Example Business Models Built

| Model Name               | Description                                            |
| ------------------------ | ------------------------------------------------------ |
| **fct_daily_sales**      | Daily sales aggregation (date, orders, units, revenue) |
| **fct_top_skus**         | Top-selling SKUs by date and revenue                   |
| **fct_channel_sales**    | Channel-based revenue and unit metrics                 |
| **fct_returns_summary**  | Daily return rates and refund analysis                 |
| **fct_marketing_roas**   | Marketing spend vs. sales (ROAS) analysis              |
| **fct_commerce_summary** | Unified daily performance dashboard model              |

---

## 🧠 Example Use Cases

* 🛍️ **E-commerce analytics** → sales trends, returns, and ROAS
* 🏨 **Motel management analytics** → bookings, occupancy, and revenue
* 💳 **Finance data marts** → transaction pipelines, KPI dashboards
* ⚙️ **Startup analytics stack** → BI-ready metrics for investors
* 🧾 **Client analytics service** → manage multiple clients securely via schema isolation

---

## 🚀 Setup Instructions

### 1️⃣ Clone the repository

```bash
git clone https://github.com/Kurapati1309/commerce-analytics-starter.git
cd commerce-analytics-starter/commerce-analytics
```

### 2️⃣ Create and activate virtual environment

```bash
python -m venv .venv
source .venv/bin/activate   # (Linux/Mac)
.venv\Scripts\activate     # (Windows)
```

### 3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

### 4️⃣ Run dbt models

```bash
dbt run
```

### 5️⃣ View documentation

```bash
dbt docs generate
dbt docs serve
```

Then open browser → [http://localhost:8080](http://localhost:8080)

---

## 🧾 Outputs & Exports

* All generated analytics models (Parquet + CSV) stored under `exports/`
* Ready to connect with Power BI, Tableau, or Excel directly.

Example export:

```bash
duckdb ../lake/commerce.duckdb -c "SELECT * FROM analytics.fct_commerce_summary LIMIT 10;"
```

---

## 💡 Why Choose Moonlight Aura Data Warehouse

| Benefit           | Description                                           |
| ----------------- | ----------------------------------------------------- |
| 💼 Business-Ready | Designed for real client data across industries       |
| 🔒 Secure         | Data stays fully under your control                   |
| ⚡ Fast            | Query billions of rows in seconds with DuckDB         |
| 🧱 Modular        | dbt structure allows easy model addition              |
| 🔁 Portable       | Works locally or on any cloud storage                 |
| 🌍 Extensible     | Can integrate with APIs, Airflow, or Databricks later |

---

## 🧰 Future Enhancements

* 🌐 Deploy as cloud-based API (REST or GraphQL)
* ⚙️ Automate jobs using Airflow / Prefect
* 📊 Integrate Power BI or Looker dashboards
* 🔁 Add CI/CD pipelines with GitHub Actions
* ☁️ Migrate to Databricks or Snowflake for enterprise scale

---

---

## 🧠 Moonlight Aura Data Warehouse — Lineage & Analytics Overview

The **Moonlight Aura Data Warehouse** is a secure, modular analytics platform designed to transform raw client data into real-time, business-ready insights.

This dbt-powered architecture automates the flow of data from ingestion to analytics using **DuckDB**, **dbt**, and **Python** — making it ideal for:
- E-commerce platforms (orders, marketing, and return analytics)
- Hotel/Motel systems (occupancy, housekeeping, and revenue metrics)
- Restaurants & delivery apps (menu performance and order trends)
- Marketing agencies (ad spend ROI and engagement insights)

---

### ⚙️ Architecture Flow
| Layer | Description | Example Models |
|:------|:-------------|:---------------|
| **Raw (Bronze)** | Ingests unprocessed client data (CSV, API, event logs). | `raw.orders_raw`, `raw.ad_spend_raw`, `raw.events_stream` |
| **Staging (Silver)** | Cleans and standardizes fields for analytics. | `stg_orders`, `stg_products`, `stg_inventory` |
| **Analytics (Gold)** | Business-ready facts & KPIs used by dashboards. | `fct_commerce_summary`, `fct_returns_summary`, `marketing_roas` |

---

### 🧩 dbt Lineage Graph
Below is a visual representation of all transformations within the warehouse — from raw ingestion to analytics marts.

![dbt Lineage Graph](./images/dbt_lineage.png)

Each node represents a dbt model; arrows show dependencies and transformation flow.
This makes it simple to audit, scale, and onboard client datasets securely.

---

### 📘 Example Model — `fct_returns_summary`

| Column | Type | Description |
|:--------|:------|:-------------|
| `date` | DATE | Calendar date of aggregation |
| `orders` | BIGINT | Number of total orders |
| `returned_units` | BIGINT | Total returned items |
| `returned_value` | DOUBLE | Dollar value of returned items |
| `return_rate` | DOUBLE | Return ratio (%) |

This fact table summarizes e-commerce returns for performance tracking and ROAS optimization.

---

### 🚀 Vision
> “🧠 Powered by Moonlight Aura — Secure Data Engineering for Modern Businesses.”

Designed for clients who want to transform their **raw data into reliable analytics tables + KPIs** — fast, scalable, and cloud-ready.

--

## 👨‍💻 Author & Credits

**Hemanth Kumar Kurapati**
Founder — Moonlight Aura Data Warehouse
📍 Houston, TX
✉️ [hk485@nau.edu](mailto:hk485@nau.edu)
🔗 [LinkedIn](https://www.linkedin.com/in/hemanth-kurapati1021)
🔗 [GitHub](https://github.com/Kurapati1309)

---

### 🏢 Built and Maintained by **Moonlight Aura Studios**

                                Delivering creative, data-driven, and scalable software for the modern world.
