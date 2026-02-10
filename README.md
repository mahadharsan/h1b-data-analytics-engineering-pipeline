# H1B Visa Analytics Engineering Pipeline (2023-2025)

## 🎯 Project Objective
Processed and analyzed **200,000+ raw USCIS records** representing **1.2M+ H1B applications**. The core challenge was performing **Entity Resolution** on 1k+ messy employer names to provide accurate sponsorship insights.

<p align="center">
  <img src="assets/architecture_diagram.png" alt="H1B Pipeline Architecture" width="800">
</p>


## 🏗️ Technical Architecture
**Postgres** ➔ **Airbyte** ➔ **BigQuery** ➔ **PySpark** ➔ **dbt** ➔ **Looker Studio**

* **Ingestion:** Airbyte managed EL from Postgres to BigQuery.
* **Processing:** PySpark for initial large-scale cleaning and data type standardization.
* **Modeling:** dbt (Data Build Tool) for modular SQL modeling (Staging -> Intermediate -> Marts).
* **BI:** Looker Studio for geospatial and time-series trend analysis.

The dbt pipeline follows a **Medallion Architecture** (Bronze, Silver, Gold) to transform 200k raw records into high-fidelity analytical marts.

## 🛠️ Key Engineering Features
* **Regex-based Entity Resolution:** Consolidated variations of employer names (e.g., "Amazon.com Services LLC" vs "Amazon") using dbt macros.
* **Automated Data Lineage:** Full documentation of the 3-layer dbt architecture.
* **Dynamic Analytics:** Integrated 3 years of visa data with a 3.97 GPA level of precision (wit intended!).

## 📊 Dashboard
[Link to Looker Studio Dashboard](https://lookerstudio.google.com/reporting/c1a28f6a-c990-4c2b-995d-7e3c834f3d10)
