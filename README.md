# Data Engineering with Apache Airflow & GCP

This repository contains the practical lessons and code examples for the **Data Engineering Course** delivered as part of the government's digital transformation program. The curriculum focuses on building robust ETL/ELT pipelines using **Apache Airflow**, **Apache Beam (Dataflow)**, and **Google Cloud Platform (GCP)**.

---

## 🏗️ The Medallion Architecture

Most of the pipelines in this course follow the **Medallion Architecture**, a data design pattern used to logically organize data in a lakehouse:

1. **Bronze (Raw):** The landing zone. Data is ingested directly from sources (like Scrapy or APIs) in its original format (HTML, JSON, CSV).
2. **Silver (Cleaned/Filtered):** Data is cleaned, standardized, and validated. We use **Apache Beam/Dataflow** to transform "Bronze" HTML files into structured Parquet files.
3. **Gold (Curated):** Business-ready data. This layer is usually handled within BigQuery using **Dataform** to create final tables for BI and analytics.

---

## 🕷️ Web Scraping with Scrapy

In this course, we utilize **Scrapy** as our primary ingestion engine.

* **Purpose:** To extract historical sports data and astronaut information from various web sources.
* **Orchestration:** Airflow manages the Scrapy spiders, ensuring they run on schedule and successfully deposit raw files into **Google Cloud Storage (GCS)** buckets (the Bronze layer).

---

## 🛠️ Apache Airflow: The Orchestrator

Airflow is the "brain" of our operations. The examples provided ( `ejemplo1.py` through `ejemplo7.py`) cover the core concepts:

* **DAGs & Operators:** Defining workflows and using `PythonOperator` for custom logic.
* **Sensors:** Using `PythonSensor` to wait for data availability before proceeding (`ejemplo5.py`).
* **XComs:** Sharing data between tasks, such as passing API results to downstream processing (`ejemplo4.py`).
* **Branching:** Creating conditional paths in a pipeline based on the day of the week or execution date (`ejemplo7.py`).
* **Monitoring:** Integrating **Slack notifications** to alert on pipeline failures (`ejemplo6.py`).

---

## 🌊 Google Cloud Dataflow (Apache Beam)

For heavy-duty transformations (Bronze to Silver), we use **Apache Beam** (running on **Cloud Dataflow**).

**File:** `apache_beam_plata.py`
This script demonstrates a real-world transformation:

1. **Read:** Matches HTML files in GCS.
2. **Transform:** The `TransformacionHTML` class cleans table headers, standardizes team names, and enforces data types using `pandas`.
3. **Write:** Outputs the cleaned data to Parquet format in the Silver bucket, optimized for BigQuery ingestion.

---

## 📊 Google Cloud Dataform

Once data is in BigQuery, we use **Dataform** to manage SQL-based transformations.

**File:** `creacion_dataform.py`
We automate the infrastructure setup using Airflow:

* **DataformCreateRepositoryOperator:** Creates the central repository for SQLX files.
* **DataformCreateWorkspaceOperator:** Sets up individual developer environments.
This ensures that the Gold layer is built using version-controlled, documented, and tested SQL.

---

## 🛠️ Data Architecture Image

<img width="1084" height="373" alt="DataArchitecture" src="https://github.com/user-attachments/assets/9cc8a92f-3875-47a0-840e-b8c93fdb2f02" />


## 🚀 How to Use This Repo

1. **Environment Setup:** Ensure you have an Airflow environment (or Astro CLI) and a GCP project.
2. **Variables:** Set up your GCP project IDs and region in Airflow Variables or Environment Variables.
3. **Run DAGs:** Start with `ejemplo1.py` to understand basic scheduling and progress toward the complex `multi_rama_ejemplo`.
