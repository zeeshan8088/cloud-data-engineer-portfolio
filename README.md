Cloud Data Engineering Pipeline — Airflow + BigQuery
Overview

This project demonstrates the implementation of a modern data engineering pipeline designed to ingest, orchestrate, store, and analyze cryptocurrency market data using industry-standard tooling.

The pipeline automates the ingestion of Bitcoin price data from a public API and processes it through a structured data architecture consisting of:

Data ingestion

Data lake storage

Data warehouse loading

Analytical transformation

Workflow orchestration

The entire pipeline is orchestrated using Apache Airflow, enabling scheduling, retry policies, and operational monitoring.

Architecture

The system architecture follows a layered data engineering design.

External API → Airflow Orchestration → Data Lake → BigQuery Raw Layer → SQL Analytics Layer

Components:

Layer	Technology
API Ingestion	Python + REST API
Orchestration	Apache Airflow
Storage Layer	Local Data Lake (simulating cloud object storage)
Data Warehouse	Google BigQuery
Transformation	SQL
Infrastructure	Docker

Architecture diagram:

architecture/pipeline_architecture.png
Pipeline Workflow

The pipeline executes the following sequence:

1. API Data Extraction

Bitcoin price data is retrieved from the CoinGecko API using a Python ingestion script.

The extracted record contains:

timestamp
price
2. Data Lake Storage

The extracted API payload is persisted to a local data lake layer.

Location:

data_lake/api_data/api_data.json

This layer simulates a cloud object storage system such as Google Cloud Storage or Amazon S3.

3. Data Warehouse Ingestion

The stored API record is loaded into Google BigQuery using the Python BigQuery client library.

Target dataset:

data_engineering_pipeline

Target table:

bitcoin_api_data
4. Analytical Transformation

A SQL transformation job generates an analytics table containing processed insights derived from the raw ingestion layer.

Target table:

bitcoin_price_analytics
Apache Airflow Orchestration

The pipeline is orchestrated using an Airflow DAG containing three primary tasks:

extract_api_data
       ↓
load_api_to_bigquery
       ↓
run_sql_transformation

The workflow includes:

Daily scheduling

Retry policies

SLA monitoring

Task dependency management

Scheduling configuration:

schedule_interval = "@daily"

Retry configuration:

retries = 3
retry_delay = 2 minutes
Running the Project

Start Airflow services:

docker compose up

Access Airflow UI:

http://localhost:8080

Trigger the pipeline DAG:

day6_api_pipeline
Key Engineering Concepts Demonstrated

This project showcases multiple real-world data engineering patterns:

Workflow orchestration using Apache Airflow

API ingestion pipelines

Data lake architecture

BigQuery data warehouse integration

SQL transformation pipelines

Dockerized development environment

Task retries and operational logging

Future Enhancements

Potential production improvements include:

Migrating the data lake layer to Google Cloud Storage

Implementing data quality validation checks

Introducing dbt for transformation management

Adding monitoring and alerting integration