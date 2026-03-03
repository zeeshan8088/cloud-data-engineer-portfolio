# Airflow – Docker Setup (Design Phase)

## Why Docker for Airflow
Apache Airflow is a distributed system, not a local script.
Running it via Docker ensures:
- OS-independent behavior
- Production-like setup
- Clean separation of services

## Core Airflow Components
- Webserver: UI & DAG visibility
- Scheduler: Decides task execution
- Metadata DB: Source of truth
- Executor/Workers: Execute tasks

## Execution Model
This setup mirrors how Airflow runs in:
- Docker Compose (local)
- Kubernetes
- Managed services (GCP Composer, MWAA)

No tasks are executed at this stage.
This folder represents the orchestration foundation.