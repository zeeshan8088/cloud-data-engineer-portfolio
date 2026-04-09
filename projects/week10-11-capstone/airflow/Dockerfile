# ============================================================
# RetailFlow — Custom Airflow Image
# ============================================================
# Extends the official Airflow 2.8 image with:
#   - dbt-bigquery     (core transformation tool)
#   - great-expectations (data quality)
#   - google-cloud-bigquery (BQ client for callbacks)
#   - pandas / pyarrow      (GE validation uses pandas)
#   - faker                 (ingestion scripts)
#   - requests              (fetch_orders.py)
#
# Build:  docker build -t retailflow-airflow:latest .
# ============================================================

FROM apache/airflow:2.8.4-python3.10

# Switch to root to install system deps
USER root

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Back to airflow user for pip installs
USER airflow

COPY requirements.txt /tmp/airflow_requirements.txt

RUN pip install --no-cache-dir --user -r /tmp/airflow_requirements.txt

# Verify critical installs
RUN python -c "import dbt; print('dbt OK')" && \
    python -c "import great_expectations; print('GE OK')" && \
    python -c "from google.cloud import bigquery; print('BQ client OK')"
