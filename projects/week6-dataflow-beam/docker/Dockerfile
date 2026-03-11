# We use Python 3.11 — officially supported by Apache Beam
# Apache Beam's own base image — same one Dataflow uses internally
FROM apache/beam_python3.11_sdk:2.61.0

# Set working directory inside container
WORKDIR /pipeline

# Copy requirements first (Docker caching — faster rebuilds)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all pipeline code
COPY . .

# Default command — can be overridden when running container
CMD ["python", "local_pipelines/hospital_batch_pipeline.py"]