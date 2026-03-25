# Variables
IMAGE_NAME = week8-etl
IMAGE_TAG  = latest
FULL_IMAGE = $(IMAGE_NAME):$(IMAGE_TAG)

# Default target — shows available commands
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make build       — Build the Docker image"
	@echo "  make run         — Run the ETL pipeline in Docker"
	@echo "  make run-local   — Run the pipeline directly with Python (no Docker)"
	@echo "  make test        — Run unit tests"
	@echo "  make lint        — Run ruff linter"
	@echo "  make clean       — Remove Docker image and caches"
	@echo "  make size        — Show image size"

.PHONY: build
build:
	docker build -t $(FULL_IMAGE) .

.PHONY: run
run:
	docker run --rm \
		-e ENVIRONMENT=local \
		-e HIGH_VALUE_THRESHOLD=500.0 \
		$(FULL_IMAGE)

.PHONY: run-local
run-local:
	cd etl && python etl_pipeline.py

.PHONY: test
test:
	cd etl && python -m pytest tests/ -v --cov=etl_pipeline --cov-report=term-missing

.PHONY: lint
lint:
	cd etl && ruff check .

.PHONY: clean
clean:
	docker rmi $(FULL_IMAGE) 2>/dev/null || true
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

.PHONY: size
size:
	docker images $(IMAGE_NAME)