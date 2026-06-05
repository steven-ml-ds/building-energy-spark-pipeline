.PHONY: install dev lint fmt test cov train produce stream dashboard-export \
	up down kafka-up kafka-down clean

install:        ## Install runtime package
	pip install -e .

dev:            ## Install with dev + viz + dashboard extras and pre-commit hooks
	pip install -e ".[dev,viz,dashboard]"
	pre-commit install

lint:           ## Lint with ruff
	ruff check src tests

fmt:            ## Auto-format / fix with ruff
	ruff check --fix src tests
	ruff format src tests

test:           ## Run the test suite
	pytest

cov:            ## Run tests with coverage report
	pytest --cov --cov-report=term-missing

train:          ## Train the model (use ARGS="--sample-fraction 0.05" for a quick run)
	python -m energy_pipeline.batch_train $(ARGS)

produce:        ## Start the Kafka weather producer
	python -m energy_pipeline.producer $(ARGS)

stream:         ## Start the streaming inference job
	python -m energy_pipeline.stream_infer

dashboard-export: ## Bridge Kafka predictions -> Prometheus metrics (:8000/metrics)
	python -m energy_pipeline.dashboard_export $(ARGS)

up:             ## Start full stack: Kafka + Prometheus + Grafana (Grafana on :3000)
	docker compose -f docker/docker-compose.yml up -d

down:           ## Stop the full stack
	docker compose -f docker/docker-compose.yml down

kafka-up:       ## Start only Kafka (KRaft) via docker compose
	docker compose -f docker/docker-compose.yml up -d kafka

kafka-down:     ## Stop Kafka
	docker compose -f docker/docker-compose.yml down

clean:          ## Remove build/test artifacts
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
