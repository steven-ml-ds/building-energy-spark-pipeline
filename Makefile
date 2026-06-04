.PHONY: install dev lint fmt test cov train produce stream kafka-up kafka-down clean

install:        ## Install runtime package
	pip install -e .

dev:            ## Install with dev + viz extras and pre-commit hooks
	pip install -e ".[dev,viz]"
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

kafka-up:       ## Start Kafka (KRaft) via docker compose
	docker compose -f docker/docker-compose.yml up -d

kafka-down:     ## Stop Kafka
	docker compose -f docker/docker-compose.yml down

clean:          ## Remove build/test artifacts
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .coverage htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +
