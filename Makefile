.PHONY: check
check:
	flake8 .
	black --check --exclude='/(venv|dataflow\/ons_scripts)/' --skip-string-normalization .
	mypy .

.PHONY: format
format:
	black --exclude='/(venv|dataflow\/ons_scripts)/' --skip-string-normalization .

.PHONY: test
test:
	pytest tests/unit --cov=dataflow --cov-report=term-missing

.PHONY: docker-build
docker-build:
	docker-compose -f docker-compose-test.yml build

.PHONY: test-integration
test-integration: docker-build
	docker-compose -f docker-compose-test.yml -p data-flow-test run --rm data-flow-test dockerize -wait tcp://data-flow-db-test:5432 && airflow initdb && pytest tests/integration

.PHONY: save-requirements
save-requirements:
	pip-compile requirements.in
	pip-compile requirements-tensorflow-worker.in
