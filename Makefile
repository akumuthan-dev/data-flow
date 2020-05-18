.PHONY: check
check:
	flake8 .
	black --check --exclude='/(venv|dataflow\/ons_scripts)/' --skip-string-normalization .
	mypy .

.PHONY: format
format:
	black --check --exclude='/(venv|dataflow\/ons_scripts)/' --skip-string-normalization .

.PHONY: test
test:
	pytest --cov=dataflow --cov-report=term-missing

.PHONY: compile-requirements
compile-requirements:
	pip-compile
