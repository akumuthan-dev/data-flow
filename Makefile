.PHONY: check
check:
	flake8 .
	black --exclude=venv --skip-string-normalization --check .
	mypy .

.PHONY: format
format:
	black --exclude=venv --skip-string-normalization .
