.PHONY: check
check:
	flake8 .
	black --skip-string-normalization --check .
	mypy .

.PHONY: format
format:
	black --skip-string-normalization .
