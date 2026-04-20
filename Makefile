SHELL        := bash
.SHELLFLAGS  := -Eeuo pipefail -c

MODULES := . ./e2e ./tools

TEGUH_SQL     := sql/teguh.sql
TEGUH_SQL_DST := e2e/testdata/teguh.sql

.PHONY: fetch-schema
fetch-schema: $(TEGUH_SQL_DST) ## Copy teguh.sql into e2e testdata

$(TEGUH_SQL_DST): $(TEGUH_SQL)
	cp $(TEGUH_SQL) $(TEGUH_SQL_DST)

.PHONY: format
format: ## Format all Go code with golangci-lint fmt
	for m in $(MODULES); do (cd "$$m" && go tool golangci-lint fmt ./...); done

.PHONY: lint
lint: ## Run golangci-lint on all modules
	for m in $(MODULES); do (cd "$$m" && go tool golangci-lint run ./...); done

.PHONY: test
test: ## Run unit tests
	go test -race ./...

.PHONY: test.e2e
test.e2e: fetch-schema ## Run e2e tests
	go test -race -count=1 -timeout 180s ./e2e/...

.PHONY: help
help:
	@grep -E '^[a-zA-Z_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-20s %s\n", $$1, $$2}'
