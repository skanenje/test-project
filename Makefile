# Simple Makefile for running/building the project

.PHONY: build run web fmt vet test test-unit test-integration test-e2e test-coverage clean

build:
	go build ./...

run:
	go run main.go

web:
	go run main.go web 8080

fmt:
	gofmt -w .

vet:
	go vet ./...

# Run all tests (including package-specific tests)
test:
	go test ./...

# Run unit tests only (fast tests - organized tests)
test-unit:
	go test -v ./tests/unit/...

# Run package-specific unit tests (storage, schema, etc.)
test-pkg-unit:
	go test -v ./storage/... ./schema/...

# Run integration tests (component interactions)
test-integration:
	go test -v ./tests/integration/...

# Run end-to-end tests (full system tests)
test-e2e:
	go test -v ./tests/e2e/...

# Run all organized tests (tests/ directory only)
test-organized:
	go test -v ./tests/...

# Run all tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./tests/... ./storage/... ./schema/...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run tests across all test directories
test-all: test-pkg-unit test-organized
	@echo "All test suites completed"

# Clean up test artifacts
clean:
	rm -f coverage.out coverage.html
	go clean -testcache
