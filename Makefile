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

# Run all tests
test:
	go test ./...

# Run unit tests only (fast tests)
test-unit:
	go test -v ./tests/unit/...

# Run integration tests (component interactions)
test-integration:
	go test -v ./tests/integration/...

# Run end-to-end tests (full system tests)
test-e2e:
	go test -v ./tests/e2e/...

# Run all tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./tests/... ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run tests across all test directories
test-all: test-unit test-integration test-e2e
	@echo "All test suites completed"

# Clean up test artifacts
clean:
	rm -f coverage.out coverage.html
	go clean -testcache
