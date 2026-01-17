# Simple Makefile for running/building the project

.PHONY: build run web run-web fmt vet test

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

test:
	go test ./...
