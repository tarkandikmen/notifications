.PHONY: up down build test vet migrate migrate-down

up:
	docker-compose up -d --build

down:
	docker-compose down -v

build:
	go build -o notifications ./cmd/notifications

test:
	go test ./...

vet:
	go vet ./...

migrate:
	go run ./cmd/notifications migrate up

migrate-down:
	go run ./cmd/notifications migrate down
