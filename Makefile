.PHONY: fmt test test-race vet lint integration up down

fmt:
	@test -z "$$(gofmt -l .)"

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run

integration:
	go test -tags=integration ./internal/integration/...

up:
	docker compose -f deployments/compose.yaml up -d postgres rabbitmq

down:
	docker compose -f deployments/compose.yaml down -v

