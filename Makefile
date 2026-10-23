test:
	go test -v -race ./...

# End-to-end suite against a real network. Starts a local Stellar Quickstart
# container, streams from it, and removes it again. Opt-in because it needs
# Docker and the network, while `make test` stays offline and deterministic.
test-e2e:
	PRISM_E2E=1 go test -tags e2e -race -count=1 ./internal/cli -run TestE2E -v

lint:
	golangci-lint run

fmt:
	go fmt ./...

build:
	go build ./...

