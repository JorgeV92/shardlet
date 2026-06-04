.PHONY: test race bench demo

export GOCACHE ?= $(CURDIR)/.gocache

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -bench=. -benchmem ./pkg/shardlet

demo:
	go run ./cmd/shardlet -workers 32 -ops 100000
