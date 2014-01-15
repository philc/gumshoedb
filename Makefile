SUBPACKAGES=$(shell find src -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)

export GOPATH=$(PWD):$(PWD)/vendor

build: deps
	go install gumshoe

deps:
	git submodule update --init

test:
	go test $(SUBPACKAGES)

web: build
	go build -o bin/gumshoe_server server

run-web: web
	bin/gumshoe_server

benchmark: build
	go install benchmark

# Runs all benhcmarks appearing in any *_test.go files.
run-benchmark: benchmark
	bin/benchmark -minimal-set=true

fmt:
	gofmt -s -l -w src
