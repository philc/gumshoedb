export GOPATH=$(PWD):$(PWD)/vendor

build: deps
	go install gumshoe/core

deps:
	git submodule update --init

run-test: build
	go test ...gumshoe/core

web: build
	go build -o bin/gumshoe_server gumshoe/server

run-web: web
	bin/gumshoe_server

benchmark: build
	go install ...gumshoe/benchmark

# Runs all benhcmarks appearing in any *_test.go files.
run-benchmark: benchmark
	bin/benchmark -minimal-set=true
