export GOPATH=$(PWD)

build:
	go install gumshoe/core

deps:
	git submodule update --init

run-test: build
	go test ...gumshoe/core

web: build
	go install ...gumshoe/gumshoe_server

run-web: web
	bin/gumshoe_server

benchmark: build
	go install ...gumshoe/benchmark

# Runs all benhcmarks appearing in any *_test.go files.
run-benchmark: build_benchmark
	bin/benchmark -minimal-set=true
