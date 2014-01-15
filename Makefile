export GOPATH=$(PWD):$(PWD)/vendor

build: deps
	go install gumshoe

deps:
	git submodule update --init

run-test: build
	go test ...gumshoe

web: build
	go build -o bin/gumshoe_server server

run-web: web
	bin/gumshoe_server

benchmark: build
	go install ...benchmark

# Runs all benhcmarks appearing in any *_test.go files.
run-benchmark: benchmark
	bin/benchmark -minimal-set=true

fmt:
	gofmt -s -l -w src
