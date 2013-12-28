
GOPATH := $$PWD

build:
	GOPATH=$(GOPATH) go install gumshoe/core

run: build
	./go_playground

test: build
	GOPATH=$(GOPATH) go test ...gumshoe/core

build_web: build
	GOPATH=$(GOPATH) go install ...gumshoe/gumshoe_server

run_web: build_web
	bin/gumshoe_server

# Runs all benhcmarks appearing in any *_test.go files.
benchmark: build_benchmark
	bin/benchmark

build_benchmark: build
	GOPATH=$(GOPATH) go install ...gumshoe/benchmark
