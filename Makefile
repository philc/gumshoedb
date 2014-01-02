
GOPATH := $$PWD

build:
	GOPATH=$(GOPATH) go install gumshoe/core

test: build
	GOPATH=$(GOPATH) go test ...gumshoe/core

build_web: build
	GOPATH=$(GOPATH) go install ...gumshoe/gumshoe_server

run_web: build_web
	bin/gumshoe_server

# Runs all benhcmarks appearing in any *_test.go files.
benchmark: build_benchmark
	bin/benchmark -minimal-set=true

build_benchmark: build
	GOPATH=$(GOPATH) go install ...gumshoe/benchmark
