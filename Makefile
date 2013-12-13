
GOPATH := $$PWD

build:
	go build

run: build
	./go_playground

test: build
	go test

# Runs all benhcmarks appearing in any *_test.go files.
benchmark: build
	GOPATH=$(GOPATH) go install benchmark
	bin/benchmark
