SUBPACKAGES=$(shell find src -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)
PLATFORM=$(shell uname)

export GOPATH=$(PWD):$(PWD)/vendor

default: web migrator

web: build
	go build -o bin/gumshoe_server server

build: deps
	go install gumshoe

deps:
	git submodule update --init

run-web: web
	bin/gumshoe_server

migrator: build
	go build -o bin/migrator migrator

test:
	go test $(SUBPACKAGES)

benchmark:
	go test -run=NONE -bench=. gumshoe

synthetic-benchmark:
	go test -run=NONE -bench=. synthetic

fmt:
	gofmt -s -l -w src

clean:
	rm -fr bin build pkg

release:
ifeq ($(PLATFORM), Darwin)
	vagrant up
	vagrant ssh -c 'cd gumshoedb && make release'
else
	make
endif
