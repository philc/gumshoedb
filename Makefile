SUBPACKAGES=$(shell find src -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)
PLATFORM=$(shell uname)
PRETTYBENCH=$(shell which prettybench)

export GOPATH=$(PWD):$(PWD)/vendor

default: web

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

benchmark:
ifeq ($(PRETTYBENCH),)
	go test -run=NONE -bench=. gumshoe
else
	go test -run=NONE -bench=. gumshoe | prettybench
endif

synthetic-benchmark:
ifeq ($(PRETTYBENCH),)
	go test -run=NONE -bench=. synthetic
else
	go test -run=NONE -bench=. synthetic | prettybench
endif

fmt:
	gofmt -s -l -w src

clean:
	rm -fr bin build pkg

release:
ifeq ($(PLATFORM), Darwin)
	vagrant up
	vagrant ssh -c 'cd gumshoedb && make release'
else
	make web
endif
