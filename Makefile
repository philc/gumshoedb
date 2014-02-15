PLATFORM=$(shell uname)

fmt:
	gofmt -s -l -w src

release:
ifeq ($(PLATFORM), Darwin)
  # We use vagrant to build a gumshoedb linux binary
	vagrant up
	vagrant ssh -c 'cd gumshoedb && make release'
else
	make
endif
