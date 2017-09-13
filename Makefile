GO ?= go
export GOPATH := $(CURDIR)/_vendor

all:
	$(GO) build

submodules:
	git submodule sync
	git submodule update --init --recursive
