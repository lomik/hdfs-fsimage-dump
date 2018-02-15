GO ?= go
export GOPATH := $(CURDIR)/_vendor

MODULE:=github.com/lomik/hdfs-fsimage-dump

all:
	$(GO) build $(MODULE)
