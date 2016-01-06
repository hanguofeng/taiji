SHELL=/bin/bash

BIN_NAME := $(notdir $(shell pwd))

$(BIN_NAME):
	godep go build

clean:
	test ! -e $(BIN_NAME) || rm -v $(BIN_NAME)

format:
	gofmt -w *.go
	goimports -w *.go

.PHONY: all clean format $(BUILDDIRS) $(CLEANDIRS)
