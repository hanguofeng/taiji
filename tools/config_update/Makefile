SHELL=/bin/bash

BIN_NAME := $(notdir $(shell pwd))

$(BIN_NAME):
	go build

clean:
	test ! -e $(BIN_NAME) || rm -v $(BIN_NAME)

.PHONY: all clean $(BUILDDIRS) $(CLEANDIRS)
