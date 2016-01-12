SHELL=/bin/bash

DIRS := $(shell find . -maxdepth 1 -type d)
DIRS := $(basename $(patsubst ./%,%,$(DIRS)))
BUILDDIRS = $(DIRS:%=build-%)
CLEANDIRS = $(DIRS:%=clean-%)
FORMATDIRS = $(DIRS:%=format-%)

all: $(BUILDDIRS)
$(DIRS): $(BUILDDIRS)
$(BUILDDIRS):
	make -C $(@:build-%=%)

clean: $(CLEANDIRS)
$(CLEANDIRS):
	make -C $(@:clean-%=%) clean

format: $(FORMATDIRS)
$(FORMATDIRS):
	make -C $(@:format-%=%) format

.PHONY: all clean format $(BUILDDIRS) $(CLEANDIRS)
