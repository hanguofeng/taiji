SHELL=/bin/bash

DIRS := $(shell find . -maxdepth 1 -type d)
DIRS := $(basename $(patsubst ./%,%,$(DIRS)))
BUILDDIRS = $(DIRS:%=build-%)
CLEANDIRS = $(DIRS:%=clean-%)

all: $(BUILDDIRS)
$(DIRS): $(BUILDDIRS)
$(BUILDDIRS):
	make -C $(@:build-%=%)

clean: $(CLEANDIRS)
$(CLEANDIRS):
	make -C $(@:clean-%=%) clean

.PHONY: all clean $(BUILDDIRS) $(CLEANDIRS)
