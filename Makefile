# This is a maintainer-centric Makefile to help performing
# typical operations for all projects in this repo.
#
# Example:
#   make build


DIRS=$(shell find . -name project.json -exec dirname {} \;)

all:
	@echo "Usage:   make <dotnet-command>"
	@echo "Example: make build - runs 'dotnet build' for all projects"

.PHONY: test

%:
	for d in $(DIRS) ; do dotnet $@ $$d; done

test:
	dotnet test test/Confluent.Kafka.UnitTests

