# This is a maintainer-centric Makefile to help performing
# typical operations for all projects in this repo.
#
# Example:
#   make build


DIRS=$(shell find . -name project.json -exec dirname {} \;)
OS=$(shell uname -s)
LINUX_FRAMEWORK=netcoreapp1.0
DEFAULT_FRAMEWORK=$(LINUX_FRAMEWORK)


all:
	@echo "Usage:   make <dotnet-command>"
	@echo "Example: make build - runs 'dotnet build' for all projects"

.PHONY: test

%:
	for d in $(DIRS) ; do dotnet $@ $$d; done

build:
	# Assuming .NET Core on Linux (net451 will not work).
	@(if [ "$(OS)" = "Linux" ]] ; then \
		for d in $(DIRS) ; do dotnet $@ -f $(LINUX_FRAMEWORK) $$d; done ; \
	else \
		for d in $(DIRS) ; do dotnet $@ $$d; done ; \
	fi)

test:
	dotnet test -f $(LINUX_FRAMEWORK) test/Confluent.Kafka.UnitTests

