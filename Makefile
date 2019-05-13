# This is a maintainer-centric Makefile to help performing
# typical operations for all projects in this repo.
#
# Example:
#   make build

OS=$(shell uname -s)

EXAMPLE_DIRS=$(shell find ./examples -name '*.csproj' -exec dirname {} \;)
TEST_DIRS==$(shell find ./test -name '*.csproj' -exec dirname {} \;)

LINUX_FRAMEWORK=netcoreapp2.1
DEFAULT_FRAMEWORK?=$(LINUX_FRAMEWORK)

all:
	@echo "Usage:   make <dotnet-command>"
	@echo "Example: make build - runs 'dotnet build' for all projects"

.PHONY: test

build:
	# Assuming .NET Core on Linux (net451 will not work).
	@(if [[ "$(OS)" == "Linux" ]] ; then \
		for d in $(EXAMPLE_DIRS) ; do dotnet $@ -f $(LINUX_FRAMEWORK) $$d; done ; \
		for d in $(TEST_DIRS) ; do dotnet $@ -f $(LINUX_FRAMEWORK) $$d; done ; \
	else \
		for d in $(EXAMPLE_DIRS) ; do dotnet $@ -f $(DEFAULT_FRAMEWORK) $$d; done ; \
		for d in $(TEST_DIRS) ; do dotnet $@ -f $(DEFAULT_FRAMEWORK) $$d; done ; \
	fi)

test:
	@(if [[ "$(OS)" == "Linux" ]] ; then \
		dotnet test -f $(LINUX_FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj ; \
	else \
		dotnet test -f $(DEFAULT_FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj ; \
	fi)
