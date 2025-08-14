# This is a maintainer-centric Makefile to help performing
# typical operations for all projects in this repo.
#
# Example:
#   make build

OS=$(shell uname -s)

EXAMPLE_DIRS=$(shell find ./examples -name '*.csproj' -exec dirname {} \;)
TEST_DIRS=$(shell find ./test -name '*.csproj'   \;)
UNIT_TEST_DIRS=$(shell find . -type d -regex '.*UnitTests$$' -exec basename {} \;)

# We want to run tests by default with latest version of .NET
DEFAULT_TEST_FRAMEWORK?=net8.0

export PATH

all:
	@echo "Usage:   make <dotnet-command>"
	@echo "Example: make build - runs 'dotnet build' for all projects"

.PHONY: test

build:
	for d in $(EXAMPLE_DIRS) ; do dotnet $@ $$d; done ; \
	for d in $(TEST_DIRS) ; do dotnet $@ $$d; done ;

test:
	@(for d in $(UNIT_TEST_DIRS) ; do \
		dotnet test test/$$d/$$d.csproj ; \
	done)

test-coverage:
	@(for d in $(UNIT_TEST_DIRS) ; do \
		dotnet-coverage collect "dotnet test -f $(DEFAULT_TEST_FRAMEWORK) test/$$d/$$d.csproj" \
			-f xml -o test/$$d/coverage.xml ; \
	done)

test-latest:
	@(for d in $(UNIT_TEST_DIRS) ; do \
		dotnet test -f $(DEFAULT_TEST_FRAMEWORK) test/$$d/$$d.csproj ; \
	done)
