# This is a maintainer-centric Makefile to help performing
# typical operations for all projects in this repo.
#
# Example:
#   make build

OS=$(shell uname -s)
LINUX_FRAMEWORK=netcoreapp1.1
DEFAULT_FRAMEWORK=$(LINUX_FRAMEWORK)

all:
	@echo "Usage:   make <dotnet-command>"
	@echo "Example: make build - runs 'dotnet build' for all projects"

.PHONY: test

build:
	@echo "build skipped - dotnet restore apparently not working"
	# Assuming .NET Core on Linux (net451 will not work).
	#@(if [ "$(OS)" = "Linux" ]] ; then \
	#	dotnet $@ -f $(LINUX_FRAMEWORK) ; \
	#else \
	#	dotnet $@ -f $(DEFAULT_FRAMEWORK) ; \
	#fi)

test:
	@(if [ "$(OS)" = "Linux" ]] ; then \
		dotnet test -f $(LINUX_FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj ; \
	else \
		dotnet test -f $(DEFAULT_FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj ; \
	fi)
