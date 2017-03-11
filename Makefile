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
	# Assuming .NET Core on Linux (net451 will not work).
	dotnet restore src/Confluent.Kafka/Confluent.Kafka.csproj
	@(if [ "$(OS)" = "Linux" ]] ; then \
		dotnet $@ -f $(LINUX_FRAMEWORK) src/Confluent.Kafka/Confluent.Kafka.csproj ; \
	else \
		dotnet $@ -f $(DEFAULT_FRAMEWORK) src/Confluent.Kafka/Confluent.Kafka.csproj ; \
	fi)

test:
	dotnet restore test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj
	@(if [ "$(OS)" = "Linux" ]] ; then \
		dotnet test -f $(LINUX_FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj ; \
	else \
		dotnet test -f $(DEFAULT_FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj ; \
	fi)
