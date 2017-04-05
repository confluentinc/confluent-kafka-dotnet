# This is a maintainer-centric Makefile to help performing
# typical operations for all projects in this repo.
#
# Example:
#   make build

OS=$(shell uname -s)

EXAMPLE_DIRS=$(shell find ./examples -name '*.csproj' -exec dirname {} \;)
TEST_DIRS==$(shell find ./test -name '*.csproj' -exec dirname {} \;)

LINUX_FRAMEWORK=netcoreapp1.1
DEFAULT_FRAMEWORK?=$(LINUX_FRAMEWORK)

CONFLUENT_VERSION=3.2
CONFLUENT_OSS=confluent-oss-3.2.0-2.11
CONFLUENT_DIR=confluent-3.2.0

all:
	@echo "Usage:   make <dotnet-command>"
	@echo "Example: make build - runs 'dotnet build' for all projects"

.PHONY: unit_test integration_test

build:
	# Assuming .NET Core on Linux (net451 will not work).
	@(if [ "$(OS)" = "Linux" ]] ; then \
		for d in $(EXAMPLE_DIRS) ; do dotnet $@ -f $(LINUX_FRAMEWORK) $$d; done ; \
		for d in $(TEST_DIRS) ; do dotnet $@ -f $(LINUX_FRAMEWORK) $$d; done ; \
	else \
		for d in $(EXAMPLE_DIRS) ; do dotnet $@ -f $(DEFAULT_FRAMEWORK) $$d; done ; \
		for d in $(TEST_DIRS) ; do dotnet $@ -f $(DEFAULT_FRAMEWORK) $$d; done ; \
	fi)

unit_test:
	@(if [ "$(OS)" = "Linux" ]] ; then \
		dotnet test -f $(LINUX_FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj ; \
	else \
		dotnet test -f $(DEFAULT_FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj ; \
	fi)
	
integration_test:
	#install and run zookeeper / kafka if necessary
	@(if [ "$(INSTALL_KAFKA)" ]] ; then \
		echo "Installing kafka" ; \
		wget http://packages.confluent.io/archive/$(CONFLUENT_VERSION)/$(CONFLUENT_OSS).tar.gz ; \
		tar xzf confluent-oss-3.2.0-2.11.tar.gz ; \
		$(CONFLUENT_DIR)/bin/zookeeper-server-start -daemon $(CONFLUENT_DIR)/etc/kafka/zookeeper.properties ; \
		$(CONFLUENT_DIR)/bin/kafka-server-start -daemon $(CONFLUENT_DIR)/etc/kafka/server.properties ; \
		sleep 10 ; \
		sh test/Confluent.Kafka.IntegrationTests/bootstrap-topics.sh $(CONFLUENT_DIR) "localhost:2181" ; \
	fi)
	@(if [ "$(OS)" = "Linux" ]] ; then \
		dotnet test -f $(LINUX_FRAMEWORK) test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj ; \
	else \
		dotnet test -f $(DEFAULT_FRAMEWORK) test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj ; \
	fi)