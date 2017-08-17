# This is a maintainer-centric Makefile to help performing
# typical operations for all projects in this repo.
#
# Example:
#   make build

OS=$(shell uname -s)

EXAMPLE_DIRS=$(shell find ./examples -name '*.csproj' -exec dirname {} \;)
TEST_DIRS==$(shell find ./test -name '*.csproj' -exec dirname {} \;)

#framework can be change to net45 or similar
FRAMEWORK?=netcoreapp1.1

#Used for url
# http://packages.confluent.io/archive/$(CONFLUENT_VERSION)/$(CONFLUENT_OSS).tar.gz
CONFLUENT_VERSION?=3.2
CONFLUENT_OSS?=confluent-oss-3.2.0-2.11
#the directory inside the tar.gz wher confluent bin/config are located
CONFLUENT_DIR?=confluent-3.2.0

all:
	@echo "Usage:   make <command> (build, unit_test, integration_test, install_kafka)"
	@echo "Example: make build - runs 'dotnet build' for all projects"

.PHONY: unit_test integration_test

build:
	# will also build Confluent.Kafka fot netstandard1.3
	# we can't make a build on the solution as net45 will not work
	# and we can't build it specifying netcoreapp as netstandard is used also
	@for d in $(EXAMPLE_DIRS) ; do dotnet $@ -f $(FRAMEWORK) $$d; done
	@for d in $(TEST_DIRS) ; do dotnet $@ -f $(FRAMEWORK) $$d; done

unit_test:
	@dotnet test -f $(FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj

integration_test:
	@dotnet test -f $(FRAMEWORK) test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj

install_kafka:
	@echo "Installing kafka"
	@wget http://packages.confluent.io/archive/$(CONFLUENT_VERSION)/$(CONFLUENT_OSS).tar.gz
	@tar xzf $(CONFLUENT_OSS).tar.gz
	@echo "starting zookeeper"
	@$(CONFLUENT_DIR)/bin/zookeeper-server-start -daemon $(CONFLUENT_DIR)/etc/kafka/zookeeper.properties
	@echo "starting kafka"
	@$(CONFLUENT_DIR)/bin/kafka-server-start -daemon $(CONFLUENT_DIR)/etc/kafka/server.properties
	@echo "wait arbitrary 10s for kafka to launch"
	@sleep 10
	@echo "create topics necessary for the tests"
	@sh test/Confluent.Kafka.IntegrationTests/bootstrap-topics.sh $(CONFLUENT_DIR) "localhost:2181"