# This is a maintainer-centric Makefile to help performing
# typical operations for all projects in this repo.
#
# Example:
#   make build

OS=$(shell uname -s)

EXAMPLE_DIRS=$(shell find ./examples -name '*.csproj' -exec dirname {} \;)
TEST_DIRS==$(shell find ./test -name '*.csproj' -exec dirname {} \;)

FRAMEWORK?=netcoreapp1.1

#Used when INSTALL_KAFKA is set
CONFLUENT_VERSION?=3.2
CONFLUENT_OSS?=confluent-oss-3.2.0-2.11
CONFLUENT_DIR?=confluent-3.2.0

all:
	@echo "Usage:   make <dotnet-command>"
	@echo "Example: make build - runs 'dotnet build' for all projects"

.PHONY: unit_test integration_test

build:
	@for d in $(EXAMPLE_DIRS) ; do dotnet $@ -f $(FRAMEWORK) $$d; done
	@for d in $(TEST_DIRS) ; do dotnet $@ -f $(FRAMEWORK) $$d; done

unit_test:
	@dotnet test -f $(FRAMEWORK) test/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj
	
integration_test:
	#install and run zookeeper / kafka if necessary
	@(if [ "$(INSTALL_KAFKA)" ] ; then \
		echo "Installing kafka" ; \
		wget http://packages.confluent.io/archive/$(CONFLUENT_VERSION)/$(CONFLUENT_OSS).tar.gz ; \
		tar xzf $(CONFLUENT_OSS).tar.gz ; \
		$(CONFLUENT_DIR)/bin/zookeeper-server-start -daemon $(CONFLUENT_DIR)/etc/kafka/zookeeper.properties ; \
		$(CONFLUENT_DIR)/bin/kafka-server-start -daemon $(CONFLUENT_DIR)/etc/kafka/server.properties ; \
		#arbitrary wait for kafka to start
		sleep 10 ; \
		sh test/Confluent.Kafka.IntegrationTests/bootstrap-topics.sh $(CONFLUENT_DIR) "localhost:2181" ; \
	fi)
	@dotnet test -f $(FRAMEWORK) test/Confluent.Kafka.IntegrationTests/Confluent.Kafka.IntegrationTests.csproj