#!/bin/bash

docker run -d \
    --name schema_registry \
    --network host \
    -e SCHEMA_REGISTRY_HOST_NAME="schema-registry" \
    -e SCHEMA_REGISTRY_LISTENERS="http://0.0.0.0:8081" \
    -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS="PLAINTEXT://$ADVERTISED_IP:9092" \
    confluentinc/cp-schema-registry \
    bash -c "sleep 30; /etc/confluent/docker/run"

docker volume create schema_registry_auth
docker run -d \
    --name schema_registry_auth \
    --network=host \
    -v schema_registry_auth:/conf/schema-registry \
    -e SCHEMA_REGISTRY_HOST_NAME="schema-registry-ssl" \
    -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS="PLAINTEXT://$ADVERTISED_IP:9092" \
    -e SCHEMA_REGISTRY_LISTENERS="http://0.0.0.0:8082" \
    -e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL="$ADVERTISED_IP:2181" \
    -e SCHEMA_REGISTRY_AUTHENTICATION_METHOD=BASIC \
    -e SCHEMA_REGISTRY_AUTHENTICATION_REALM=SchemaRegistry \
    -e SCHEMA_REGISTRY_AUTHENTICATION_ROLES=Testers \
    -e SCHEMA_REGISTRY_OPTS="-Djava.security.auth.login.config=/conf/schema-registry/schema-registry.jaas" \
    confluentinc/cp-schema-registry \
    bash -c "sleep 30; /etc/confluent/docker/run"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
docker cp $SCRIPT_DIR/conf/schema-registry/login.properties schema_registry_auth:/conf/schema-registry/
docker cp $SCRIPT_DIR/conf/schema-registry/schema-registry.jaas schema_registry_auth:/conf/schema-registry/
