#!/bin/bash

docker volume create kafka1_logs
docker run -d \
    --name kafka_1 \
    --network=host \
    -v kafka1_logs:/data/kafka-logs \
    -e KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" \
    -e BROKER_ID=0 \
    -e METRICS_PORT=7071 \
    -e ADVERTISED_IP=$ADVERTISED_IP \
    -e BROKER_PORT=9092 \
    -e ZK_IP=$ADVERTISED_IP -e ZK_PORT=2181 \
    -e LOG_SEGMENT_BYTES=100000000 \
    -e LOG_RETENTION_CHECK_INTERVAL_MS=60000 \
    confluentinc/dotnet_test_kafka:1


docker volume create kafka2_logs
docker run -d \
    --name kafka_2 \
    --network=host \
    -v kafka2_logs:/data/kafka-logs \
    -e KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" \
    -e BROKER_ID=1 \
    -e METRICS_PORT=7072 \
    -e ADVERTISED_IP=$ADVERTISED_IP \
    -e BROKER_PORT=9093 \
    -e ZK_IP=$ADVERTISED_IP -e ZK_PORT=2181 \
    -e LOG_SEGMENT_BYTES=100000000 \
    -e LOG_RETENTION_CHECK_INTERVAL_MS=60000 \
    confluentinc/dotnet_test_kafka:1


docker volume create kafka3_logs
docker run -d \
    --name kafka_3 \
    --network=host \
    -v kafka3_logs:/data/kafka-logs \
    -e KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" \
    -e BROKER_ID=2 \
    -e METRICS_PORT=7073 \
    -e ADVERTISED_IP=$ADVERTISED_IP \
    -e BROKER_PORT=9094 \
    -e ZK_IP=$ADVERTISED_IP -e ZK_PORT=2181 \
    -e LOG_SEGMENT_BYTES=100000000 \
    -e LOG_RETENTION_CHECK_INTERVAL_MS=60000 \
    confluentinc/dotnet_test_kafka:1
