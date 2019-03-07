#!/bin/bash

OUT=/opt/server.properties /opt/make_settings.sh
LOG_DIR=/data/kafka-varlog KAFKA_OPTS="$KAFKA_OPTS -javaagent:/opt/jmx_prometheus_javaagent-0.9.jar=$METRICS_PORT:/opt/jmx_kafka-2.0.yml" /opt/kafka_2.11-2.1.1/bin/kafka-server-start.sh /opt/server.properties
