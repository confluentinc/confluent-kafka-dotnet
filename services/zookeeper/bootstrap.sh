#!/bin/bash

OUT=/opt/zookeeper.properties /opt/make_settings.sh
LOG_DIR=/data/zookeeper-varlog KAFKA_OPTS="$KAFKA_OPTS -javaagent:/opt/jmx_prometheus_javaagent-0.9.jar=$METRICS_PORT:/opt/jmx_zookeeper.yml" /opt/kafka_2.11-2.1.1/bin/zookeeper-server-start.sh /opt/zookeeper.properties
