#!/bin/bash

set -e

if [ "$#" -ne 2 ]; then
    echo "usage: bootstrap-topics.sh <confluent platform path> <zookeeper>"
    exit 1
fi

$1/bin/kafka-topics --zookeeper $2 --create --topic test-topic-1 --partitions 1 --replication 1
$1/bin/kafka-topics --zookeeper $2 --create --topic test-topic-2 --partitions 2 --replication 1
