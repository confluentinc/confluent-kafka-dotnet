#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "usage: .. <advertised_ip>"
    exit 1
fi

export ADVERTISED_IP=$1
./zookeeper/docker-up.sh
./kafka/docker-up.sh
./schemaregistry/docker-up.sh
./prometheus/docker-up.sh
