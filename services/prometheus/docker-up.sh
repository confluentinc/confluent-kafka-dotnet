#!/bin/bash

docker volume create prometheus

docker run -d \
    --name=prometheus \
    --network=host \
    -v prometheus:/data/prometheus \
    -e SCRAPE_INTERVAL="30s" -e EVAL_INTERVAL="30s" \
    -e ZK_IP=$ADVERTISED_IP -e ZK_METRICS_PORT=7062 \
    -e BROKER_1_IP=$ADVERTISED_IP -e BROKER_1_METRICS_PORT=7071 \
    -e BROKER_2_IP=$ADVERTISED_IP -e BROKER_2_METRICS_PORT=7072 \
    -e BROKER_3_IP=$ADVERTISED_IP -e BROKER_3_METRICS_PORT=7073 \
    confluentinc/dotnet_test_prometheus:1 bash -c "sleep 30; /opt/bootstrap.sh"
