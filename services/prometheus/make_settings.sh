#!/bin/bash

cat <<EOF >$OUT
global:
 scrape_interval: $SCRAPE_INTERVAL
 evaluation_interval: $EVAL_INTERVAL
scrape_configs:
 - job_name: 'kafka'
   static_configs:
    - targets:
      - $BROKER_1_IP:$BROKER_1_METRICS_PORT
      - $BROKER_2_IP:$BROKER_2_METRICS_PORT
      - $BROKER_3_IP:$BROKER_3_METRICS_PORT
 - job_name: 'zookeeper'
   static_configs:
    - targets:
      - $ZK_IP:$ZK_METRICS_PORT
EOF
