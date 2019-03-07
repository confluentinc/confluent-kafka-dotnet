#!/bin/bash

OUT=/opt/prometheus.yml /opt/make_settings.sh
/opt/prometheus-2.6.0.linux-amd64/prometheus --config.file=/opt/prometheus.yml --storage.tsdb.path=/data/prometheus --web.listen-address=:9090
