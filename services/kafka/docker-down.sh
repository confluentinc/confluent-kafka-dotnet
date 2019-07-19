#!/bin/bash

docker kill kafka_1
docker rm kafka_1
docker volume rm -f kafka1_logs

docker kill kafka_2
docker rm kafka_2
docker volume rm -f kafka2_logs

docker kill kafka_3
docker rm kafka_3
docker volume rm -f kafka3_logs
