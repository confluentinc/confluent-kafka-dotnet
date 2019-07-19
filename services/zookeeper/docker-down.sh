#!/bin/bash

docker kill zookeeper_1
docker rm zookeeper_1
docker volume rm -f zk1_data
