#!/bin/bash

./zookeeper/docker-down.sh
./kafka/docker-down.sh
./prometheus/docker-down.sh
./schemaregistry/docker-down.sh

docker volume rm $(docker volume ls -qf dangling=true)
