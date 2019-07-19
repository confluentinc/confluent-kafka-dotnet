#!/bin/bash

docker kill prometheus
docker rm prometheus
docker volume rm prometheus
