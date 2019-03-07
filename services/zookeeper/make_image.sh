#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
docker build -t confluentinc/dotnet_test_zookeeper:1 $SCRIPT_DIR
