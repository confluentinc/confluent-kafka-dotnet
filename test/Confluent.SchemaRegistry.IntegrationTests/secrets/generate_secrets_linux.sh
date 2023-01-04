#!/usr/bin/env bash

# Clone the script to generate the stores, and run the script with the correct params.
git clone https://github.com/confluentinc/confluent-platform-security-tools.git --depth=1

export COUNTRY="US"
export STATE="CA"
export ORGANIZATION_UNIT="Confluent Inc"
export CITY="Mountain View"
export PASSWORD="cnf123"

bash confluent-platform-security-tools/kafka-generate-ssl-automatic.sh

# Copy the files into the right place.
cp keystore/kafka.keystore.jks schema-registry.keystore.jks
cp truststore/kafka.truststore.jks schema-registry.truststore.jks
cp cert-signed schema-registry-ca.cer

# Duplicate to docker secrets.
# Docker containers will need to be restarted after this step.
cp schema-registry.keystore.jks  ../../docker/secrets/schema-registry.keystore.jks
cp schema-registry.truststore.jks  ../../docker/secrets/schema-registry.truststore.jks
cp schema-registry-ca.cer ../../docker/secrets/schema-registry-ca.cer

# Cleanup the script, and the files which are not required.
rm -rf confluent-platform-security-tools
rm -rf truststore
rm -rf keystore
rm cert-file cert-signed
