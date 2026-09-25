#!/bin/bash
#
# Run the Kafka integration tests ("classic" group protocol) and the Schema
# Registry serdes integration tests on the native s390x (IBM Z) Semaphore agent,
# against a real broker and Schema Registry.
#
# test/docker/*.yaml can't be used here: cp-zookeeper, cp-kafka, apache/kafka and
# cp-schema-registry 7.x publish no s390x images. cp-server and
# cp-schema-registry 8.2+ do, so the broker is cp-server (KRaft) running the
# repo's own KRaft configuration (test/docker/kraft), which provides the same
# PLAINTEXT (9092) and SASL_PLAINTEXT (9093) listeners that testconf.json uses.

set -e

KAFKA_IMAGE=confluentinc/cp-server:8.3.0
SCHEMA_REGISTRY_IMAGE=confluentinc/cp-schema-registry:8.3.0
DOTNET_IMAGE=registry.access.redhat.com/ubi9/dotnet-100

cleanup() {
    docker rm -f s390x-it-tests s390x-it-schema-registry s390x-it-kafka >/dev/null 2>&1 || true
    rm -f kraft-s390x.properties run-integration-tests.sh
    # The test container runs as root, so bin/ and obj/ come back root owned.
    # Hand the checkout back to the agent user so the shared agent stays clean.
    sudo chown -R "$(id -u):$(id -g)" . || true
}
trap cleanup EXIT

# Overrides on top of the KRaft config (later entries win):
# - num.partitions=1: the amd64 "classic" and serdes jobs run against
#   test/docker/docker-compose.yaml, whose broker keeps the default of 1. Several
#   serdes tests produce to auto-created topics and then read partition 0 only.
# - cp-server keeps its license, command and metadata state in topics that
#   default to replication factor 3. A single broker can't create them, and
#   cp-server shuts itself down after a few minutes when the license topic is
#   missing.
cp test/docker/kraft/server.properties kraft-s390x.properties
# server.properties doesn't end with a newline, so add one before appending.
echo >> kraft-s390x.properties
cat >> kraft-s390x.properties <<'EOF'
num.partitions=1
confluent.license.topic.replication.factor=1
confluent.command.topic.replication.factor=1
confluent.metadata.topic.replication.factor=1
confluent.balancer.enable=false
confluent.telemetry.enabled=false
EOF

# --hostname kafka: the KRaft config names the controller and in-container
# listeners "kafka". Host ports 9092/9093 map to the DOCKER listeners, which
# advertise localhost:9092 / localhost:9093.
docker run -d --name s390x-it-kafka --hostname kafka \
    -p 9092:29092 -p 9093:29093 \
    -v "$PWD/kraft-s390x.properties:/cfg/server.properties:ro" \
    -v "$PWD/test/docker/kafka_server_jaas.conf:/cfg/jaas.conf:ro" \
    -e KAFKA_OPTS=-Djava.security.auth.login.config=/cfg/jaas.conf \
    --entrypoint /bin/bash "$KAFKA_IMAGE" -c '
        set -e
        kafka-storage format -t "$(kafka-storage random-uuid)" -c /cfg/server.properties
        exec kafka-server-start /cfg/server.properties'

echo "Waiting for the broker"
for i in $(seq 1 60); do
    if docker exec s390x-it-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo "Broker is up"
        break
    fi
    if [[ $i == 60 ]]; then
        docker logs --tail 50 s390x-it-kafka
        echo "Broker did not start"
        exit 1
    fi
    sleep 5
done

docker run -d --name s390x-it-schema-registry --network host \
    -e SCHEMA_REGISTRY_HOST_NAME=localhost \
    -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
    -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://localhost:9092 \
    "$SCHEMA_REGISTRY_IMAGE"

echo "Waiting for Schema Registry"
for i in $(seq 1 60); do
    if curl -sf http://localhost:8081/subjects >/dev/null; then
        echo "Schema Registry is up"
        break
    fi
    if [[ $i == 60 ]]; then
        docker logs --tail 50 s390x-it-schema-registry
        echo "Schema Registry did not start"
        exit 1
    fi
    sleep 5
done

# The container entrypoint is written into the checkout so the container has a
# single entrypoint and we avoid several layers of shell quoting.
cat > run-integration-tests.sh <<'INNER'
#!/bin/bash
set -e

# Strong naming signs with SHA-1, which RHEL 9's crypto policy refuses; allow it
# for the build (see scripts/run-tests-s390x.sh).
sed 's/^\[ evp_properties \]/[ evp_properties ]\nrh-allow-sha1-signatures = yes/' \
    /etc/pki/tls/openssl.cnf > /tmp/openssl-allow-sha1.cnf

PROJECTS="Confluent.Kafka.IntegrationTests Confluent.SchemaRegistry.Serdes.IntegrationTests"
for p in $PROJECTS; do
    echo "--- build $p ---"
    OPENSSL_CONF=/tmp/openssl-allow-sha1.cnf dotnet build "test/$p/$p.csproj" -f net10.0
done

# Same as the amd64 integration jobs.
export SEMAPHORE_SKIP_FLAKY_TESTS=true

# Run the xunit.v3 test executables directly (xunit's native runner), from the
# project directory so testconf.json is found:
# - Producer_Produce_SyncOverAsync and the serdes SyncOverAsync test deliberately
#   block all but one thread-pool worker to probe the sync-over-async deadlock
#   boundary. On the small s390x agent that leaves no headroom and they deadlock,
#   so they're excluded; they don't exercise anything architecture specific.
# - -longRunning names any test that runs for more than two minutes.
# - -result-xml keeps a per-test record of what passed, failed or was skipped.
RET=0
for p in $PROJECTS; do
    echo "--- test $p ---"
    (cd "test/$p" && "./bin/Debug/net10.0/$p" -noLogo -method- '*SyncOverAsync' \
        -longRunning 120 -result-xml "/work/test-results-$p.xml") || RET=1
done
exit $RET
INNER
chmod +x run-integration-tests.sh

# --network host so the tests reach the broker and Schema Registry on localhost.
# A hard timeout so a hung test fails the job instead of holding the agent.
set +e
timeout --kill-after=60 5400 docker run --name s390x-it-tests -u 0 --network host \
    -e DOTNET_CLI_TELEMETRY_OPTOUT=true \
    -v "$PWD:/work" -w /work \
    "$DOTNET_IMAGE" \
    ./run-integration-tests.sh
RET=$?
set -e

exit $RET
