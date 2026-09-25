#!/bin/bash
#
# Run the integration tests on the native s390x (IBM Z) Semaphore agent, against
# a Kafka broker (and, for the Schema Registry tests, Schema Registry) running
# locally on the agent.
#
# Usage: scripts/run-integration-tests-s390x.sh classic|consumer|schema-registry
#
#   classic          Confluent.Kafka.IntegrationTests, "classic" group protocol
#   consumer         Confluent.Kafka.IntegrationTests, "consumer" group protocol
#   schema-registry  Confluent.SchemaRegistry.Serdes.IntegrationTests and
#                    Confluent.SchemaRegistry.IntegrationTests
#
# These mirror the amd64 integration jobs. Those start the broker and Schema
# Registry from the images in test/docker, none of which is published for s390x.
# Kafka and Schema Registry are plain Java, so here they run directly on the agent
# from the Confluent Platform community release, on a Java runtime downloaded for
# the job. Each job uses the release that matches its amd64 counterpart's images:
# 7.9.0 (cp-kafka and cp-schema-registry 7.9.0) for "classic" and
# "schema-registry", and 8.0.0 (Kafka 4.0, as apache/kafka:4.0.0) for "consumer".
# The broker runs the repo's KRaft configuration (test/docker/kraft).

set -e

MODE=$1
case "$MODE" in
    classic)
        PROJECTS="Confluent.Kafka.IntegrationTests"
        CP_VERSION=7.9.0
        ;;
    consumer)
        PROJECTS="Confluent.Kafka.IntegrationTests"
        CP_VERSION=8.0.0
        ;;
    schema-registry)
        PROJECTS="Confluent.SchemaRegistry.Serdes.IntegrationTests Confluent.SchemaRegistry.IntegrationTests"
        CP_VERSION=7.9.0
        ;;
    *)
        echo "Usage: $0 classic|consumer|schema-registry" >&2
        exit 1
        ;;
esac

case "$CP_VERSION" in
    7.9.0) CP_SHA256=bcc936b2136f859ec9f99eb5fca214ff5ee936895e2e91ef9da6684cbb2cfb0a ;;
    8.0.0) CP_SHA256=2cbea83bc0a03ca8dffa28e45b30c15748a2d0f65a03c1dd3fe3cf51bc19856b ;;
esac
CP_URL=https://packages.confluent.io/archive/${CP_VERSION%.*}/confluent-community-$CP_VERSION.tar.gz
JRE_URL=https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20.1%2B1/OpenJDK17U-jre_s390x_linux_hotspot_17.0.20.1_1.tar.gz
JRE_SHA256=162ca8775d96b8c9d71e672fb498ddc61bd2cd62c8ce3f7d05e2e228ca53f2f4
DOTNET_IMAGE=registry.access.redhat.com/ubi9/dotnet-100
TEST_CONTAINER=s390x-it-tests-$MODE

WORK=$(mktemp -d)
PIDS=()
declare -A SCHEMA_REGISTRY_PID

cleanup() {
    local rc=$?
    docker rm -f "$TEST_CONTAINER" >/dev/null 2>&1 || true
    if [[ $rc != 0 ]]; then
        for log in "$WORK"/*.log; do
            [[ -f $log ]] || continue
            echo "--- last 50 lines of $(basename "$log") ---"
            tail -n 50 "$log"
        done
    fi
    # Stop Schema Registry before the broker it stores its schemas in.
    local i
    for (( i=${#PIDS[@]}-1; i>=0; i-- )); do
        kill "${PIDS[i]}" 2>/dev/null || true
        for _ in $(seq 1 30); do
            kill -0 "${PIDS[i]}" 2>/dev/null || break
            sleep 1
        done
        kill -9 "${PIDS[i]}" 2>/dev/null || true
    done
    rm -rf "$WORK" run-integration-tests.sh
    # The test container runs as root, so bin/ and obj/ come back root owned.
    # Hand the checkout back to the agent user so the shared agent stays clean.
    sudo chown -R "$(id -u):$(id -g)" . || true
    exit $rc
}
trap cleanup EXIT

# Fail fast, rather than halfway through the tests, if another job on this agent
# already holds one of the ports that testconf.json and the Schema Registry tests use.
for port in 9092 9093 38705 8081 8082 8083; do
    if [[ -n $(ss -Htln "sport = :$port") ]]; then
        echo "Port $port is already in use on this agent"
        exit 1
    fi
done

download() {
    curl --proto '=https' -fsSL --retry 3 -o "$WORK/download.tar.gz" "$1"
    echo "$2  $WORK/download.tar.gz" | sha256sum -c -
    mkdir -p "$3"
    tar -xzf "$WORK/download.tar.gz" -C "$3" --strip-components=1
    rm "$WORK/download.tar.gz"
}

echo "Downloading a Java runtime and Confluent Platform $CP_VERSION"
download "$JRE_URL" "$JRE_SHA256" "$WORK/jre"
download "$CP_URL" "$CP_SHA256" "$WORK/confluent"
export JAVA_HOME=$WORK/jre
CP=$WORK/confluent
"$JAVA_HOME/bin/java" -version

# Overrides on top of the KRaft config (later entries win):
# - The config is written for a container named "kafka" whose DOCKER listeners are
#   mapped to localhost:9092/9093. On the agent, listen on and advertise those
#   ports directly. Listen on all addresses, as the Docker port mappings do:
#   localhost resolves to ::1 first, and a broker listening on 127.0.0.1 only
#   makes every client log a refused connection before it falls back to IPv4.
# - log.dirs: keep the data in this job's directory.
# - num.partitions=1 for "classic" and "schema-registry": the amd64 jobs run those
#   against test/docker/docker-compose.yaml, whose broker keeps the default of 1,
#   and several serdes tests produce to auto-created topics and then read
#   partition 0 only. "consumer" keeps the KRaft config's 4, as on amd64.
cp test/docker/kraft/server.properties "$WORK/server.properties"
# server.properties doesn't end with a newline, so add one before appending.
echo >> "$WORK/server.properties"
cat >> "$WORK/server.properties" <<EOF
listeners=PLAINTEXT://:9092,SASL_PLAINTEXT://:9093,CONTROLLER://localhost:38705
advertised.listeners=PLAINTEXT://localhost:9092,SASL_PLAINTEXT://localhost:9093
controller.quorum.voters=0@localhost:38705
log.dirs=$WORK/kafka-data
EOF
if [[ $MODE != consumer ]]; then
    echo "num.partitions=1" >> "$WORK/server.properties"
fi

"$CP/bin/kafka-storage" format -t "$("$CP/bin/kafka-storage" random-uuid)" -c "$WORK/server.properties"
KAFKA_OPTS="-Djava.security.auth.login.config=$PWD/test/docker/kafka_server_jaas.conf" \
    LOG_DIR=$WORK/kafka-logs \
    nohup "$CP/bin/kafka-server-start" "$WORK/server.properties" > "$WORK/kafka.log" 2>&1 &
PIDS+=($!)

echo "Waiting for the broker"
for i in $(seq 1 60); do
    if "$CP/bin/kafka-topics" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo "Broker is up"
        break
    fi
    if [[ $i == 60 ]] || ! kill -0 "${PIDS[0]}" 2>/dev/null; then
        echo "Broker did not start"
        exit 1
    fi
    sleep 5
done

# Schema Registry: the three servers the amd64 job starts from
# test/docker/docker-compose.yaml, on the same ports, each keeping its schemas in
# its own topic of the one broker:
# - 8081: no authentication (both test projects)
# - 8082: HTTP basic authentication (Confluent.SchemaRegistry.IntegrationTests)
# - 8083: HTTPS with client certificates (Confluent.SchemaRegistry.IntegrationTests)
start_schema_registry() {
    local name=$1 opts=$2
    shift 2
    {
        echo "host.name=localhost"
        echo "kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092"
        echo "kafkastore.topic=_schemas_$name"
        echo "schema.registry.group.id=schema-registry-$name"
        printf '%s\n' "$@"
    } > "$WORK/schema-registry-$name.properties"
    SCHEMA_REGISTRY_OPTS=$opts LOG_DIR=$WORK/schema-registry-$name-logs \
        nohup "$CP/bin/schema-registry-start" "$WORK/schema-registry-$name.properties" \
        > "$WORK/schema-registry-$name.log" 2>&1 &
    PIDS+=($!)
    SCHEMA_REGISTRY_PID[$name]=$!
}

if [[ $MODE == schema-registry ]]; then
    # The repo's JAAS file points at the path the compose file mounts it on.
    sed "s|/conf/|$PWD/test/docker/conf/|" test/docker/conf/schema-registry/schema-registry.jaas \
        > "$WORK/schema-registry.jaas"
    SECRETS=$PWD/test/docker/secrets

    start_schema_registry plain "" \
        "listeners=http://localhost:8081"
    start_schema_registry auth "-Djava.security.auth.login.config=$WORK/schema-registry.jaas" \
        "listeners=http://localhost:8082" \
        "authentication.method=BASIC" \
        "authentication.realm=SchemaRegistry" \
        "authentication.roles=Testers"
    start_schema_registry ssl "" \
        "listeners=https://localhost:8083" \
        "inter.instance.protocol=https" \
        "ssl.keystore.location=$SECRETS/schema-registry.keystore.jks" \
        "ssl.keystore.password=cnf123" \
        "ssl.key.password=cnf123" \
        "ssl.truststore.location=$SECRETS/schema-registry.truststore.jks" \
        "ssl.truststore.password=cnf123" \
        "ssl.client.auth=true"

    echo "Waiting for Schema Registry"
    for name in plain auth ssl; do
        for i in $(seq 1 60); do
            if grep -q "Server started, listening for requests" "$WORK/schema-registry-$name.log"; then
                echo "Schema Registry ($name) is up"
                break
            fi
            if [[ $i == 60 ]] || ! kill -0 "${SCHEMA_REGISTRY_PID[$name]}" 2>/dev/null; then
                echo "Schema Registry ($name) did not start"
                exit 1
            fi
            sleep 5
        done
    done
fi

# The container entrypoint is written into the checkout so the container has a
# single entrypoint and we avoid several layers of shell quoting.
cat > run-integration-tests.sh <<'INNER'
#!/bin/bash
set -e

# "which" is not installed in the UBI9 image; command -v is the shell built-in equivalent.
command -v dotnet
dotnet --version
dotnet --list-sdks

# Strong naming signs with SHA-1, which RHEL 9's crypto policy refuses; allow it
# for the build (see scripts/run-tests-s390x.sh).
sed 's/^\[ evp_properties \]/[ evp_properties ]\nrh-allow-sha1-signatures = yes/' \
    /etc/pki/tls/openssl.cnf > /tmp/openssl-allow-sha1.cnf

for p in $PROJECTS; do
    echo "--- build $p ---"
    OPENSSL_CONF=/tmp/openssl-allow-sha1.cnf dotnet build "test/$p/$p.csproj" -f net10.0 -c "$CONFIGURATION"
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
# - -noColor: on Linux the runner colours every line, which the CI log shows as
#   white text.
RET=0
for p in $PROJECTS; do
    echo "--- test $p ---"
    (cd "test/$p" && "./bin/$CONFIGURATION/net10.0/$p" -noLogo -noColor -method- '*SyncOverAsync' \
        -longRunning 120 -result-xml "/work/test-results-$p.xml") || RET=1
done
exit $RET
INNER
chmod +x run-integration-tests.sh

docker pull -q "$DOTNET_IMAGE"
docker image inspect --format '{{index .RepoDigests 0}}' "$DOTNET_IMAGE"

# --network host so the tests reach the broker and Schema Registry on localhost.
# A hard timeout so a hung test fails the job instead of holding the agent.
TEST_ENV=(-e DOTNET_CLI_TELEMETRY_OPTOUT=true -e "CONFIGURATION=${CONFIGURATION:-Release}" -e "PROJECTS=$PROJECTS")
if [[ $MODE == consumer ]]; then
    TEST_ENV+=(-e TEST_CONSUMER_GROUP_PROTOCOL=consumer)
fi
set +e
timeout --kill-after=60 5400 docker run --name "$TEST_CONTAINER" -u 0 --network host \
    "${TEST_ENV[@]}" \
    -v "$PWD:/work" -w /work \
    "$DOTNET_IMAGE" \
    ./run-integration-tests.sh
RET=$?
set -e

exit $RET
