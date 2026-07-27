#!/bin/bash
#
# Run the confluent-kafka-dotnet unit test suites on a native s390x (IBM Z) host.
#
# Semaphore has no native s390x agent, so this script runs in two modes:
#
#   1. Driver mode (default) - executes on a regular amd64 agent. Reads the
#      s390x host address and SSH key from Vault, copies this script to that
#      host and re-invokes it there over SSH.
#
#   2. Remote mode (--on-s390x) - executes on the s390x host. Clones the repo
#      and runs the test suites inside Red Hat's UBI9 .NET SDK image. Red Hat
#      is the only vendor shipping a .NET SDK for s390x (Microsoft ships none),
#      and the hosts are Ubuntu, so the SDK has to come from a container.
#
# The managed assemblies are architecture independent; what this guards is that
# librdkafka.redist keeps shipping an s390x native library and that the
# serializers stay correct on a big-endian host.
#
# Modelled on packaging/tools/build-release-artifacts-s390x.sh in librdkafka.

set -e

ON_S390X="$1"

if [[ "$ON_S390X" != "--on-s390x" ]]; then
    #
    # Driver mode - on the amd64 Semaphore agent.
    #
    if [ -z "$S390X_USER" ]; then
        echo "S390X_USER not defined"
        exit 1
    fi
    if [ -z "$LOCAL_KEY" ]; then
        echo "LOCAL_KEY not defined"
        exit 1
    fi

    SSH_KEY_PATH="v1/devel/kv/cp-env/s390x-key/IBM-Cloud-S390x-key"
    S390X_HOST=$(vault kv get -field=ip $SSH_KEY_PATH)
    SSH_USER_AT_HOST="$S390X_USER@$S390X_HOST"
    SSH_COMMAND="ssh -o ServerAliveInterval=60 -i ./$LOCAL_KEY $SSH_USER_AT_HOST"
    SCP_COMMAND="scp -i ./$LOCAL_KEY"

    vault kv get -field=private_key $SSH_KEY_PATH > ./$LOCAL_KEY
    chmod go-rwx ./$LOCAL_KEY
    echo "SSH key saved to $LOCAL_KEY"

    if [ -z "$(ssh-keygen -F $S390X_HOST)" ]; then
        vault kv get -field=known_host $SSH_KEY_PATH >> ~/.ssh/known_hosts
        echo "Added $S390X_HOST to the list of known hosts"
    fi

    # Prefer Semaphore's own refs: its checkout can leave a detached HEAD, in which
    # case symbolic-ref finds nothing and describe --exact-match fails on a branch
    # build, leaving the remote with no ref to clone.
    CURRENT_TARGET="${SEMAPHORE_GIT_TAG_NAME:-$SEMAPHORE_GIT_BRANCH}"
    if [ -z "$CURRENT_TARGET" ]; then
        CURRENT_TARGET=$(git symbolic-ref --short -q HEAD || git describe --tags --exact-match)
    fi
    if [ -z "$CURRENT_TARGET" ]; then
        echo "Could not determine the git ref to build"
        exit 1
    fi

    DIR=$(mktemp -d --suffix=ckdotnet)
    eval $SSH_COMMAND mkdir -p $DIR
    eval $SCP_COMMAND ./scripts/run-tests-s390x.sh $SSH_USER_AT_HOST:$DIR/run-tests-s390x.sh

    echo "Running .NET unit tests on s390x host for $CURRENT_TARGET"

    # Don't let set -e abort before the remote work directory is cleaned up.
    set +e
    eval $SSH_COMMAND $DIR/run-tests-s390x.sh --on-s390x $CURRENT_TARGET
    RET=$?
    set -e

    if [[ "$DIR" =~ ^/tmp/.*$ ]]; then
        eval $SSH_COMMAND rm -rf $DIR || echo "Failed to remove remote work directory $DIR"
    fi

    exit $RET
fi

#
# Remote mode - on the s390x host.
#
export DEBIAN_FRONTEND=noninteractive
CURRENT_TARGET=$2
DIR=$(dirname $0)

# These hosts are shared with other pipelines. Only ever touch work directories
# created by this script, and only once they are stale.
find /tmp -maxdepth 1 -name "tmp.*ckdotnet" -mtime +1 -exec rm -rf {} + 2>/dev/null || true

echo "ON s390x: installing prerequisites"
sudo apt update
sudo apt install -y git ca-certificates curl gnupg

if ! command -v docker >/dev/null 2>&1; then
    echo "ON s390x: installing docker"
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc

    sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

    sudo apt update
    sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    sudo systemctl start docker || true
    sudo usermod -aG docker $USER
    echo "User added to docker group"
fi

echo "ON s390x: cloning confluent-kafka-dotnet at $CURRENT_TARGET"
git clone --depth 1 --single-branch --branch $CURRENT_TARGET \
    https://github.com/confluentinc/confluent-kafka-dotnet.git $DIR/confluent-kafka-dotnet
cd $DIR/confluent-kafka-dotnet

# Written into the throwaway clone so the container has a single entrypoint and
# we avoid several layers of shell quoting.
cat > run-unit-tests.sh <<'INNER'
#!/bin/bash
set -e

# .NET strong naming always signs with SHA-1, and RHEL 9's default crypto policy
# refuses SHA-1 signatures, so signing an assembly fails on UBI9 with
# "OpenSslCryptographicException: error:03000098 ... invalid digest".
#
# Re-enable it for this build only, via the RHEL-specific OpenSSL knob. Turning
# signing off instead is not an option: Confluent.Kafka grants friend access with
# InternalsVisibleTo(..., PublicKey=...), so unsigned test assemblies fail to
# compile with CS0281.
sed 's/^\[ evp_properties \]/[ evp_properties ]\nrh-allow-sha1-signatures = yes/' \
    /etc/pki/tls/openssl.cnf > /tmp/openssl-allow-sha1.cnf
export OPENSSL_CONF=/tmp/openssl-allow-sha1.cnf

# UseAppHost:      Microsoft publishes no Microsoft.NETCore.App.Host.linux-s390x.
# TargetFramework: single TFM, as the OSX x64 block does. Must match between
#                  restore and test or the global properties disagree.
PROPS="-p:TargetFramework=net10.0 -p:UseAppHost=false"

echo "--- dotnet --info ---"
dotnet --info | head -20

dotnet restore $PROPS

for p in Confluent.Kafka.UnitTests \
         Confluent.SchemaRegistry.UnitTests \
         Confluent.SchemaRegistry.Serdes.UnitTests \
         Confluent.Kafka.OAuthBearer.Aws.UnitTests; do
    echo "--- dotnet test $p ---"
    dotnet test -f net10.0 $PROPS -l "console;verbosity=normal" "test/$p/$p.csproj"
done
INNER
chmod +x run-unit-tests.sh

# No --platform flag: the host is natively s390x, so this pulls the s390x image.
# -u 0 because the UBI9 image runs as uid 1001, which cannot write bin/ and obj/
# into the bind mount.
newgrp docker <<'EOF'
docker run --rm -u 0 \
    -e DOTNET_CLI_TELEMETRY_OPTOUT=true \
    -v "$PWD:/work" -w /work \
    registry.access.redhat.com/ubi9/dotnet-100 \
    ./run-unit-tests.sh
EOF
