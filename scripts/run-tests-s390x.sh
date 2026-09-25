#!/bin/bash
#
# Run the confluent-kafka-dotnet unit test suites on the native s390x (IBM Z)
# Semaphore agent, inside Red Hat's UBI9 .NET SDK image. Red Hat is the only
# vendor shipping a .NET SDK for s390x (Microsoft ships none), and the agent is
# Ubuntu, so the SDK has to come from a container.
#
# The managed assemblies are architecture independent; what this guards is that
# librdkafka.redist keeps shipping an s390x native library and that the
# serializers stay correct on a big-endian host.

set -e

# The container entrypoint is written into the checkout so the container has a
# single entrypoint and we avoid several layers of shell quoting.
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

# "which" is not installed in the UBI9 image; command -v is the shell built-in equivalent.
command -v dotnet
dotnet --version
dotnet --list-sdks

# Build and test each unit project on its own rather than restoring the whole
# solution. A solution-wide restore drags in the Exe helper projects (Benchmark,
# VerifiableClient, SyncOverAsync, Transactions, ConfigGen), which resolve the
# portable linux-s390x app host pack that Microsoft does not publish (only Red
# Hat's rhel.9-s390x exists), so restore fails with NU1101. The four unit projects
# don't reference any of them, so building them individually avoids it.
#
# The test projects multi-target net8.0;net10.0 (test/Directory.Build.props); select
# net10.0 with -f, as the main CI does. Don't override it with -p:TargetFramework:
# a global single-TFM property confuses xunit.v3's Microsoft.Testing.Platform test
# discovery and it reports "Zero tests ran". UseAppHost is left at its default (true):
# xunit.v3 test projects must build an app host, and Red Hat's s390x SDK ships the
# rhel.9-s390x app host pack, so it builds natively here.
for p in Confluent.Kafka.UnitTests \
         Confluent.SchemaRegistry.UnitTests \
         Confluent.SchemaRegistry.Serdes.UnitTests \
         Confluent.Kafka.OAuthBearer.Aws.UnitTests; do
    echo "--- build + test $p ---"
    # xunit.v3 runs on Microsoft.Testing.Platform, invoked through --project rather
    # than a positional argument. Build explicitly first and run with --no-build:
    # letting dotnet test build implicitly makes the platform report "Zero tests ran",
    # so the main CI runs --no-build too.
    dotnet build "test/$p/$p.csproj" -f net10.0 -c "$CONFIGURATION"
    dotnet test --project "test/$p/$p.csproj" -f net10.0 -c "$CONFIGURATION" --no-build
done
INNER
chmod +x run-unit-tests.sh

# No --platform flag: the agent is natively s390x, so this pulls the s390x image.
# -u 0 because the UBI9 image runs as uid 1001, which cannot write bin/ and obj/
# into the bind-mounted checkout.
#
# CONFIGURATION: the pipeline sets it for every block (Release); pass it into the
# container, which doesn't inherit the agent's environment.
docker pull -q registry.access.redhat.com/ubi9/dotnet-100
docker image inspect --format '{{index .RepoDigests 0}}' registry.access.redhat.com/ubi9/dotnet-100
set +e
docker run --rm -u 0 \
    -e DOTNET_CLI_TELEMETRY_OPTOUT=true \
    -e "CONFIGURATION=${CONFIGURATION:-Release}" \
    -v "$PWD:/work" -w /work \
    registry.access.redhat.com/ubi9/dotnet-100 \
    ./run-unit-tests.sh
RET=$?
set -e

# The container runs as root, so bin/ and obj/ come back root owned. Hand the
# checkout back to the agent user so the shared agent's workspace stays clean.
sudo chown -R "$(id -u):$(id -g)" . || true

exit $RET
