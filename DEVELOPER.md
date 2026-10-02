# Developer Notes

This document provides information useful to developers working on confluent-kafka-dotnet.


## Building

Nuget packages are built automatically by Semaphore CI corresponding to every commit to a PR or master branch as well as release tags. For further details, inspect the [.semaphore/semaphore.yml](.semaphore/semaphore.yml) file.

Pushing a release tag also fires the **Mend SCA + SAST Scan** block, which triggers the
ad hoc `mend-source-scan` task in `appsec-semaphore-workflows` (via `sem-trigger`) to run
a Mend SCA (`mend dep`) and SAST (`mend sast`) scan against the tagged source. This runs
out-of-band: the trigger call itself is fire-and-forget (bounded by a 60s local timeout)
and does not block or fail the pipeline, since the scan itself can run far longer than a
release build should wait. Check scan results/findings in Mend directly (product family
`COSS`), not in this pipeline's own job status.


## Tests

### Unit Tests

There are unit test suites corresponding to each nuget package. These are [Confluent.Kafka.UnitTests](test/Confluent.Kafka.UnitTests), 
[Confluent.SchemaRegistry.UnitTests](test/Confluent.SchemaRegistry.UnitTests) and
[Confluent.SchemaRegistry.Serdes.UnitTests](test/Confluent.SchemaRegistry.Serdes.UnitTests). To execute, enter the
relevant directory and run:

```
dotnet test
```

### Integration Tests

From the test/docker directory bring up the Kafka cluster with two schema registry instances (one with basic auth enabled, one without).

```
docker-compose up
```

There are integration test suites corresponding to each nuget package. These are [Confluent.Kafka.IntegrationTests](test/Confluent.Kafka.IntegrationTests), 
[Confluent.SchemaRegistry.IntegrationTests](test/Confluent.SchemaRegistry.IntegrationTests) and
[Confluent.SchemaRegistry.Serdes.IntegrationTests](test/Confluent.SchemaRegistry.Serdes.IntegrationTests).

To execute, enter the relevant directory and run:

```
dotnet test
```
