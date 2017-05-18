Strongly Named Unit Tests
=========================

This project, which is not included in the main solution, is designed for the sole purpose of verifying that the signed version of `Confluent.Kafka` (`Confluent.Kafka.StrongName`) has been built and signed correctly.  This project is built and run by the Travis build script.

When the build script begins, it performs a `dotnet build` on the main solution file.  After this has completed, it rebuilds the main `Confluent.Kafka` library, this time including signing information.  The output of this build step is the `Confluent.Kafka.StrongName` assembly.  Once `Confluent.Kafka.StrongName` has been built, this unit test assmbly, `Confluent.Kafka.StrongName.UnitTests`, is built and run against `Confluent.Kafka.StrongName`.  The `Confluent.Kafka.StrongName.UnitTests` assmbly itself is signed, and as such requires all dependencies to also be signed.  Should `Confluent.Kafka.StrongName` not actually be signed this build and test will fail.
