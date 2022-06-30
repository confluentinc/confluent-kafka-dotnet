#### Avro

You can generate the specific classes in this example using the `avrogen` tool, available via Nuget (.NET Core 2.1 required):

```
dotnet tool install --global Apache.Avro.Tools
```

Usage:

In this example we use an avro namespace different from the .NET one.
It's especially useful if you have programs in different
programming languages reading from the same topic.

```
avrogen -s User.avsc . --namespace "confluent.io.examples.serialization.avro:Confluent.Kafka.Examples.AvroSpecific"
```

For more information about working with Avro in .NET, refer to the the blog post [Decoupling Systems with Apache Kafka, Schema Registry and Avro](https://www.confluent.io/blog/decoupling-systems-with-apache-kafka-schema-registry-and-avro/)
