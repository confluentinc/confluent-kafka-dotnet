#### Avro

You can generate the specific classes in this example using the `avrogen` tool, available via Nuget (.NET Core 2.1 required):

```
dotnet tool install --global Apache.Avro.Tools
```

Usage:

```
avrogen -s User.avsc .
```

For more information about working with Avro in .NET, refer to the the blog post [Decoupling Systems with Apache Kafka, Schema Registry and Avro](https://www.confluent.io/blog/decoupling-systems-with-apache-kafka-schema-registry-and-avro/)
