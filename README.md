Confluent.Kafka - Apache Kafka .NET client library
==================================================

**Confluent.Kafka** is a .NET client for [Apache Kafka](http://kafka.apache.org/) based on
[librdkafka](https://github.com/edenhill/librdkafka).

Derived from Andreas Heider's [rdkafka-dotnet](https://github.com/ah-/rdkafka-dotnet).

*****developer preview*****

- Only limited testing was performed
- Minor API changes anticipated
- Feedback encouraged

*****developer preview*****

## Usage

Reference the [Confluent.Kafka NuGet package](https://www.nuget.org/packages/Confluent.Kafka/) (version 0.9.4-preview2).

To install Confluent.Kafka from within Visual Studio, run the following command in the Package Manager Console:

```
Install-Package Confluent.Kafka -Pre -Version 0.9.4-preview2
```

To reference in a dotnet core project, add `"Confluent.Kafka": "0.9.4-preview2"` to the dependencies section of the project.json file.

## Examples

Take a look in the [examples](examples) directory. The [integration tests](test/Confluent.Kafka.IntegrationTests/Tests) also serve as good examples.


## Build

To build the library or any test or example project, run the following from within the relevant project directory:

```
dotnet restore
dotnet build
```

To run an example project, run the following from within the example's project directory:

```
dotnet run <args>
```

To run the integration or unit tests, run the following from within the relevant project directory:

```
dotnet test
```

To create a nuget package, run the following from wihin `src/Confluent.Kafka`:

```
dotnet pack
```


Copyright (c) 2016-2017 [Confluent Inc.](https://www.confluent.io), 2015-2016, [Andreas Heider](mailto:andreas@heider.io)
