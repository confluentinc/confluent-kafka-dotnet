# AvroSpecificWebApi

This project contains a basic .Net Core WebApi implementation with Swagger, and shows how to produce a Kafka Message from WebApi. 

Swagger: /swagger
Api: /api/Producer

The producerSettings object has the following format:

{
  "BootstrapServers": "kafka1.myserver.com:9092,kafka2.myserver.com:9092,",
  "SchemaRegistryUrl": "http://sr.myserver.com:8081",
  "TopicName": "mytopic"
}

## Async Methods Only!

You must use async WebApi methods to work with the Confluent.Kafka library. Attempting to use .Result, or GetAwaiter().GetResult() will freeze the WebApi thread and prevent your calls from returning any results. This is true for all async methods under WebApi, not just the Confluent libraries.
