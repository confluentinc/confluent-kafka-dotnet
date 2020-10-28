## Web Example

This project demonstrates how to integrate Kafka with a .NET web application. It provides
examples of both producing and consuming messages.


#### KafkaClientHandle

Demonstrates how to wrap the Confluent.Kafka producer in a class that can be registered as
a singleton service, including how to bind client configuration from an injected IConfiguration
instance to a Confluent.Kafka.ProducerConfig object.


#### KafkaDependentProducer

Demonstrates how to produce messages with different types using a single producer instance.
This is more efficient than creating more than one producer instance.


#### HomeController

Demonstrates how utilize a previously registered KafkaDependentProducer singleton service in
a controller.


#### RequestTimerMiddleware

Demonstrates how utilize a KafkaDependentProducer service in a middleware component that
measures how long a web request takes to handle, and logs the information to Kafka.


#### RequestTimeConsumer

Demonstrates how to run a Confluent.Kafka consumer as an IHostedService.

