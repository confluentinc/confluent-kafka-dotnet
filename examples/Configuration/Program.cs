// Copyright 2022 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;


// An example demonstrating working with Confluent.Kafka and ASP.NET style configuration.

IConfiguration configuration = new ConfigurationBuilder()
    .AddJsonFile("./appsettings.json")
    .Build();

// You can bind configuration directly to the strongly typed configuration classes.
var pConfig = configuration.GetSection("Producer").Get<ProducerConfig>().ThrowIfContainsNonUserConfigurable();
var cConfig = configuration.GetSection("Consumer").Get<ConsumerConfig>().ThrowIfContainsNonUserConfigurable();
var topicName = configuration.GetValue<string>("General:TopicName");

// After reading the user configuration, adjust as required.
// Note that some Consumer config properties have implications for application logic and generally
// shouldn't be set independent of the code. The ThrowIfHasNonUserConfigurable() helper method above
// is used to check for these and will throw an ArgumentException if any are present in the
// configuration. EnableAutoCommit is one such property. There are no such Producer or AdminClient
// properties, and the ThrowIfContainsNonUserConfigurable method is (currently) a noop in these cases.
cConfig.EnableAutoCommit = false;

var assigned = false;
using var consumer = new ConsumerBuilder<Null, string>(cConfig)
    .SetPartitionsAssignedHandler((c, ps) => { assigned = true; })
    .Build();

using var producer = new ProducerBuilder<Null, string>(pConfig).Build();

Console.WriteLine($"Subscribing to topic '{topicName}'...");
consumer.Subscribe(topicName);

Console.WriteLine("Entering consume loop...");
while (true)
{
    var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));

    if (consumeResult != null)
    {
        Console.WriteLine($"Read value: '{consumeResult.Message.Value}' from partition {consumeResult.Partition} at offset {consumeResult.Offset}");
        consumer.Commit();
        break;
    }

    if (!assigned)
    {
        Console.WriteLine("Waiting for assignment...");
        continue;
    }

    Console.WriteLine("Producing message with value: 'testvalue'...");
    await producer.ProduceAsync(topicName, new Message<Null, string> { Value = "testvalue" });
}

consumer.Close();
