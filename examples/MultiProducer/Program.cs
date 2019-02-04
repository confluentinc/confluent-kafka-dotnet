// Copyright 2016-2017 Confluent Inc.
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
using System.Text;
using System.Collections.Generic;


namespace Confluent.Kafka.Examples.MultiProducer
{
    /// <summary>
    ///     An example showing how to construct a Producer which re-uses the 
    ///     underlying librdkafka client instance (and Kafka broker connections)
    ///     of another producer instance. This allows you to produce messages
    ///     with different types efficiently.
    /// </summary>
    public class Program
    {
        public static void Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = args[0] };

            using (var producer = new ProducerBuilder<string, string>(config).Build())
            using (var producer2 = new DependentProducerBuilder<Null, int>(producer.Handle).Build())
            {
                // write (string, string) data to topic "first-topic".
                producer.ProduceAsync("first-topic", new Message<string, string> { Key = "my-key-value", Value = "my-value" });

                // write (null, int) data to topic "second-data" using the same underlying broker connections.
                producer2.ProduceAsync("second-topic", new Message<Null, int> { Value = 42 });

                // producers are not tied to topics. Although it's unusual that you might want to
                // do so, you can use different producers to write to the same topic.
                producer2.ProduceAsync("first-topic", new Message<Null, int> { Value = 107 });

                // As the Tasks returned by ProduceAsync are not waited on there will still be messages in flight.
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
