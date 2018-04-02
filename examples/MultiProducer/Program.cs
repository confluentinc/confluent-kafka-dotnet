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
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Examples.MultiProducer
{
    /// <summary>
    ///     An example showing how to wrap a single Producer to produce messages using
    ///     different serializers.
    /// </summary>
    /// <remarks>
    ///     If you only want to use a single pair of serializers in your application,
    ///     you should use the Producer&lt;TKey, TValue&gt; constructor instead.
    /// </remarks>
    public class Program
    {
        public static void Main(string[] args)
        {
            var config = new Dictionary<string, object> { { "bootstrap.servers", args[0] } };

            using (var producer1 = new Producer<string, string>(config, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            using (var producer2 = new Producer<Null, int>(producer1.Handle, null, new IntSerializer()))
            {
                // write (string, string) data to topic "first-topic", statically type checked.
                producer1.ProduceAsync("first-topic", new Message<string, string> { Key = "my-key-value", Value = "my-value" });

                // write (null, int) data to topic "second-data". statically type checked, using
                // the same underlying librdkafka handle as producer1.
                producer2.ProduceAsync("second-topic", new Message<Null, int> { Value = 42 });

                // producers are NOT tied to topics. Although it's unusual that you might want to
                // do so, you can use different serializing producers to write to the same topic.
                producer2.ProduceAsync("first-topic", new Message<Null, int> { Value = 107 });

                // ProducerAsync tasks are not waited on - there is a good chance they are still
                // in flight.
                producer1.Flush(TimeSpan.FromSeconds(10));
            }
        }
    }
}
