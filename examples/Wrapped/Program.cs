using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;

namespace Confluent.Kafka.Wrapped
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

            using (var producer = new Producer(config))
            {
                // sProducer1 is a lightweight wrapper around a Producer instance that adds
                // (string, string) serialization. Note that sProducer1 does not need to be
                // (and cannot be) disposed.
                var sProducer1 = producer.Wrap<string, string>(new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8));

                // sProducer2 is another lightweight wrapper around kafkaProducer that adds
                // (null, int) serialization. When you do not wish to write any data to a key
                // or value, the Null type should be used.
                var sProducer2 = producer.Wrap<Null, int>(new NullSerializer(), new IntSerializer());

                // write (string, string) data to topic "first-topic", statically type checked.
                sProducer1.ProduceAsync("first-topic", "my-key-value", "my-value");

                // write (null, int) data to topic "second-data". statically type checked, using
                // the same underlying producer as the producer1.
                sProducer2.ProduceAsync("second-topic", null, 42);

                // producers are NOT tied to topics. Although it's unusual that you might want to
                // do so, you can use different serializing producers to write to the same topic.
                sProducer2.ProduceAsync("first-topic", null, 107);

                // ProducerAsync tasks are not waited on - there is a good chance they are still
                // in flight.
                producer.Flush();
            }
        }
    }
}
