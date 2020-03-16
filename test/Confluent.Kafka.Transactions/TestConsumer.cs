using System;
using System.Collections.Generic;


namespace Confluent.Kafka.Transactions
{
    public class TestConsumer
    {
        static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);
        
        string bootstrapServers;
        SimulationConfig conf;

        public TestConsumer(string bootstrapServers, SimulationConfig conf)
        {
            this.conf = conf;
            this.bootstrapServers = bootstrapServers;
        }

        public void Run()
        {
            var cConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = Guid.NewGuid().ToString(),
                AutoOffsetReset = AutoOffsetReset.Earliest,
                IsolationLevel = IsolationLevel.ReadCommitted,
            };

            var lasts = new Dictionary<int, int>();

            IConsumer<int, int> consumer = null;
            try
            {
                consumer = new ConsumerBuilder<int, int>(cConfig).Build();
                consumer.Subscribe(conf.Topic);

                while (true)
                {
                    var cr = consumer.Consume();

                    if (!lasts.ContainsKey(cr.Message.Key)) { lasts.Add(cr.Message.Key, -1); }
                    if (cr.Message.Value == lasts[cr.Message.Key] + 1) { Console.Write("."); }
                    else { Console.Write($"[producer {cr.Message.Key} expected seq {lasts[cr.Message.Key]+1} but got {cr.Message.Value}]"); break; }
                    Console.Out.Flush();
                    lasts[cr.Message.Key] = cr.Message.Value;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            finally
            {
                if (consumer != null)
                {
                    consumer.Close();
                    consumer.Dispose();
                }
                Console.WriteLine("Consume loop exited...");
            }
        }
    }
}
