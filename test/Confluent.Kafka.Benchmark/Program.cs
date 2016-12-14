
namespace Confluent.Kafka.Benchmark
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var bootstrapServers = args[0];
            var topic = args[1];
            BenchmarkProducer.TaskProduce(bootstrapServers, topic);
            BenchmarkProducer.DeliveryHandlerProduce(bootstrapServers, topic);
        }
    }
}
