using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using System.Reflection;

namespace Confluent.Kafka.IntegrationTests
{
    public static partial class Tests
    {
        private static List<object[]> kafkaParameters;

        public static IEnumerable<object[]> KafkaParameters()
        {
            if (kafkaParameters == null)
            {
                var codeBase = typeof(Tests).GetTypeInfo().Assembly.CodeBase;
                // TODO: Better way to turn Uri into path?
                var assemblyPath = codeBase.Substring("file://".Length);
                var assemblyDirectory = Path.GetDirectoryName(assemblyPath);
                var jsonPath = Path.Combine(assemblyDirectory, "kafka.parameters.json");
                dynamic json = JsonConvert.DeserializeObject(File.ReadAllText(jsonPath));
                kafkaParameters = new List<object[]>() { new object[] { json.bootstrapServers.ToString(), json.topic.ToString() } };
            }
            return kafkaParameters;
        }
    }
}
