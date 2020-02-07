using AvroSpecificWebApi.Entities;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;

namespace AvroSpecificWebApi.Controllers
{
    public class ProducerController : ApiController
    {
        private ProducerFactory _producerFactory = new ProducerFactory();

        // Note: WebApi and MVC controller methods that use Confluent.Kafka MUST use the async. 
        // Using .Result or .GetAwaiter().GetResult() will cause your application to hang. 
        public async Task<string> Post([FromBody]Entities.ProducerSettings producerSettings)
        {
            User user = new User { name = "Test User Name", favorite_color = "Green", favorite_number = 42 };

            return await _producerFactory.GetProducer(producerSettings)
                .ProduceAsync(producerSettings.TopicName, new Message<string, User> { Key = user.name, Value = user })
                .ContinueWith(task => task.IsFaulted
                    ? $"error producing message: {task.Exception.Message}"
                    : $"produced to: {task.Result.TopicPartitionOffset}");
        }
    }
}
