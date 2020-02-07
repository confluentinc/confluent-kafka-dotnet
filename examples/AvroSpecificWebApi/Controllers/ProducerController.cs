using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AvroSpecificWebApi.Entities;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.AspNetCore.Mvc;

namespace AvroSpecificWebApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : Controller
    {
        public ProducerFactory ProducerFactory { get; }

        // Receives the ProducerFactory created in Startup.cs
        public ProducerController(ProducerFactory producerFactory)
        {
            ProducerFactory = producerFactory;
        }
        
        // Note: WebApi and MVC controller methods that use Confluent.Kafka MUST use the async. 
        // Using .Result or .GetAwaiter().GetResult() will cause your application to hang. 
        [HttpPost]
        public async Task<string> Post([FromBody]Entities.ProducerSettings producerSettings)
        {
            User user = new User { name = "Test User Name", favorite_color = "Green", favorite_number = 42 };

            return await ProducerFactory.GetProducer(producerSettings)
                .ProduceAsync(producerSettings.TopicName, new Message<string, User> { Key = user.name, Value = user })
                .ContinueWith(task => task.IsFaulted
                    ? $"error producing message: {task.Exception.Message}"
                    : $"produced to: {task.Result.TopicPartitionOffset}");
        }
    }
}