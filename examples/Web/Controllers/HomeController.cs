using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;


namespace Web.Controllers
{
    public class HomeController : Controller
    {
        private readonly Random random;
        private string topic;
        private readonly KafkaDependentProducer<Null, string> producer;

        public HomeController(KafkaDependentProducer<Null, string> producer, IConfiguration config)
        {
            // In a real-world application, you might be using Kafka as a system of record and may wish
            // to update application state in your request handlers. Or you may wish to write some
            // analytics data related to your handler logic. In this example, we aren't doing anything
            // interesting, so we just write frivolous messages to demonstrate how to set things up.
            this.topic = config.GetValue<string>("Kafka:FrivolousTopic");
            this.producer = producer;
            this.random = new Random();
        }

        public async Task<IActionResult> Index()
        {
            // Simulate a complex request handler by delaying a random amount of time.
            await Task.Delay((int)(this.random.NextDouble()*100));

            // fire-and-forget call - do not delay completion of the page request on the
            // result of produce call to Kafka. Any delivery error will be silently ignored.
            this.producer.Produce(topic, new Message<Null, string> { Value = "Frivolous message (index)" });
            return View();
        }

        public async Task<IActionResult> Page1()
        {
            await Task.Delay((int)(this.random.NextDouble()*100));

            // Delay completion of the page request on the result of the produce call.
            // An exception will be thrown in the case of an error.
            await this.producer.ProduceAsync(topic, new Message<Null, string> { Value = "Frivolous message #1" });
            return View();
        }
    }
}
