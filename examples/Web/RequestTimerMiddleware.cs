using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;
using System.Threading.Tasks;
using Confluent.Kafka;


namespace Web
{
    /// <summary>
    ///     Middleware that times how long a web request takes to be handled,
    ///     and logs this to Kafka.
    /// </summary>
    public class RequestTimerMiddleware
    {
        private readonly string topic;
        private readonly KafkaDependentProducer<string, long> producer;
        private readonly RequestDelegate next;

        public RequestTimerMiddleware(RequestDelegate next, KafkaDependentProducer<string, long> producer, IConfiguration config)
        {
            this.next = next;
            this.producer = producer;
            topic = config.GetValue<string>("Kafka:RequestTimeTopic");
        }

        public async Task Invoke(HttpContext context)
        {
            Stopwatch s = new Stopwatch();
            try
            {
                s.Start();
                await next(context);
            }
            finally
            {
                s.Stop();

                // Non-blocking fire-and-forget produce call. Any errors will go unreported / will not be handled.
                producer.Produce(topic, new Message<string, long> { Key = context.Request.Path.Value, Value = s.ElapsedMilliseconds });

                // Alternatively, you can await the produce call. This will delay the request until the result of
                // the produce call is known. An exception will be throw in the event of an error.
                // await producer.ProduceAsync(topic, new Message<string, long> { Key = context.Request.Path.Value, Value = s.ElapsedMilliseconds });
            }
        }

    }
}
