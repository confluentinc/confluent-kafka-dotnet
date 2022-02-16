// Copyright 2020 Confluent Inc.
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
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;


namespace Web.Controllers
{
    public class HomeController : Controller
    {
        private string topic;
        private readonly KafkaDependentProducer<Null, string> producer;
        private readonly ILogger logger;

        public HomeController(KafkaDependentProducer<Null, string> producer, IConfiguration config, ILogger<HomeController> logger)
        {
            // In a real-world application, you might be using Kafka as a system of record and may wish
            // to update application state in your request handlers. Or you may wish to write some
            // analytics data related to your handler logic. In this example, we aren't doing anything
            // interesting, so we just write frivolous messages to demonstrate how to set things up.
            this.topic = config.GetValue<string>("Kafka:FrivolousTopic");
            this.producer = producer;
            this.logger = logger;
        }

        public async Task<IActionResult> Index()
        {
            // Simulate a complex request handler by delaying a random amount of time.
            await Task.Delay((int)(new Random((int)DateTime.Now.Ticks).NextDouble()*100));

            // Important note: DO NOT create a new producer instance every time you
            // need to produce a message (this is a common pattern with relational database
            // drivers, but it is extremely inefficient here). Instead, use a long-lived
            // singleton instance, as per this example.

            // Do not delay completion of the page request on the result of produce call.
            // Any errors are handled out of band in deliveryReportHandler.
            this.producer.Produce(topic, new Message<Null, string> { Value = "Frivolous message (index)" }, deliveryReportHandler);
            return View();
        }

        public async Task<IActionResult> Page1()
        {
            await Task.Delay((int)(new Random((int)DateTime.Now.Ticks).NextDouble()*100));

            // Delay completion of the page request on the result of the produce call.
            // An exception will be thrown in the case of an error.
            await this.producer.ProduceAsync(topic, new Message<Null, string> { Value = "Frivolous message #1" });
            return View();
        }

        private void deliveryReportHandler(DeliveryReport<Null, string> deliveryReport)
        {
            if (deliveryReport.Status == PersistenceStatus.NotPersisted)
            {
                // It is common to write application logs to Kafka (note: this project does not provide
                // an example logger implementation that does this). Such an implementation should
                // ideally fall back to logging messages locally in the case of delivery problems.
                this.logger.Log(LogLevel.Warning, $"Message delivery failed: {deliveryReport.Message.Value}");
            }
        }
    }
}
