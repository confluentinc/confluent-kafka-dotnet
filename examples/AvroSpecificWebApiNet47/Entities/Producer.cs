using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace AvroSpecificWebApi.Entities
{
    public class Producer
    {
        public string BootstrapServers { get; set; }
        public string SchemaRegistryUrl { get; set; }
        public string TopicName { get; set; }
    }
}