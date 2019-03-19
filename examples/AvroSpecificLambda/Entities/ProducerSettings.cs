using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace AvroSpecificLambda.Entities
{
    public class ProducerSettings 
    {
        public string BootstrapServers { get; set; }
        public string SchemaRegistryUrl { get; set; }
        public string TopicName { get; set; }

        public override bool Equals(object obj)
        {
            if (obj is ProducerSettings)
            {
                var target = (ProducerSettings)obj;

                return BootstrapServers == target.BootstrapServers
                    && SchemaRegistryUrl == target.SchemaRegistryUrl
                    && TopicName == target.TopicName;
            }
            else
            {
                return base.Equals(obj);
            }
        }

        public override int GetHashCode()
        {
            return BootstrapServers.GetHashCode() ^ SchemaRegistryUrl.GetHashCode() ^ TopicName.GetHashCode();
        }
    }
}