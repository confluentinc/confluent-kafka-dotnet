// Copyright 2016-2017 Confluent Inc.
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
using System.Text;
using System.Collections.Generic;
using Xunit;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;


namespace Confluent.Kafka.IntegrationTests
{
    public static class Util
    {
        /// <summary>
        ///     Produces <param name="number"/> messages of size <param name="size"/>.
        /// </summary>
        /// <returns>
        ///     The offset of the first produced message.
        /// </returns>
        public static TopicPartitionOffset ProduceNullStringMessages(string bootstrapServers, string topic, int size, int number)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
            
            var sb = new StringBuilder(size);
            for (int i=0; i<size; ++i)
            {
                // 01234 ... xyz0123 ...
                sb.Append((char)(0x30 + i%74));
            }
            var msg = sb.ToString();

            DeliveryResult<Null, string> firstDeliveryReport = null;
            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                for (int i=0; i<number; ++i)
                {
                    var dr = producer.ProduceAsync(topic, new Message<Null, string> { Value = msg }).Result;
                    Assert.NotNull(dr);
                    Assert.NotNull(dr.Message);
                    if (i == 0)
                    {
                        firstDeliveryReport = dr;
                    }
                    Assert.Equal(topic, dr.Topic);
                    Assert.NotEqual<long>(dr.Offset, Offset.Unset);
                }

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            return firstDeliveryReport.TopicPartitionOffset;
        }

        public static string GetUnsecuredJwt(string aud, string scope, DateTimeOffset iat, DateTimeOffset exp)
        {
            var header = new
            {
                alg = "none",
                typ = "JWT"
            };
            var payload = new
            {
                iat = iat.ToUnixTimeSeconds(),
                exp = exp.ToUnixTimeSeconds(),
                typ = "Bearer",
                sub = "Tester",
                aud,
                scope
            };

            var headerJson = JsonConvert.SerializeObject(header);
            var payloadJson = JsonConvert.SerializeObject(payload);

            var jwt = $"{headerJson.ToUnpaddedBase64()}.{payloadJson.ToUnpaddedBase64()}.";
            return jwt;
        }

        private static string ToUnpaddedBase64(this string s)
            => Convert.ToBase64String(Encoding.UTF8.GetBytes(s)).TrimEnd('=');
    }
}
