// Copyright 20 Confluent Inc.
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
using System.Collections.Generic;
using System.Net.Http;
using Xunit;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void SslAuth(Config config)
        {
            var testSchema1 =
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            // 1. valid configuration cases

            // 1.1. using SSL valid certificate.
            var conf = new SchemaRegistryConfig
            {
                Url = config.ServerWithSsl,
                SslKeystoreLocation = config.KeystoreLocation,
                SslKeystorePassword = config.KeystorePassword,
                SslCaLocation = config.CaLocation,
                EnableSslCertificateVerification = bool.Parse(config.EnableSslCertificateVerification),
            };

            // some sanity checking of strongly typed config property name mappings.
            Assert.Equal(config.ServerWithSsl, conf.Get("schema.registry.url"));

            using (var sr = new CachedSchemaRegistryClient(conf))
            {
                var topicName = Guid.NewGuid().ToString();
                var subject = SubjectNameStrategy.Topic.ToDelegate()(new SerializationContext(MessageComponentType.Value, topicName), null);
                var id = sr.RegisterSchemaAsync(subject, testSchema1).Result;
                var schema = sr.GetLatestSchemaAsync(subject).Result;
                Assert.Equal(schema.Id, id);
            }

            // try to connect with invalid SSL config. shouldn't work.
            Assert.Throws<HttpRequestException>(() =>
            {
                var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.ServerWithSsl });
                var topicName = Guid.NewGuid().ToString();
                var subject = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
                try
                {
                    var id = sr.RegisterSchemaAsync(subject, testSchema1).Result;
                }
                catch (Exception e)
                {
                    throw e.InnerException;
                }
            });

        }
    }
}
