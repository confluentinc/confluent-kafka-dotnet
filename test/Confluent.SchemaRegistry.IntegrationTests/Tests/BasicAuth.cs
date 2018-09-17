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
using System.Web;
using System.Net.Http;
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void BasicAuth(Config config)
        {
            var testSchema1 = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            // 1. valid configuration cases

            using (var sr = new CachedSchemaRegistryClient(
                new Dictionary<string, object>
                { 
                    { "schema.registry.url", config.ServerWithAuth },
                    { "schema.registry.basic.auth.credentials.source", "USER_INFO" },
                    { "schema.registry.basic.auth.user.info", $"{config.Username}:{config.Password}" }
                }))
            {
                var topicName = Guid.NewGuid().ToString();
                var subject = sr.ConstructValueSubjectName(topicName);
                var id = sr.RegisterSchemaAsync(subject, testSchema1).Result;
                var schema = sr.GetLatestSchemaAsync(subject).Result;
                Assert.Equal(schema.Id, id);
            }

            using (var sr = new CachedSchemaRegistryClient(
                new Dictionary<string, object>
                { 
                    { "schema.registry.url", config.ServerWithAuth },
                    { "schema.registry.basic.auth.credentials.source", "SASL_INHERIT" },
                    { "sasl.username", config.Username },
                    { "sasl.password", config.Password }
                }))
            {
                var topicName = Guid.NewGuid().ToString();
                var subject = sr.ConstructValueSubjectName(topicName);
                var id = sr.RegisterSchemaAsync(subject, testSchema1).Result;
                var schema = sr.GetLatestSchemaAsync(subject).Result;
                Assert.Equal(schema.Id, id);
            }

            // 2. invalid configuration cases

            Assert.Throws<ArgumentException>(() =>
            {
                var sr = new CachedSchemaRegistryClient(new Dictionary<string, object>
                { 
                    { "schema.registry.url", config.ServerWithAuth },
                    { "schema.registry.basic.auth.credentials.source", "SASL_INHERIT" },
                    { "schema.registry.basic.auth.user.info", $"{config.Username:config.Password}" }
                }); 
            });

            Assert.Throws<ArgumentException>(() => 
            {   
                var srConfig = new SchemaRegistryConfig
                {
                    SchemaRegistryUrl = config.ServerWithAuth
                    
                };
srConfig.
                var sr = new CachedSchemaRegistryClient(new Dictionary<string, object>
                { 
                    { "schema.registry.url", config.ServerWithAuth },
                    { "schema.registry.basic.auth.credentials.source", "USER_INFO" },
                    { "sasl.username", config.Username },
                    { "sasl.password", config.Password }
                });
            });

            // conntect to authenticating without credentials. shouldn't work.
            Assert.Throws<HttpRequestException>(() => 
            { 
                var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = config.ServerWithAuth });
                var topicName = Guid.NewGuid().ToString();
                var subject = sr.ConstructValueSubjectName(topicName);
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