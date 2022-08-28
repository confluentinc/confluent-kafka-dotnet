// Copyright 2018 Confluent Inc.
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


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void Failover(Config config)
        {
            var testSchema =
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            using (var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = $"{config.Server},http://localhost:65432" }))
            {
                var topicName = Guid.NewGuid().ToString();
                var subject = SubjectNameStrategy.Topic.ConstructKeySubjectName(topicName, null);
                var id = sr.RegisterSchemaAsync(subject, testSchema, false).Result;
                var id2 = sr.GetSchemaIdAsync(subject, testSchema, false).Result;
                Assert.Equal(id, id2);
            }

            using (var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = $"http://localhost:65432,{config.Server}" }))
            {
                var topicName = Guid.NewGuid().ToString();
                var subject = SubjectNameStrategy.Topic.ConstructKeySubjectName(topicName, null);
                var id = sr.RegisterSchemaAsync(subject, testSchema, false).Result;
                var id2 = sr.GetSchemaIdAsync(subject, testSchema, false).Result;
                Assert.Equal(id, id2);
            }

            using (var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = $"http://localhost:65432,http://localhost:65431" }))
            {
                var topicName = Guid.NewGuid().ToString();
                var subject = SubjectNameStrategy.Topic.ConstructKeySubjectName(topicName, null);
                
                Assert.Throws<HttpRequestException>(() => 
                {
                    try
                    {
                        sr.RegisterSchemaAsync(subject, testSchema).Wait();
                    }
                    catch (AggregateException e)
                    {
                        throw e.InnerException;
                    }
                });
            }

        }
    }
}
