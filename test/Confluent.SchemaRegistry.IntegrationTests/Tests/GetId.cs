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
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void GetId(Config config)
        {
            var topicName = Guid.NewGuid().ToString();

            var testSchema1 =
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });

            var subject = SubjectNameStrategy.Topic.ConstructKeySubjectName(topicName, null);
            var id = sr.RegisterSchemaAsync(subject, testSchema1).Result;
            var id2 = sr.GetSchemaIdAsync(subject, testSchema1).Result;

            Assert.Equal(id, id2);

            Assert.Throws<SchemaRegistryException>(() => 
            {
                try
                {
                    sr.GetSchemaIdAsync(subject, "{\"type\": \"string\"}").Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException;
                }
            });
        }
    }
}
