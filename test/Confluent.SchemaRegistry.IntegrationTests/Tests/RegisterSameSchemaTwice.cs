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
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void RegisterSameSchemaTwice(string server)
        {
            var topicName = Guid.NewGuid().ToString();

            var testSchema1 = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var sr = new CachedSchemaRegistryClient(new Dictionary<string, object>{ { "schema.registry.url", server } });

            var subject = sr.ConstructKeySubjectName(topicName, "Confluent.Kafka.Examples.AvroSpecific.User");
            Assert.Equal(topicName + "-key", subject);

            var id1 = sr.RegisterSchemaAsync(subject, testSchema1).Result;
            var id2 = sr.RegisterSchemaAsync(subject, testSchema1).Result;

            Assert.Equal(id1, id2);
            
            Assert.True(sr.GetAllSubjectsAsync().Result.Contains(subject));
        }
    }
}
