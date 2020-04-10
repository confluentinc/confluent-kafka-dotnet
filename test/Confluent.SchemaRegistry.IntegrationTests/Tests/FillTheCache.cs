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
        public static void FillTheCache(Config config)
        {
            const int capacity = 16;

            const string testSchema =
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var srConfig = new SchemaRegistryConfig
            {
                Url = config.Server,
                RequestTimeoutMs = 3000,
                MaxCachedSchemas = capacity
            };

            var sr = new CachedSchemaRegistryClient(srConfig);

            var registerCount = capacity + 10;

            var subjects = new List<string>();
            var ids = new List<int>();

            // test is that this does not throw. Also, inspect in debugger.
            for (int i = 0; i < registerCount; ++i)
            {
                var topicName = Guid.NewGuid().ToString();
                var subject = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
                subjects.Add(subject);

                var id = sr.RegisterSchemaAsync(subject, testSchema).Result;
                ids.Add(id);
            }
        }
    }
}
