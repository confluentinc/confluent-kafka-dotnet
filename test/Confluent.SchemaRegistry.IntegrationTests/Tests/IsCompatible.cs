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
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void IsCompatible_Topic(Config config)
        {
            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });

            var testSchema1 = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

            var testSchema2 = // incompatible with testSchema1
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_shape\",\"type\":[\"string\",\"null\"]}]}";


            // case 1: record specified.
            var topicName = Guid.NewGuid().ToString();
            var subject = SubjectNameStrategy.Topic.ConstructKeySubjectName(topicName, "Confluent.Kafka.Examples.AvroSpecific.User");

            sr.RegisterSchemaAsync(subject, testSchema1).Wait();

            Assert.False(sr.IsCompatibleAsync(subject, testSchema2).Result);
            Assert.True(sr.IsCompatibleAsync(subject, testSchema1).Result);


            // case 2: record not specified.
            topicName = Guid.NewGuid().ToString();
            subject = SubjectNameStrategy.Topic.ConstructKeySubjectName(topicName, null);

            sr.RegisterSchemaAsync(subject, testSchema1).Wait();

            Assert.False(sr.IsCompatibleAsync(subject, testSchema2).Result);
            Assert.True(sr.IsCompatibleAsync(subject, testSchema1).Result);


            // Note: backwards / forwards compatibility scenarios are not tested here. This is really just testing the API call.
        }
    }
}
