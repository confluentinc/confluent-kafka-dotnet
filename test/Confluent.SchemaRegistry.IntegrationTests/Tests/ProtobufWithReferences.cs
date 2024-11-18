// Copyright 2020 Confluent Inc.
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
        public static void ProtobufWithReferences(Config config)
        {
            var srInitial = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var testSchemaBase64PersonName = Confluent.Kafka.Examples.Protobuf.PersonName.Descriptor.File.SerializedData.ToBase64();
            var testSchemaBase64Person = Confluent.Kafka.Examples.Protobuf.Person.Descriptor.File.SerializedData.ToBase64();
            var topicName = Guid.NewGuid().ToString();
            var subjectInitial = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
            var subject = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName + "2", null);

            // check that registering a base64 protobuf schema works, first with the referenced schema
            var PersonName = "confluent.kafka.examples.protobuf.PersonName";
            var id1 = srInitial.RegisterSchemaAsync(PersonName, new Schema(testSchemaBase64PersonName, SchemaType.Protobuf)).Result;
            var sc1 = srInitial.GetSchemaAsync(id1).Result;
            Assert.NotNull(sc1);
            
            // then with the schema that references it
            var refs = new List<SchemaReference> { new SchemaReference(PersonName, PersonName, 1) };
            var id2 = sr.RegisterSchemaAsync(subjectInitial, new Schema(testSchemaBase64Person, refs, SchemaType.Protobuf)).Result;
            var sc2 = sr.GetSchemaAsync(id2).Result;
            Assert.NotNull(sc2);

            // then with the schema that references it and a different subject
            var id3 = sr.RegisterSchemaAsync(subject, new Schema(testSchemaBase64Person, refs, SchemaType.Protobuf)).Result;
            var sc3 = sr.GetSchemaAsync(id3).Result;
            Assert.NotNull(sc3);
        }
    }
}
