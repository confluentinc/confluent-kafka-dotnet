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
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static void Protobuf(Config config)
        {
            var srInitial = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var testSchemaBase64 = Confluent.Kafka.Examples.Protobuf.User.Descriptor.File.SerializedData.ToBase64();
            var topicName = Guid.NewGuid().ToString();
            var subjectInitial = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
            var subject = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName+"2", null);

            // check that registering a base64 protobuf schema works.
            var id1 = srInitial.RegisterSchemaAsync(subjectInitial, new Schema(testSchemaBase64, SchemaType.Protobuf)).Result;
            var schema1 = sr.GetSchemaAsync(id1, "serialized").Result; // use a different sr instance to ensure a cached value is not read.
            Assert.Equal(SchemaType.Protobuf, schema1.SchemaType);
            Assert.NotNull(schema1.SchemaString); // SR slightly munges the schema as a result of moving to a text representation and back so can't check for equality.

            // check that the id of the schema just registered can be retrieved.
            var id = sr.GetSchemaIdAsync(subjectInitial, new Schema(schema1.SchemaString, SchemaType.Protobuf)).Result;
            Assert.Equal(id1, id);

            // re-register the munged schema (to a different subject) and check that it is not re-munged.
            var id2 = sr.RegisterSchemaAsync(subject, schema1).Result;
            var schema2 = sr.GetSchemaAsync(id2, "serialized").Result;
            Assert.Equal(schema1.SchemaString, schema2.SchemaString);
            Assert.Equal(schema1.SchemaType, schema2.SchemaType);

            // This sequence of operations is designed to test caching behavior (and two alternat schema getting methods).
            var schemaAsText = sr.GetSchemaAsync(id2).Result;
            var schemaAsSerialized = sr.GetSchemaAsync(id2, "serialized").Result;
            var schemaAsText2 = sr.GetSchemaAsync(id2).Result;
            var schemaAsSerialized2 = sr.GetSchemaAsync(id2, "serialized").Result;
            var latestSchema = sr.GetLatestSchemaAsync(subject).Result; // should come back as text.
            var schemaAsSerialized3 = sr.GetSchemaAsync(id2, "serialized").Result;
            var latestSchema2 = sr.GetRegisteredSchemaAsync(subject, latestSchema.Version).Result; // should come back as text.
            Assert.Equal(schema1.SchemaString, schemaAsSerialized.SchemaString);
            Assert.Equal(SchemaType.Protobuf, schemaAsSerialized.SchemaType);
            Assert.Empty(schemaAsSerialized.References);
            Assert.Equal(schema1.SchemaString, schemaAsSerialized2.SchemaString);
            Assert.Equal(schema1.SchemaString, schemaAsSerialized3.SchemaString);
            Assert.NotEqual(schema1.SchemaString, schemaAsText.SchemaString);
            Assert.Equal(schemaAsText.SchemaString, schemaAsText2.SchemaString);
            Assert.Equal(schemaAsText.SchemaString, latestSchema.SchemaString);
            Assert.Equal(schemaAsText.SchemaString, latestSchema2.SchemaString);
            Assert.Equal(SchemaType.Protobuf, schemaAsText.SchemaType);
            Assert.Empty(schemaAsText2.References);

            // compatibility
            var compat = sr.IsCompatibleAsync(subject, schema2).Result;
            Assert.True(compat);
            var avroSchema = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";
            var compat2 = sr.IsCompatibleAsync(subject, avroSchema).Result;
            Assert.False(compat2);
            var compat3 = sr.IsCompatibleAsync(subject, new Schema(avroSchema, SchemaType.Avro)).Result;
            Assert.False(compat3);

            // invalid type
            Assert.ThrowsAny<Exception>(() => {
                sr.RegisterSchemaAsync(SubjectNameStrategy.Topic.ConstructKeySubjectName(topicName+"3", null), new Schema(avroSchema, SchemaType.Protobuf)).Wait();
            });
        }
    }
}
