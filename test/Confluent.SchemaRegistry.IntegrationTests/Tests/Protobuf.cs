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
using System.Threading.Tasks;
using Xunit;


namespace Confluent.SchemaRegistry.IntegrationTests
{
    public static partial class Tests
    {
        [Theory, MemberData(nameof(SchemaRegistryParameters))]
        public static async Task Protobuf(Config config)
        {
            var srInitial = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });
            var testSchemaBase64 = Confluent.Kafka.Examples.Protobuf.User.Descriptor.File.SerializedData.ToBase64();
            var topicName = Guid.NewGuid().ToString();
            var subjectInitial = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName, null);
            var subject = SubjectNameStrategy.Topic.ConstructValueSubjectName(topicName+"2", null);

            // check that registering a base64 protobuf schema works.
            var id1 = await srInitial.RegisterSchemaAsync(subjectInitial, new Schema(testSchemaBase64, SchemaType.Protobuf));
            var schema1 = await sr.GetSchemaAsync(id1, "serialized"); // use a different sr instance to ensure a cached value is not read.
            Assert.Equal(SchemaType.Protobuf, schema1.SchemaType);
            Assert.NotNull(schema1.SchemaString); // SR slightly munges the schema as a result of moving to a text representation and back so can't check for equality.

            // check that the id of the schema just registered can be retrieved.
            var id = await sr.GetSchemaIdAsync(subjectInitial, new Schema(schema1.SchemaString, SchemaType.Protobuf));
            Assert.Equal(id1, id);

            // re-register the munged schema (to a different subject) and check that it is not re-munged.
            var id2 = await sr.RegisterSchemaAsync(subject, schema1);
            var schema2 = await sr.GetSchemaAsync(id2, "serialized");
            Assert.Equal(schema1.SchemaString, schema2.SchemaString);
            Assert.Equal(schema1.SchemaType, schema2.SchemaType);

            // This sequence of operations is designed to test caching behavior (and two alternat schema getting methods).
            var schemaAsText = await sr.GetSchemaAsync(id2);
            var schemaAsSerialized = await sr.GetSchemaAsync(id2, "serialized");
            var schemaAsText2 = await sr.GetSchemaAsync(id2);
            var schemaAsSerialized2 = await sr.GetSchemaAsync(id2, "serialized");
            var latestSchema = await sr.GetLatestSchemaAsync(subject); // should come back as text.
            var schemaAsSerialized3 = await sr.GetSchemaAsync(id2, "serialized");
            var latestSchema2 = await sr.GetRegisteredSchemaAsync(subject, latestSchema.Version); // should come back as text.
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
            var compat = await sr.IsCompatibleAsync(subject, schema2);
            Assert.True(compat);
            var avroSchema = 
                "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
                "\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
                "nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";
            var compat2 = await sr.IsCompatibleAsync(subject, avroSchema);
            Assert.False(compat2);
            var compat3 = await sr.IsCompatibleAsync(subject, new Schema(avroSchema, SchemaType.Avro));
            Assert.False(compat3);

            // invalid type
            await Assert.ThrowsAnyAsync<Exception>(async () => {
                await sr.RegisterSchemaAsync(SubjectNameStrategy.Topic.ConstructKeySubjectName(topicName+"3", null), new Schema(avroSchema, SchemaType.Protobuf));
            });
        }
    }
}
