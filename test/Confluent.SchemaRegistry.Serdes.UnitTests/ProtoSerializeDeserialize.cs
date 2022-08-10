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

// ConstructValueSubjectName is still used a an internal implementation detail.
#pragma warning disable CS0618

using Moq;
using Xunit;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;


namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class ProtobufSerializeDeserialzeTests
    {
        private ISchemaRegistryClient schemaRegistryClient;
        private string testTopic;
        private Dictionary<string, int> store = new Dictionary<string, int>();

        public ProtobufSerializeDeserialzeTests()
        {
            testTopic = "topic";
            var schemaRegistryMock = new Mock<ISchemaRegistryClient>();
            schemaRegistryMock.Setup(x => x.ConstructValueSubjectName(testTopic, It.IsAny<string>())).Returns($"{testTopic}-value");
            schemaRegistryMock.Setup(x => x.RegisterSchemaAsync("topic-value", It.IsAny<string>(), It.IsAny<bool>())).ReturnsAsync(
                (string topic, string schema, bool normalize) => store.TryGetValue(schema, out int id) ? id : store[schema] = store.Count + 1
            );
            schemaRegistryMock.Setup(x => x.GetSchemaAsync(It.IsAny<int>(), It.IsAny<string>())).ReturnsAsync(
                (int id, string format) => new Schema(store.Where(x => x.Value == id).First().Key, null, SchemaType.Protobuf)
            );
            schemaRegistryClient = schemaRegistryMock.Object;   
        }

        [Fact]
        public void Null()
        {
            var protoSerializer = new ProtobufSerializer<UInt32Value>(schemaRegistryClient);
            var protoDeserializer = new ProtobufDeserializer<UInt32Value>();

            var bytes = protoSerializer.SerializeAsync(null, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Null(bytes);
            Assert.Null(protoDeserializer.DeserializeAsync(bytes, true, new SerializationContext(MessageComponentType.Value, testTopic)).Result);
        }

        [Fact]
        public void UInt32SerDe()
        {
            var protoSerializer = new ProtobufSerializer<UInt32Value>(schemaRegistryClient);
            var protoDeserializer = new ProtobufDeserializer<UInt32Value>();

            var v = new UInt32Value { Value = 1234 };
            var bytes = protoSerializer.SerializeAsync(v, new SerializationContext(MessageComponentType.Value, testTopic)).Result;
            Assert.Equal(v.Value, protoDeserializer.DeserializeAsync(bytes, false, new SerializationContext(MessageComponentType.Value, testTopic)).Result.Value);
        }

    }
}
