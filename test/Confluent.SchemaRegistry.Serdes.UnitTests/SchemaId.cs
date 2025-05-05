// Copyright 2025 Confluent Inc.
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
using System.IO;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    public class SchemaIdTests
    {
        [Fact]
        public void TestGuid()
        {
            var schemaId = new SchemaId(SchemaType.Avro);
            byte[] input = new byte[]
            {
                0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9b, 0x90,
                0xa8, 0x02, 0xe2
            };
            schemaId.FromBytes(new MemoryStream(input));
            var guidString = schemaId.Guid.ToString();
            Assert.Equal("89791762-2336-4186-9674-299b90a802e2", guidString);

            var output = schemaId.GuidToBytes();
            Assert.Equal(output, input);
        }

        [Fact]
        public void TestId()
        {
            var schemaId = new SchemaId(SchemaType.Avro);
            byte[] input = new byte[]
            {
                0x00, 0x00, 0x00, 0x00, 0x01
            };
            schemaId.FromBytes(new MemoryStream(input));
            var id = schemaId.Id;
            Assert.Equal(1, id);

            var output = schemaId.IdToBytes();
            Assert.Equal(output, input);
        }

        [Fact]
        public void TestGuidWithMessageIndexes()
        {
            var schemaId = new SchemaId(SchemaType.Protobuf);
            byte[] input = new byte[]
            {
                0x01, 0x89, 0x79, 0x17, 0x62, 0x23, 0x36, 0x41, 0x86, 0x96, 0x74, 0x29, 0x9b, 0x90,
                0xa8, 0x02, 0xe2, 0x06, 0x02, 0x04, 0x06
            };
            schemaId.FromBytes(new MemoryStream(input));
            var guidString = schemaId.Guid.ToString();
            Assert.Equal("89791762-2336-4186-9674-299b90a802e2", guidString);

            var messageIndexes = schemaId.MessageIndexes;
            Assert.Equal(messageIndexes, new List<int>() { 1, 2, 3 });

            var output = schemaId.GuidToBytes();
            Assert.Equal(output, input);
        }

        [Fact]
        public void TestIdWithMessageIndexes()
        {
            var schemaId = new SchemaId(SchemaType.Protobuf);
            byte[] input = new byte[]
            {
                0x00, 0x00, 0x00, 0x00, 0x01, 0x06, 0x02, 0x04, 0x06
            };
            schemaId.FromBytes(new MemoryStream(input));
            var id = schemaId.Id;
            Assert.Equal(1, id);

            var messageIndexes = schemaId.MessageIndexes;
            Assert.Equal(messageIndexes, new List<int>() { 1, 2, 3 });

            var output = schemaId.IdToBytes();
            Assert.Equal(output, input);
        }
    }
}
