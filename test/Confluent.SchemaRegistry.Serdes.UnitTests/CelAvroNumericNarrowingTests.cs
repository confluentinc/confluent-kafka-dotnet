// Copyright 2026 Confluent Inc.
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
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     A message-level <c>CEL</c> transform over a record with an int or float field.
    ///
    ///     CEL has one integer width and one floating one, so the result map carries a long and a
    ///     double whatever the field declares, and Apache.Avro's writer type-checks the CLR type.
    ///     The write-back narrowed nothing, so <em>every</em> rule over such a schema failed with
    ///     "System.Int32 required to write against Int schema but found System.Int64" - the
    ///     identity transform included, since replace semantics make the rule name every field.
    ///     The reference narrows against the schema in narrowToInt / narrowToFloat.
    /// </summary>
    public class CelAvroNumericNarrowingTests : BaseSerializeDeserializeTests
    {
        private const string NumericSchema = @"
{
  ""type"": ""record"",
  ""name"": ""Numeric"",
  ""fields"": [
    { ""name"": ""count"", ""type"": ""int"" },
    { ""name"": ""ratio"", ""type"": ""float"" },
    { ""name"": ""nullableCount"", ""type"": [""null"", ""int""] },
    { ""name"": ""label"", ""type"": ""string"" }
  ]
}";

        private const string AllFields =
            "'count': message.count, 'ratio': message.ratio, " +
            "'nullableCount': message.nullableCount, 'label': message.label";

        private static GenericRecord Record()
        {
            var record = new GenericRecord((RecordSchema)Avro.Schema.Parse(NumericSchema));
            record.Add("count", 7);
            record.Add("ratio", 1.5f);
            record.Add("nullableCount", 3);
            record.Add("label", "hi");
            return record;
        }

        private GenericRecord RoundTrip(string expr)
        {
            var rule = new Rule("test-cel", RuleKind.Transform, RuleMode.Write, "CEL", null,
                null, expr, null, null, false);
            var schema = new RegisteredSchema(testTopic + "-value", 1, 1, NumericSchema,
                SchemaType.Avro, null)
            {
                RuleSet = new RuleSet(new List<Rule>(), new List<Rule> { rule })
            };
            store[NumericSchema] = 1;
            subjectStore[testTopic + "-value"] = new List<RegisteredSchema> { schema };

            var serializer = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig
                {
                    AutoRegisterSchemas = false, UseLatestVersion = true
                });
            var deserializer = new AvroDeserializer<GenericRecord>(schemaRegistryClient, null);
            var ctx = new SerializationContext(MessageComponentType.Value, testTopic);
            return deserializer.DeserializeAsync(
                serializer.SerializeAsync(Record(), ctx).Result, false, ctx).Result;
        }

        [Fact]
        public void IdentityOverNumericFieldsRoundTrips()
        {
            var result = RoundTrip("{" + AllFields + "}");

            Assert.Equal(7, result["count"]);
            Assert.Equal(1.5f, result["ratio"]);
            Assert.Equal(3, result["nullableCount"]);
        }

        /// <summary>
        ///     The discriminator: a computed int proves the rule's result was written, so the test
        ///     above cannot be passing because the transform stopped running.
        /// </summary>
        [Fact]
        public void ComputedIntIsWritten()
        {
            var result = RoundTrip(
                "{'count': message.count + 1, 'ratio': message.ratio, " +
                "'nullableCount': message.nullableCount, 'label': message.label}");

            Assert.Equal(8, result["count"]);
        }

        /// <summary>narrowToInt's range check: truncating would have written -2147483648.</summary>
        [Fact]
        public void OutOfRangeIntIsRefused()
        {
            var thrown = Assert.ThrowsAny<Exception>(() => RoundTrip(
                "{'count': 2147483648, 'ratio': message.ratio, " +
                "'nullableCount': message.nullableCount, 'label': message.label}"));

            Exception inner = thrown;
            while (inner.InnerException != null)
            {
                inner = inner.InnerException;
            }

            Assert.Contains("out of range for INT field", inner.Message);
        }
    }
}
