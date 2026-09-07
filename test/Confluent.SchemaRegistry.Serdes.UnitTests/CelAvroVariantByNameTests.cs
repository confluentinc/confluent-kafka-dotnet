// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     A schema carrying a variant at more than one site.
    ///
    ///     Apache.Avro applies a logical type only where the schema *defines* the record. A
    ///     by-name reference resolves through <c>SchemaNames</c>, which is typed to
    ///     <c>NamedSchema</c> and cannot hold the <c>LogicalSchema</c> wrapper, so the second site
    ///     got a plain <c>RecordSchema</c> - and the writer refused the <see cref="Variant" />
    ///     with "GenericRecord required to write against record schema". A C# schema could carry a
    ///     variant at exactly one site.
    ///
    ///     Two halves, because two different schemas are in play:
    ///       - **read**, and anything else the client parses itself: the reference is rebound into
    ///         object form before the parse, which does re-apply the logical type;
    ///       - **write**: the writer schema is <c>data.Schema</c>, the *caller's* parse, so the
    ///         value is converted to the base record instead.
    /// </summary>
    public class CelAvroVariantByNameTests : BaseSerializeDeserializeTests
    {
        private const string VariantDef =
            @"{""type"":""record"",""name"":""confluent.type.Variant"",""logicalType"":""variant"",""fields"":[" +
            @"{""name"":""metadata"",""type"":""bytes""},{""name"":""value"",""type"":""bytes""}]}";

        /// <summary>
        ///     `a` defines the variant; every other site references it by name - directly, inside
        ///     an array, inside a map, through a nested record, and as a union branch.
        /// </summary>
        private const string SchemaText =
            @"{""type"":""record"",""name"":""ByName"",""fields"":[" +
            @"{""name"":""a"",""type"":" + VariantDef + @"}," +
            @"{""name"":""b"",""type"":""confluent.type.Variant""}," +
            @"{""name"":""arr"",""type"":{""type"":""array"",""items"":""confluent.type.Variant""}}," +
            @"{""name"":""m"",""type"":{""type"":""map"",""values"":""confluent.type.Variant""}}," +
            @"{""name"":""nested"",""type"":{""type"":""record"",""name"":""Inner"",""fields"":[" +
            @"{""name"":""deep"",""type"":""confluent.type.Variant""}]}}," +
            @"{""name"":""maybe"",""type"":[""null"",""confluent.type.Variant""]}]}";

        public CelAvroVariantByNameTests() : base()
        {
            VariantLogicalType.EnsureRegistered();
        }

        /// <summary>The caller's own parse - which is what the write path uses.</summary>
        private static RecordSchema CallerSchema()
        {
            VariantLogicalType.EnsureRegistered();
            return (RecordSchema)Avro.Schema.Parse(SchemaText);
        }

        private static GenericRecord Message(RecordSchema schema)
        {
            var v = Variant.ParseJson("{\"name\":\"alice\"}");
            var inner = new GenericRecord((RecordSchema)schema["nested"].Schema);
            inner.Add("deep", v);

            var record = new GenericRecord(schema);
            record.Add("a", v);
            record.Add("b", v);
            record.Add("arr", new object[] { v });
            record.Add("m", new Dictionary<string, object> { ["k"] = v });
            record.Add("nested", inner);
            record.Add("maybe", v);
            return record;
        }

        private async Task<GenericRecord> RoundTrip(string topic)
        {
            RecordSchema schema = CallerSchema();
            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = true });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient);
            var ctx = new SerializationContext(MessageComponentType.Value, topic, new Headers());

            byte[] bytes = await ser.SerializeAsync(Message(schema), ctx);
            return await deser.DeserializeAsync(bytes, false, ctx);
        }

        /// <summary>
        ///     The definition site alone, as a control: it worked before and must still work, so a
        ///     failure here says the fix broke the case that was already fine.
        /// </summary>
        [Fact]
        public async Task ADefinitionSiteStillRoundTrips()
        {
            VariantLogicalType.EnsureRegistered();
            var schema = (RecordSchema)Avro.Schema.Parse(
                @"{""type"":""record"",""name"":""OneSite"",""fields"":[{""name"":""a"",""type"":" +
                VariantDef + @"}]}");
            var record = new GenericRecord(schema);
            record.Add("a", Variant.ParseJson("{\"name\":\"alice\"}"));

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = true });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient);
            var ctx = new SerializationContext(MessageComponentType.Value, "onesite", new Headers());

            var back = await deser.DeserializeAsync(
                await ser.SerializeAsync(record, ctx), false, ctx);

            Assert.Equal("{\"name\":\"alice\"}", Assert.IsType<Variant>(back["a"]).ToJson());
        }

        [Fact]
        public async Task EveryByNameReferenceSiteRoundTrips()
        {
            GenericRecord back = await RoundTrip("byname");

            const string expected = "{\"name\":\"alice\"}";
            Assert.Equal(expected, Assert.IsType<Variant>(back["a"]).ToJson());
            Assert.Equal(expected, Assert.IsType<Variant>(back["b"]).ToJson());
            Assert.Equal(expected, Assert.IsType<Variant>(back["maybe"]).ToJson());

            var arr = Assert.IsAssignableFrom<System.Collections.IList>(back["arr"]);
            Assert.Equal(expected, Assert.IsType<Variant>(arr[0]).ToJson());

            var map = Assert.IsAssignableFrom<System.Collections.IDictionary>(back["m"]);
            Assert.Equal(expected, Assert.IsType<Variant>(map["k"]).ToJson());

            var nested = Assert.IsType<GenericRecord>(back["nested"]);
            Assert.Equal(expected, Assert.IsType<Variant>(nested["deep"]).ToJson());
        }

        /// <summary>
        ///     The reference sites really are bare records in the caller's parse - so the test
        ///     above is exercising the fix rather than a schema that never had the problem.
        /// </summary>
        [Fact]
        public void TheCallersParseLosesTheLogicalTypeAtEveryReference()
        {
            RecordSchema schema = CallerSchema();

            Assert.Equal(Avro.Schema.Type.Logical, schema["a"].Schema.Tag);
            Assert.Equal(Avro.Schema.Type.Record, schema["b"].Schema.Tag);
            Assert.Equal(Avro.Schema.Type.Record,
                ((ArraySchema)schema["arr"].Schema).ItemSchema.Tag);
        }

        /// <summary>
        ///     The rebind must not touch a type that merely *ends* in "Variant" - which is why it
        ///     resolves each reference against the enclosing namespace instead of string-matching
        ///     the name. Round-tripped rather than asserted on the parse, so it covers the rebind
        ///     and the value binding together.
        /// </summary>
        [Fact]
        public async Task AnUnrelatedVariantTypeIsLeftAlone()
        {
            VariantLogicalType.EnsureRegistered();
            string text =
                @"{""type"":""record"",""name"":""Holder"",""namespace"":""my.own"",""fields"":[" +
                @"{""name"":""defined"",""type"":{""type"":""record"",""name"":""Variant"",""fields"":[" +
                @"{""name"":""x"",""type"":""int""}]}}," +
                @"{""name"":""byRef"",""type"":""Variant""}," +
                @"{""name"":""real"",""type"":" + VariantDef + @"}]}";
            var schema = (RecordSchema)Avro.Schema.Parse(text);

            var other = new GenericRecord((RecordSchema)schema["defined"].Schema);
            other.Add("x", 7);
            var record = new GenericRecord(schema);
            record.Add("defined", other);
            record.Add("byRef", other);
            record.Add("real", Variant.ParseJson("{\"name\":\"alice\"}"));

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = true });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient);
            var ctx = new SerializationContext(MessageComponentType.Value, "unrelated", new Headers());

            var back = await deser.DeserializeAsync(
                await ser.SerializeAsync(record, ctx), false, ctx);

            // my.own.Variant is a different type: it must come back a plain record, not a Variant.
            Assert.Equal(7, Assert.IsType<GenericRecord>(back["defined"])["x"]);
            Assert.Equal(7, Assert.IsType<GenericRecord>(back["byRef"])["x"]);
            // And the real one alongside it is still a Variant.
            Assert.Equal("{\"name\":\"alice\"}", Assert.IsType<Variant>(back["real"]).ToJson());
        }
    }
}
