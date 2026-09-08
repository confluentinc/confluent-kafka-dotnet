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
using System.Linq;
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
        ///     The likeliest way to meet this: two variant fields on one record, no references
        ///     anywhere. Avro rejects a duplicate definition of confluent.type.Variant, so the
        ///     second field has to be the by-name form - it is not a style choice. Combined with
        ///     a registry-sourced writer schema, that used to fail.
        /// </summary>
        [Fact]
        public async Task TwoVariantFieldsRoundTripWithALatestSchema()
        {
            VariantLogicalType.EnsureRegistered();
            var schema = (RecordSchema)Avro.Schema.Parse(
                @"{""type"":""record"",""name"":""TwoVariants"",""fields"":[" +
                @"{""name"":""a"",""type"":" + VariantDef + @"}," +
                @"{""name"":""b"",""type"":""confluent.type.Variant""}]}");
            var registered = new RegisteredSchema(
                "twofields-value", 1, 1, schema.ToString(), SchemaType.Avro, null);
            store[schema.ToString()] = 1;
            subjectStore["twofields-value"] = new List<RegisteredSchema> { registered };

            var v = Variant.ParseJson("{\"name\":\"alice\"}");
            var record = new GenericRecord(schema);
            record.Add("a", v);
            record.Add("b", v);

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig
                {
                    UseLatestVersion = true,
                    AutoRegisterSchemas = false,
                    SubjectNameStrategy = SubjectNameStrategy.Topic,
                });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient);
            var ctx = new SerializationContext(
                MessageComponentType.Value, "twofields", new Headers());

            var back = await deser.DeserializeAsync(
                await ser.SerializeAsync(record, ctx), false, ctx);

            Assert.Equal("{\"name\":\"alice\"}", Assert.IsType<Variant>(back["a"]).ToJson());
            Assert.Equal("{\"name\":\"alice\"}", Assert.IsType<Variant>(back["b"]).ToJson());
        }

        /// <summary>
        ///     The full round trip with the writer schema coming from the registry rather than
        ///     from the record. The registry parse is rebound so that rules see a Variant, which
        ///     made it structurally unequal to the caller's own parse - and Apache.Avro's writer
        ///     asserts <c>record.Schema.Equals(writerSchema)</c>, so it refused the record. The
        ///     encoder now gets an unrebound parse of the same text.
        /// </summary>
        [Fact]
        public async Task AVariantImportedThroughAReferenceRoundTrips()
        {
            VariantLogicalType.EnsureRegistered();
            var refSchema = new RegisteredSchema(
                "variant-value", 1, 1, VariantDef, SchemaType.Avro, null);
            store[VariantDef] = 1;
            subjectStore["variant-value"] = new List<RegisteredSchema> { refSchema };

            const string rootText =
                @"{""type"":""record"",""name"":""ViaReference"",""fields"":[" +
                @"{""name"":""b"",""type"":""confluent.type.Variant""}]}";
            var refs = new List<SchemaReference>
            {
                new SchemaReference("confluent.type.Variant", "variant-value", 1),
            };
            var rootRegistered = new RegisteredSchema(
                "viaref-value", 1, 2, rootText, SchemaType.Avro, refs);
            store[rootText] = 2;
            subjectStore["viaref-value"] = new List<RegisteredSchema> { rootRegistered };

            // Resolve the reference the way the client does, rather than hand-building the
            // name table, so the caller's schema is the one a caller would actually hold.
            SchemaNames names = await AvroUtils.ResolveNamedSchema(
                new Schema(rootText, refs, SchemaType.Avro), schemaRegistryClient);
            var callerSchema = (RecordSchema)Avro.Schema.Parse(rootText, names);

            var record = new GenericRecord(callerSchema);
            record.Add("b", Variant.ParseJson("{\"name\":\"alice\"}"));

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig
                {
                    UseLatestVersion = true,
                    AutoRegisterSchemas = false,
                    SubjectNameStrategy = SubjectNameStrategy.Topic,
                });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient);
            var ctx = new SerializationContext(
                MessageComponentType.Value, "viaref", new Headers());

            var back = await deser.DeserializeAsync(
                await ser.SerializeAsync(record, ctx), false, ctx);

            Assert.Equal("{\"name\":\"alice\"}", Assert.IsType<Variant>(back["b"]).ToJson());
        }

        /// <summary>
        ///     A referenced schema may define the variant once and reference it by name
        ///     elsewhere within itself. Only the root schema used to be rebound, so that nested
        ///     site stayed a bare record however the root was parsed, and a field behind it did
        ///     not surface as a Variant.
        /// </summary>
        [Fact]
        public async Task AByNameReferenceInsideAReferencedSchemaIsRebound()
        {
            VariantLogicalType.EnsureRegistered();
            const string wrapperText =
                @"{""type"":""record"",""name"":""Wrapper"",""fields"":[" +
                @"{""name"":""defined"",""type"":" + VariantDef + @"}," +
                @"{""name"":""byname"",""type"":""confluent.type.Variant""}]}";
            var refSchema = new RegisteredSchema(
                "wrapper-value", 1, 1, wrapperText, SchemaType.Avro, null);
            store[wrapperText] = 1;
            subjectStore["wrapper-value"] = new List<RegisteredSchema> { refSchema };

            const string rootText =
                @"{""type"":""record"",""name"":""Root"",""fields"":[" +
                @"{""name"":""w"",""type"":""Wrapper""}]}";
            var refs = new List<SchemaReference>
            {
                new SchemaReference("Wrapper", "wrapper-value", 1),
            };

            SchemaNames names = await AvroUtils.ResolveNamedSchema(
                new Schema(rootText, refs, SchemaType.Avro), schemaRegistryClient);

            var wrapper = (RecordSchema)names.Names.Values
                .First(x => x.Fullname == "Wrapper");

            // The definition site was always a LogicalSchema; the by-name site now is too.
            Assert.IsType<LogicalSchema>(wrapper["defined"].Schema);
            Assert.IsType<LogicalSchema>(wrapper["byname"].Schema);
        }

        /// <summary>
        ///     Resolving a reference to a schema whose top level declares the variant logical
        ///     type. Once the logical type is registered, that parse yields a LogicalSchema,
        ///     which is not a NamedSchema - so hard-casting it threw before the root schema was
        ///     ever parsed. Avro Java has no such split: a logical type is an attribute of the
        ///     Schema there, so one object is both named and logical.
        /// </summary>
        [Fact]
        public async Task AReferenceToAnAnnotatedSchemaResolves()
        {
            VariantLogicalType.EnsureRegistered();
            var refSchema = new RegisteredSchema(
                "variant-value", 1, 1, VariantDef, SchemaType.Avro, null);
            store[VariantDef] = 1;
            subjectStore["variant-value"] = new List<RegisteredSchema> { refSchema };

            const string rootText =
                @"{""type"":""record"",""name"":""ViaReference"",""fields"":[" +
                @"{""name"":""b"",""type"":""confluent.type.Variant""}]}";
            var refs = new List<SchemaReference>
            {
                new SchemaReference("confluent.type.Variant", "variant-value", 1),
            };

            SchemaNames names = await AvroUtils.ResolveNamedSchema(
                new Schema(rootText, refs, SchemaType.Avro), schemaRegistryClient);

            // The referenced variant is registered under its name, so the root schema's by-name
            // reference resolves - which is the property the cast used to destroy.
            Avro.Schema root = Avro.Schema.Parse(rootText, names);
            Assert.Equal("ViaReference", ((RecordSchema)root).Fullname);
        }

        /// <summary>
        ///     A schema already written in the annotated object form - the exact shape the
        ///     rebinder produces - must be left alone. Its `type` property holds the variant's
        ///     name, so rewriting it again nested one form inside the other and corrupted a
        ///     schema the user had written correctly by hand.
        /// </summary>
        [Fact]
        public async Task AnAlreadyAnnotatedReferenceRoundTrips()
        {
            VariantLogicalType.EnsureRegistered();
            var schema = (RecordSchema)Avro.Schema.Parse(
                @"{""type"":""record"",""name"":""AlreadyAnnotated"",""fields"":[" +
                @"{""name"":""a"",""type"":" + VariantDef + @"}," +
                @"{""name"":""b"",""type"":{""type"":""confluent.type.Variant""," +
                @"""logicalType"":""variant""}}]}");
            var v = Variant.ParseJson("{\"name\":\"alice\"}");
            var record = new GenericRecord(schema);
            record.Add("a", v);
            record.Add("b", v);

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = true });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient);
            var ctx = new SerializationContext(
                MessageComponentType.Value, "annotated", new Headers());

            var back = await deser.DeserializeAsync(
                await ser.SerializeAsync(record, ctx), false, ctx);

            Assert.Equal("{\"name\":\"alice\"}", Assert.IsType<Variant>(back["a"]).ToJson());
            Assert.Equal("{\"name\":\"alice\"}", Assert.IsType<Variant>(back["b"]).ToJson());
        }

        /// <summary>
        ///     Serializing must leave the caller's record alone. The rebinding walk turns a
        ///     Variant into its base record at every by-name site, and it used to write those
        ///     back into the record it was handed - so a successful serialize silently changed
        ///     the caller's object, and two threads serializing one record could race over it.
        /// </summary>
        [Fact]
        public async Task SerializingDoesNotMutateTheCallersRecord()
        {
            RecordSchema schema = CallerSchema();
            GenericRecord record = Message(schema);
            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = true });
            var ctx = new SerializationContext(
                MessageComponentType.Value, "no-mutation", new Headers());

            await ser.SerializeAsync(record, ctx);

            // Every by-name site still holds what the caller put there.
            Assert.IsType<Variant>(record["a"]);
            Assert.IsType<Variant>(record["b"]);
            Assert.IsType<Variant>(((object[])record["arr"])[0]);
            Assert.IsType<Variant>(((Dictionary<string, object>)record["m"])["k"]);
            Assert.IsType<Variant>(((GenericRecord)record["nested"])["deep"]);
            Assert.IsType<Variant>(record["maybe"]);
        }

        /// <summary>
        ///     A union may carry a container branch rather than the variant record directly. The
        ///     branch was previously chosen as the first non-null one and only tested for a
        ///     record, so an optional array or map of variants was never descended into and the
        ///     generic writer then rejected the Variant inside it.
        /// </summary>
        [Fact]
        public async Task AVariantInsideAUnionContainerRoundTrips()
        {
            VariantLogicalType.EnsureRegistered();
            var schema = (RecordSchema)Avro.Schema.Parse(
                @"{""type"":""record"",""name"":""UnionContainer"",""fields"":[" +
                @"{""name"":""a"",""type"":" + VariantDef + @"}," +
                @"{""name"":""arr"",""type"":[""null"",{""type"":""array""," +
                @"""items"":""confluent.type.Variant""}]}," +
                @"{""name"":""m"",""type"":[""null"",{""type"":""map""," +
                @"""values"":""confluent.type.Variant""}]}]}");
            var v = Variant.ParseJson("{\"name\":\"alice\"}");
            var record = new GenericRecord(schema);
            record.Add("a", v);
            record.Add("arr", new object[] { v });
            record.Add("m", new Dictionary<string, object> { ["k"] = v });

            var ser = new AvroSerializer<GenericRecord>(schemaRegistryClient,
                new AvroSerializerConfig { AutoRegisterSchemas = true });
            var deser = new AvroDeserializer<GenericRecord>(schemaRegistryClient);
            var ctx = new SerializationContext(
                MessageComponentType.Value, "unioncontainer", new Headers());

            var back = await deser.DeserializeAsync(
                await ser.SerializeAsync(record, ctx), false, ctx);

            Assert.Equal("{\"name\":\"alice\"}",
                Assert.IsType<Variant>(((object[])back["arr"])[0]).ToJson());
            Assert.Equal("{\"name\":\"alice\"}",
                Assert.IsType<Variant>(
                    ((IDictionary<string, object>)back["m"])["k"]).ToJson());
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
