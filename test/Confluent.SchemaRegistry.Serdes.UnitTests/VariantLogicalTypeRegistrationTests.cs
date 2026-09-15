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

using Xunit;

namespace Confluent.SchemaRegistry.Serdes.UnitTests
{
    /// <summary>
    ///     The <c>variant</c> logical type has to be registered before any variant schema is
    ///     parsed, not when a serde is constructed. Apache.Avro resolves a logical type at parse
    ///     time and caches it on the <see cref="Avro.LogicalSchema" />; registering afterwards
    ///     does not retrofit it, so a schema parsed too early keeps <c>UnknownLogicalType</c>
    ///     for the life of that object. A generated specific record parses its schema in a
    ///     static initializer, so constructing one is enough to lose the race - which is why
    ///     registration moved to a module initializer, the .NET counterpart of the reference's
    ///     static block on <c>AvroSchemaUtils</c>.
    /// </summary>
    public class VariantLogicalTypeRegistrationTests
    {
        private const string VariantSchema = @"{
          ""type"":""record"",""name"":""Doc"",""namespace"":""t"",
          ""fields"":[{""name"":""payload"",""type"":
            {""type"":""record"",""name"":""VariantRec"",
             ""fields"":[{""name"":""metadata"",""type"":""bytes""},
                         {""name"":""value"",""type"":""bytes""}],
             ""logicalType"":""variant""}}]}";

        /// <summary>
        ///     Parses a variant schema without constructing a serde first. Loading this assembly
        ///     is the only thing that has happened, and that has to be enough.
        /// </summary>
        [Fact]
        public void ParsingAVariantSchemaNeedsNoSerdeFirst()
        {
            var parsed = (Avro.RecordSchema)Avro.Schema.Parse(VariantSchema);
            var payload = parsed["payload"].Schema;

            Assert.Equal(Avro.Schema.Type.Logical, payload.Tag);
            Assert.IsType<VariantLogicalType>(((Avro.LogicalSchema)payload).LogicalType);
        }

        /// <summary>
        ///     And the explicit entry point stays idempotent, for the one case a module
        ///     initializer cannot reach: a caller that parses a variant schema without ever
        ///     touching this assembly.
        /// </summary>
        [Fact]
        public void EnsureRegisteredIsIdempotent()
        {
            VariantLogicalType.EnsureRegistered();
            VariantLogicalType.EnsureRegistered();

            var parsed = (Avro.RecordSchema)Avro.Schema.Parse(VariantSchema);
            Assert.IsType<VariantLogicalType>(
                ((Avro.LogicalSchema)parsed["payload"].Schema).LogicalType);
        }
    }
}
